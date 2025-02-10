/*
 Copyright (c) 2021, 2025, Oracle and/or its affiliates.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License, version 2.0,
 as published by the Free Software Foundation.

 This program is also distributed with certain software (including
 but not limited to OpenSSL) that is licensed under separate terms,
 as designated in a particular file or component or in included license
 documentation.  The authors of MySQL hereby grant you an additional
 permission to link the program and your derivative works with the
 separately licensed software that they have included with MySQL.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include "mrs/authentication/authorize_manager.h"

#include <time.h>

#include <cassert>
#include <chrono>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <string_view>

#include "my_macros.h"

#include "mrs/authentication/auth_handler_factory.h"
#include "mrs/authentication/track_authorize_handler.h"
#include "mrs/authentication/www_authentication_handler.h"
#include "mrs/http/error.h"
#include "mrs/rest/request_context.h"

#include "helper/container/generic.h"
#include "helper/container/map.h"
#include "helper/json/rapid_json_to_map.h"
#include "helper/json/rapid_json_to_struct.h"
#include "helper/json/text_to.h"
#include "helper/make_shared_ptr.h"
#include "helper/string/hex.h"  // unhex
#include "helper/string/random.h"
#include "helper/string/replace.h"
#include "helper/token/jwt.h"

#include "http/base/headers.h"
#include "mysql/harness/logging/logging.h"
#include "mysql/harness/string_utils.h"

IMPORT_LOG_FUNCTIONS()

namespace mrs {
namespace authentication {

using milliseconds = std::chrono::milliseconds;
using seconds = std::chrono::seconds;
using Headers = ::http::base::Headers;
using JwtHolder = helper::JwtHolder;
using Jwt = helper::Jwt;
using SessionId = AuthorizeManager::SessionId;
using Session = AuthorizeManager::Session;
using Handlers = AuthorizeManager::AuthHandlers;
using AuthorizeHandlerPtr = AuthorizeManager::AuthorizeHandlerPtr;

const UniversalId k_vendor_mrs{{0x30, 0}};
const UniversalId k_vendor_mysql{{0x31, 0}};
const UniversalId k_vendor_facebook{{0x32, 0}};
const UniversalId k_vendor_google{{0x34, 0}};
const UniversalId k_vendor_oidc{{0x35, 0}};

namespace {

class AuthenticationOptions {
 public:
  std::optional<uint64_t> host_requests_per_minute_{};
  std::optional<milliseconds> host_minimum_time_between_requests{};
  std::optional<uint64_t> account_requests_per_minute_{};
  std::optional<milliseconds> account_minimum_time_between_requests;
  seconds block_for{60};
};

class ParseAuthenticationOptions
    : public helper::json::RapidReaderHandlerStringValuesToStruct<
          AuthenticationOptions> {
 public:
  uint64_t to_uint(const std::string &value) {
    return std::stoull(value.c_str());
  }

  void handle_object_value(const std::string &key,
                           const std::string &vt) override {
    using std::to_string;
    if (key ==
        "authentication.throttling.perAccount.minimumTimeBetweenRequestsInMs") {
      result_.account_minimum_time_between_requests = milliseconds{to_uint(vt)};
    } else if (key ==
               "authentication.throttling.perAccount."
               "maximumAttemptsPerMinute") {
      result_.account_requests_per_minute_ = to_uint(vt);
    } else if (key ==
               "authentication.throttling.perHost."
               "minimumTimeBetweenRequestsInMs") {
      result_.host_minimum_time_between_requests = milliseconds{to_uint(vt)};
    } else if (key ==
               "authentication.throttling.perHost."
               "maximumAttemptsPerMinute") {
      result_.host_requests_per_minute_ = to_uint(vt);
    } else if (key ==
               "authentication.throttling.blockWhenAttemptsExceededInSeconds") {
      result_.block_for = seconds{to_uint(vt)};
    }
  }
};

auto parse_json_options(const std::string &options) {
  return helper::json::text_to_handler<ParseAuthenticationOptions>(options);
}

class UserIdContainer {
 public:
  using UserId = database::entry::AuthUser::UserId;
  auto begin() const { return std::begin(user_id_.raw); }
  auto end() const { return std::end(user_id_.raw); }
  void push_back(uint8_t value) { user_id_.raw[push_index_++] = value; }
  auto get_user_id() const { return user_id_; }

 private:
  UserId user_id_;
  uint64_t push_index_{0};
};

void throw_max_rate_exceeded(milliseconds ms) {
  std::string v;
  auto s = std::chrono::duration_cast<seconds>(ms);
  v = (s.count() == 0) ? "1" : std::to_string(s.count());

  throw http::ErrorWithHttpHeaders(HttpStatusCode::TooManyRequests,
                                   {{"Retry-After", v}});
}

std::string get_peer_host(rest::RequestContext &ctxt) {
  return ctxt.request->get_connection()->get_peer_address();
}

Jwt get_bearer_token_jwt(const Headers &headers) {
  auto authorization =
      headers.find_cstr(WwwAuthenticationHandler::kAuthorization);

  if (!authorization) return {};

  auto args = mysql_harness::split_string(authorization, ' ', false);
  std::string value = args.size() > 1 ? args[1] : "";

  log_debug("authorization: \"%s\"", authorization);

  JwtHolder holder;

  try {
    helper::Jwt::parse(value, &holder);

    auto jwt = helper::Jwt::create(holder);
    return jwt;
  } catch (const std::exception &e) {
    log_debug("JWT failure: %s.", e.what());
  }
  return {};
}

std::string get_session_cookie_key_name(const UniversalId &id) {
  return "session_" + id.to_string();
}

}  // namespace

AuthorizeManager::AuthorizeManager(collector::MysqlCacheManager *cache_manager,
                                   const std::string &jwt_secret,
                                   AuthHandlerFactoryPtr factory)
    : cache_manager_{cache_manager},
      jwt_secret_{jwt_secret},
      factory_{factory},
      random_data_{
          helper::generate_string<64, helper::Generator8bitsValues>()} {
  log_info("JWT bearer authorization disabled, the signing secret is empty.");
}

AuthorizeManager::AuthorizeManager(collector::MysqlCacheManager *cache_manager,
                                   const std::string &jwt_secret)
    : cache_manager_{cache_manager},
      jwt_secret_{jwt_secret},
      factory_{std::make_shared<AuthHandlerFactory>()} {}

void AuthorizeManager::configure(const std::string &options) {
  auto cnf = parse_json_options(options);

  // Move object in a thread safe mode
  // The assign operator is overloaded.
  accounts_rate_ = RateControlFor<std::string>(
      cnf.account_requests_per_minute_, cnf.block_for,
      cnf.account_minimum_time_between_requests);
  hosts_rate_ =
      RateControlFor<std::string>(cnf.host_requests_per_minute_, cnf.block_for,
                                  cnf.host_minimum_time_between_requests);
}

void AuthorizeManager::update(const Entries &entries) {
  Container::iterator it;

  if (entries.size()) {
    log_debug("auth_app: Number of updated entries:%i", (int)entries.size());
  }

  for (const auto &e : entries) {
    log_debug("auth_app: Processing update of id=%s", e.id.to_string().c_str());
    auto auth = make_auth(e);

    if (get_handler_by_id(e.id, &it)) {
      *it = auth;

      if (!auth) container_.erase(it);
    } else {
      if (auth) container_.push_back(auth);
    }
  }
}

AuthorizeManager::Container AuthorizeManager::get_handlers_by_service_id(
    const UniversalId service_id) {
  Container out_result;

  helper::container::copy_if(
      container_,
      [service_id](auto &element) {
        return element->get_service_ids().contains(service_id);
      },
      out_result);

  return out_result;
}

bool AuthorizeManager::get_handler_by_id(const UniversalId auth_id,
                                         Container::iterator *out_it) {
  auto it = std::find_if(container_.begin(), container_.end(),
                         [auth_id](auto &i) { return i->get_id() == auth_id; });

  if (it != container_.end()) {
    *out_it = it;
    return true;
  }

  return false;
}

bool AuthorizeManager::get_handler_by_id(const UniversalId auth_id,
                                         AuthorizeHandlerPtr &out_it) {
  Container::iterator it;
  if (get_handler_by_id(auth_id, &it)) {
    out_it = *it;
    return true;
  }
  return false;
}

AuthorizeHandlerPtr AuthorizeManager::make_auth(const AuthApp &entry) {
  if (entry.deleted) return {};

  if (!entry.active) return {};

  if (entry.vendor_id == k_vendor_mysql) {
    return factory_->create_basic_auth_handler(this, entry, cache_manager_);
  } else if (entry.vendor_id == k_vendor_facebook) {
    return factory_->create_facebook_auth_handler(this, entry);
  } else if (entry.vendor_id == k_vendor_google) {
    return factory_->create_google_auth_handler(this, entry);
  } else if (entry.vendor_id == k_vendor_mrs) {
    return factory_->create_scram_auth_handler(this, entry, random_data_);
  } else if (entry.vendor_id == k_vendor_oidc) {
    if (entry.url.empty()) {
      log_error(
          "OIDC Authentication application, requires that "
          "app-specific-URL is set.");
      return {};
    }

    return factory_->create_oidc_auth_handler(this, entry);
  }

  log_error(
      "authentication application with name '%s' not available, because it has "
      "unsupported vendor-id '%s'",
      entry.app_name.c_str(), entry.vendor_id.to_string().c_str());
  return {};
}

void AuthorizeManager::pre_authorize_account(
    interface::AuthorizeHandler *handler, const std::string &account) {
  auto unique_account_name = handler->get_id().to_string() + account;
  AcceptInfo ac;
  log_debug("AuthorizeManager::pre_authorize_account %s",
            unique_account_name.c_str());
  if (!accounts_rate_.allow(unique_account_name, &ac)) {
    if (ac.reason == BlockReason::kRateExceeded) {
      log_debug("Too many requests from user: '%s' for handler:%s.",
                account.c_str(), handler->get_id().to_string().c_str());
    }
    throw_max_rate_exceeded(ac.next_request_allowed_after);
  }
}

bool AuthorizeManager::unauthorize(Session *session, http::Cookie *cookies) {
  if (cookies->direct().count(session->get_holder_name())) {
    cookies->clear(session->get_holder_name().c_str());
  }

  return session_manager_.remove_session(session);
}

static std::string current_timestamp(std::chrono::system_clock::duration d) {
  auto now = std::chrono::system_clock::now() + d;
  std::time_t tt = std::chrono::system_clock::to_time_t(now);

  std::ostringstream os;
  os << std::put_time(gmtime(&tt), "%F %T");

  return os.str();
}

static bool is_timestamp_in_past(const std::string ts) {
  std::tm t = {};
  std::istringstream ss(ts);
  ss >> std::get_time(&t, "%Y-%m-%d %T");
  if (ss.fail()) return true;

  auto past = IF_WIN(_mkgmtime, timegm)(&t);
  auto current =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  if (past == -1 || current == -1) return true;

  if (difftime(current, past) <= 0.0) return false;

  return true;
}

template <typename Document>
void doc_set_member(Document &doc, std::string_view name,
                    std::string_view value) {
  rapidjson::Value jname{name.data(), name.size(), doc.GetAllocator()};
  rapidjson::Value jvalue{value.data(), value.size(), doc.GetAllocator()};
  doc.AddMember(jname, jvalue, doc.GetAllocator());
}

std::string AuthorizeManager::get_jwt_token(UniversalId service_id,
                                            Session *s) {
  using namespace rapidjson;
  Document payload;
  auto exp = current_timestamp(session_manager_.get_timeout());
  auto user_id_hex = helper::string::hex(s->user.user_id.raw);

  payload.SetObject();
  doc_set_member(payload, "user_id", user_id_hex);

  if (!s->user.email.empty()) doc_set_member(payload, "email", s->user.email);

  const auto &aid = s->get_authorization_handler_id();
  doc_set_member(payload, "exp", exp);
  doc_set_member(
      payload, "auth_app",
      std::string_view{reinterpret_cast<const char *>(aid.raw.data()),
                       aid.raw.size()});

  auto jwt = helper::Jwt::create("HS256", payload);

  auto token = jwt.sign(jwt_secret_);

  std::string session_id =
      service_id.to_string() + "." + s->user.user_id.to_string() + "." + exp;
  if (session_manager_.get_session(session_id)) return token;

  auto session = session_manager_.new_session(session_id);
  session->user = s->user;
  session->state = http::SessionManager::Session::kUserVerified;

  return token;
}

std::vector<std::pair<std::string, SessionId>>
AuthorizeManager::get_session_ids_cookies(const UniversalId &service_id,
                                          http::Cookie *cookies) {
  std::vector<std::pair<std::string, SessionId>> result;
  Container handlers = get_supported_authentication_applications(service_id);

  for (const auto &h : handlers) {
    const auto &session_cookie_key = get_session_cookie_key_name(h->get_id());
    std::string session_identifier;

    if (helper::container::get_value(cookies->direct(), session_cookie_key,
                                     &session_identifier)) {
      result.emplace_back(session_cookie_key, session_identifier);
    }
  }

  return result;
}

std::vector<SessionId> AuthorizeManager::get_session_ids_from_cookies(
    const UniversalId &service_id, http::Cookie *cookies) {
  std::vector<SessionId> result;
  Container handlers = get_supported_authentication_applications(service_id);

  for (const auto &h : handlers) {
    const auto session_cookie_key = get_session_cookie_key_name(h->get_id());
    std::string session_identifier;

    if (helper::container::get_value(cookies->direct(), session_cookie_key,
                                     &session_identifier)) {
      result.emplace_back(session_identifier);
    }
  }

  return result;
}

AuthorizeManager::Container
AuthorizeManager::get_supported_authentication_applications(ServiceId id) {
  return get_handlers_by_service_id(id);
}

Session *AuthorizeManager::authorize_jwt(const UniversalId service_id,
                                         const helper::Jwt &jwt) {
  log_debug("Validating JWT token: %s", jwt.get_token().c_str());
  if (!jwt.is_valid()) {
    log_debug("JWT token is invalid");
    return nullptr;
  }
  // We allow HS256, still it doesn't change much because
  // Jws supports only HS256 and none (just blocking use of none).
  if (jwt.get_header_claim_algorithm() != "HS256") {
    log_debug("JWT token not supported algorithm");
    return nullptr;
  }
  if (!jwt.verify(jwt_secret_)) {
    log_debug("JWT token verification failed");
    return nullptr;
  }

  auto claims = jwt.get_payload_claim_names();
  if (!helper::container::has(claims, "user_id")) return nullptr;
  if (!helper::container::has(claims, "exp")) return nullptr;
  if (!helper::container::has(claims, "auth_app")) return nullptr;

  auto json_uid = jwt.get_payload_claim_custom("user_id");
  auto json_exp = jwt.get_payload_claim_custom("exp");
  auto json_aid = jwt.get_payload_claim_custom("auth_app");

  if (!json_uid->IsString()) return nullptr;
  if (!json_exp->IsString()) return nullptr;
  if (!json_aid->IsString()) return nullptr;

  // TODO(lkotula): Change from_raw ? (Shouldn't be in review)
  auto user_id = helper::string::unhex<UserIdContainer>(json_uid->GetString())
                     .get_user_id();
  auto exp = json_exp->GetString();
  auto aid = UniversalId::from_cstr(json_aid->GetString(),
                                    json_aid->GetStringLength());

  auto handlers = this->get_handlers_by_service_id(service_id);

  if (!helper::container::get_if(
          handlers, [&aid](auto &h) { return h->get_id() == aid; }, nullptr)) {
    log_debug("Wrong service id.");
    return nullptr;
  }

  if (is_timestamp_in_past(exp)) {
    log_debug("Token expired.");
    return nullptr;
  }

  std::string session_id = user_id.to_string() + "." + exp;
  if (auto session = session_manager_.get_session(session_id)) {
    log_debug("Session for token already exsits: %s", session_id.c_str());
    return session;
  }

  auto session = session_manager_.new_session(session_id);

  auto instance = cache_manager_->get_instance(
      collector::kMySQLConnectionMetadataRW, false);
  if (user_manager_.user_get_by_id(user_id, &session->user, &instance)) {
    session->state = http::SessionManager::Session::kUserVerified;
    return session;
  }

  log_debug("User not found");
  // User verification failed, remove just created session.
  session_manager_.remove_session(session);
  return nullptr;
}

AuthorizeHandlerPtr AuthorizeManager::choose_authentication_handler(
    rest::RequestContext &ctxt, ServiceId service_id,
    const std::optional<std::string> &app_name) {
  auto handlers = get_handlers_by_service_id(service_id);
  if (handlers.empty())
    throw http::Error{
        HttpStatusCode::BadRequest,
        "Bad request - there is no authorization application available"};

  if (!app_name.has_value() && handlers.size() == 1) {
    return handlers[0];
  }

  auto app_name_value = app_name.value_or("");
  AuthorizeHandlerPtr result;
  if (!helper::container::get_if(
          handlers,
          [&app_name_value](const auto &handler) {
            return (app_name_value == handler->get_entry().app_name);
          },
          &result)) {
    // When there is no app name, try to find the handler
    // by looking at the payload.
    //
    // The payload may contain data pointing to the handler.
    if (!app_name.has_value()) {
      for (auto &h : handlers) {
        auto previouse_session_id = h->get_session_id_from_request_data(ctxt);
        if (previouse_session_id.has_value()) {
          auto session = session_manager_.get_session_secondary_id(
              previouse_session_id.value());
          // Even if the handler can parse the request and it thinks
          // that its his payload, we need look a second time,
          // for the handler using handler-id.
          // There may be a few different handlers, with the same
          // vendor-id.
          auto handler_id = session->get_authorization_handler_id();
          for (auto &handler : handlers) {
            if (handler_id == handler->get_id()) return handler;
          }
        }
      }
    }
    throw http::Error{
        HttpStatusCode::BadRequest,
        "Bad request - chosen authorization application no available"};
  }
  return result;
}

struct AuthorizeParameters {
  bool use_jwt{false};
  std::optional<std::string> session_id;
  std::optional<std::string> auth_app;
};

template <typename Container>
AuthorizeParameters extract_parameters(const Container &container,
                                       const bool allow_shorts = false) {
  AuthorizeParameters result;
  std::string value;

  if (helper::container::get_value(container, "sessionType", &value) &&
      value == "bearer") {
    result.use_jwt = 1;
  }

  if (helper::container::get_value(container, "authApp", &value)) {
    result.auth_app = value;
  } else if (allow_shorts &&
             helper::container::get_value(container, "app", &value)) {
    // Keep this "case" for backward compatibility.
    result.auth_app = value;
  }

  if (helper::container::get_value(container, "session", &value)) {
    result.session_id = value;
  }

  return result;
}

AuthorizeParameters get_authorize_parameters(::http::base::Request *request) {
  const auto method = request->get_method();
  const auto &uri = request->get_uri();

  // Please note that the handler that causes the call to
  // 'AuthorizeManager::authorize', should be configured to allow only
  // POST and GET, like following handler:
  //
  //  uint32_t HandlerAuthorizeLogin::get_access_rights() const {
  //    using Op = mrs::database::entry::Operation::Values;
  //    return Op::valueRead | Op::valueCreate;
  //  }
  if (method != HttpMethod::Get && method != HttpMethod::Post)
    throw http::Error{HttpStatusCode::BadRequest,
                      "Bad request - authorization must be either done in POST "
                      "or GET request."};

  if (method == HttpMethod::Get) {
    return extract_parameters(uri.get_query_elements(), true);
  }

  // POST
  auto body_object_fields = helper::json::text_to_handler<
      helper::json::RapidReaderHandlerToMapOfSimpleValues>(
      request->get_input_body());
  return extract_parameters(body_object_fields);
}

Session *AuthorizeManager::get_session_id_from_cookie(
    const UniversalId &service_id, http::Cookie &cookies) {
  auto session_ids = get_session_ids_from_cookies(service_id, &cookies);

  for (size_t index = 0; index < session_ids.size(); ++index) {
    auto session = session_manager_.get_session(session_ids[index]);
    if (session) {
      return session;
    }
  }

  return nullptr;
}

bool AuthorizeManager::authorize(const std::string &proto,
                                 const std::string &host, ServiceId service_id,
                                 rest::RequestContext &ctxt,
                                 AuthUser *out_user) {
  if (auto session = get_session_id_from_cookie(service_id, ctxt.cookies);
      session) {
    log_debug("Session source: cookie");
    ctxt.session = session;
  }

  log_debug(
      "AuthorizeManager::authorize(service_id:%s, session_id:%s, "
      "can_use_jwt:%s)",
      service_id.to_string().c_str(),
      ctxt.session ? ctxt.session->get_session_id().c_str() : "<NONE>",
      (jwt_secret_.empty() ? "no" : "yes"));

  AuthorizeHandlerPtr selected_handler;

  auto [use_jwt, url_session_id, auth_app] =
      get_authorize_parameters(ctxt.request);

  log_debug(
      "AuthorizeManager::authorize - use_jwt:%s, url_session_id:%s, "
      "auth_app:%s",
      (use_jwt ? "yes" : "no"), url_session_id.value_or("<NONE>").c_str(),
      auth_app.value_or("<NONE>").c_str());

  if (!ctxt.session && url_session_id.has_value()) {
    log_debug("SessionId source: URL parameter or json body");
    ctxt.session = session_manager_.get_session(url_session_id.value());
  }

  if (use_jwt && jwt_secret_.empty()) {
    throw http::Error{HttpStatusCode::BadRequest,
                      "Bad request - bearer not allowed."};
  }

  AcceptInfo ac;
  auto peer_host = get_peer_host(ctxt);
  if (!hosts_rate_.allow(peer_host, &ac)) {
    if (ac.reason == BlockReason::kRateExceeded)
      log_warning("Too many requests from host: '%s'.", peer_host.c_str());
    throw_max_rate_exceeded(ac.next_request_allowed_after);
  }
  selected_handler = choose_authentication_handler(ctxt, service_id, auth_app);

  // Ensure that all code paths, had selected the handlers.
  assert(nullptr != selected_handler.get());

  ctxt.selected_handler = selected_handler;
  if (ctxt.session) {
    if (ctxt.session->get_authorization_handler_id() !=
        selected_handler->get_id()) {
      log_debug(
          "SessionId source: resetting because of wrong handler "
          "id");
      session_manager_.remove_session(ctxt.session);
      ctxt.session = nullptr;
    }
  }

  using namespace std::literals::string_literals;

  if (!ctxt.session) {
    std::optional<std::string> handler_spcific_session_id =
        selected_handler->get_session_id_from_request_data(ctxt);

    if (handler_spcific_session_id.has_value()) {
      ctxt.session = session_manager_.get_session_secondary_id(
          handler_spcific_session_id.value());
      if (ctxt.session) {
        log_debug("SessionId source: from-handler id=%s",
                  ctxt.session->get_session_id().c_str());
      }
    }
  }

  if (!ctxt.session) {
    ctxt.session = session_manager_.new_session(
        selected_handler->get_id(),
        get_session_cookie_key_name(selected_handler->get_id()));
    ctxt.session->generate_token = use_jwt;
    // For now we set in only in case of authentication,
    // Its needed for building full-urls for redirection authentication-server.
    //
    // We do not set it for sessions created from JWT tokens (received from
    // other MRS instances).
    ctxt.session->proto = proto;
    ctxt.session->host = host;

    log_debug("SessionId source: new id=%s",
              ctxt.session->get_session_id().c_str());
  }

  assert(nullptr != session);
  ctxt.session->handler_name = selected_handler->get_entry().app_name;

  if (selected_handler->authorize(ctxt, ctxt.session, out_user)) {
    return true;
  }

  return false;
}

users::UserManager *AuthorizeManager::get_user_manager() {
  return &user_manager_;
}

bool AuthorizeManager::is_authorized(ServiceId service_id,
                                     rest::RequestContext &ctxt,
                                     AuthUser *user) {
  if (auto session = get_session_id_from_cookie(service_id, ctxt.cookies);
      session) {
    log_debug("Session source: cookie");
    ctxt.session = session;
  }

  log_debug(
      "AuthorizeManager::is_authorized(service_id:%s, session_id:%s, "
      "can_use_jwt:%s)",
      service_id.to_string().c_str(),
      (ctxt.session ? ctxt.session->get_session_id().c_str() : "<NONE>"),
      (jwt_secret_.empty() ? "no" : "yes"));

  if (!ctxt.session) {
    if (!jwt_secret_.empty()) {
      auto jwt = get_bearer_token_jwt(ctxt.get_in_headers());

      ctxt.session = authorize_jwt(service_id, jwt);
    }
  }

  if (!ctxt.session) return false;

  if (ctxt.session->state == Session::kUserVerified) {
    *user = ctxt.session->user;
    return true;
  }

  ctxt.session = nullptr;

  return false;
}

void AuthorizeManager::discard_current_session(ServiceId id,
                                               http::Cookie *cookies) {
  auto session_cookie_key = get_session_cookie_key_name(id);
  auto session_identifier = cookies->get(session_cookie_key);
  session_manager_.remove_session(session_identifier);
}

void AuthorizeManager::clear() { container_.clear(); }

void AuthorizeManager::update_users_cache(
    const ChangedUsersIds &changed_users_ids) {
  get_user_manager()->update_users_cache(changed_users_ids);
  for (auto &auth_handler : container_) {
    auth_handler->get_user_manager().update_users_cache(changed_users_ids);
  }
}

}  // namespace authentication
}  // namespace mrs
