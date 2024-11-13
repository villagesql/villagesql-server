/*
  Copyright (c) 2022, 2024, Oracle and/or its affiliates.

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

#include "mrs/authentication/www_authentication_handler.h"

#include "helper/container/map.h"
#include "helper/json/rapid_json_to_struct.h"
#include "helper/json/text_to.h"
#include "mrs/http/error.h"
#include "mrs/rest/request_context.h"

#include "mysqlrouter/base64.h"

#include "mysql/harness/logging/logging.h"
IMPORT_LOG_FUNCTIONS()

namespace mrs {
namespace authentication {

namespace {

struct UserJsonData {
  std::optional<std::string> on_completion_redirect;
  std::optional<std::string> on_completion_close;
  std::string username;
  std::string password;
};

class CredentialOptions
    : public helper::json::RapidReaderHandlerStringValuesToStruct<
          UserJsonData> {
 public:
  uint64_t to_uint(const std::string &value) {
    return std::stoull(value.c_str());
  }

  void handle_object_value(const std::string &key,
                           const std::string &value) override {
    using std::to_string;
    if (key == "onCompletionRedirect") {
      result_.on_completion_redirect = value;
    } else if (key == "onCompletionClose") {
      result_.on_completion_close = value;
    } else if (key == "username") {
      result_.username = value;
    } else if (key == "password") {
      result_.password = value;
    }
  }
};

}  // namespace

static bool extract_user_credentials_from_token(
    [[maybe_unused]] const std::string &token,
    [[maybe_unused]] std::string *user,
    [[maybe_unused]] std::string *password) {
  auto result = Base64::decode(token.c_str());

  auto it = std::find(result.begin(), result.end(), ':');
  if (it == result.end()) return false;

  *user = std::string(result.begin(), it);
  *password = std::string(it + 1, result.end());

  return true;
}

const static char *kBasicSchema = "basic";

using AuthApp = mrs::database::entry::AuthApp;
using AuthUser = mrs::database::entry::AuthUser;
using Session = mrs::http::SessionManager::Session;

struct WwwAuthSessionData : mrs::http::SessionManager::Session::SessionData {};

bool WwwAuthenticationHandler::redirects(RequestContext &ctxt) const {
  log_debug("WwwAuthenticationHandler::redirects %s",
            (!ctxt.post_authentication ? "yes" : "no"));
  return !ctxt.post_authentication;
}

bool WwwAuthenticationHandler::is_authorized(Session *session, AuthUser *user) {
  log_debug("WwwAuthenticationHandler::is_authorized");
  // TODO(lkotula): Right now we do not need to get the session_data (Shouldn't
  // be in review)
  auto session_data = session->get_data<WwwAuthSessionData>();
  if (!session_data) return false;
  if (session->state != Session::kUserVerified) {
    log_debug("WwwAuth: user not verified");
    return false;
  }

  log_debug("is_authorized returned true");
  *user = session->user;

  return true;
}

bool WwwAuthenticationHandler::authorize(RequestContext &ctxt, Session *session,
                                         AuthUser *out_user) {
  log_debug("WwwAuth: Authorize user");
  if (session->state == Session::kUserVerified) {
    log_debug("WwwAuth: user already verified");
    *out_user = session->user;
    return true;
  }

  auto method = ctxt.request->get_method();
  std::optional<Credentials> credentials_opt;

  if (HttpMethod::Get == method)
    credentials_opt = authorize_method_get(ctxt, session);
  else if (HttpMethod::Post == method)
    credentials_opt = authorize_method_post(ctxt, session);

  if (!credentials_opt.has_value()) throw_add_www_authenticate(kBasicSchema);

  if (verify_credential(credentials_opt.value(), &ctxt.sql_session_cache,
                        out_user)) {
    session->user = *out_user;
    session->state = Session::kUserVerified;
    return true;
  }

  if (HttpMethod::Get == method) throw_add_www_authenticate(kBasicSchema);

  return false;
}

static std::string find_header_or(const ::http::base::Headers &headers,
                                  const std::string &name,
                                  std::string &&default_value) {
  auto result = headers.find(name);
  if (!result) return std::move(default_value);

  return *result;
}

std::optional<WwwAuthenticationHandler::Credentials>
WwwAuthenticationHandler::authorize_method_get(RequestContext &ctxt,
                                               Session *session) {
  auto url = ctxt.get_http_url();

  url.get_if_query_parameter("onCompletionRedirect",
                             &session->users_on_complete_url_redirection);
  url.get_if_query_parameter("onCompletionClose",
                             &session->users_on_complete_timeout);

  auto authorization =
      find_header_or(ctxt.get_in_headers(), kAuthorization, "");
  if (authorization.empty()) {
    log_debug("WwwAuth: no authorization selected, retry?");
    throw_add_www_authenticate(kBasicSchema);
  }

  auto args = mysql_harness::split_string(authorization, ' ', false);
  const std::string &auth_schema =
      mysql_harness::make_lower(args.size() > 0 ? args[0] : "");
  const std::string &auth_token = args.size() > 1 ? args[1] : "";

  if (auth_schema.empty() || auth_schema != kBasicSchema) {
    log_debug("WwwAuth: no authorization scheme, retry?");
    throw_add_www_authenticate(kBasicSchema);
  }

  if (authorization.empty()) {
    log_debug("WwwAuth: no authorization token, retry?");
    throw_add_www_authenticate(kBasicSchema);
  }

  Credentials result;
  if (extract_user_credentials_from_token(auth_token, &result.user,
                                          &result.password))
    return result;

  return {};
}

std::optional<WwwAuthenticationHandler::Credentials>
WwwAuthenticationHandler::authorize_method_post(RequestContext &ctxt,
                                                Session *session) {
  const UserJsonData user_post_data =
      helper::json::text_to_handler<CredentialOptions>(
          ctxt.request->get_input_body());

  if (user_post_data.on_completion_redirect.has_value())
    session->users_on_complete_url_redirection =
        user_post_data.on_completion_redirect.value();

  if (user_post_data.on_completion_close.has_value())
    session->users_on_complete_timeout =
        user_post_data.on_completion_close.value();

  ctxt.post_authentication = true;

  return {{user_post_data.username, user_post_data.password}};
}

const AuthApp &WwwAuthenticationHandler::get_entry() const { return entry_; }

void WwwAuthenticationHandler::throw_add_www_authenticate(const char *schema) {
  class ErrorAddWwwBasicAuth : public http::ErrorChangeResponse {
   public:
    ErrorAddWwwBasicAuth(const std::string &schema) : schema_{schema} {}

    const char *name() const override { return "ErrorAddWwwBasicAuth"; }
    bool retry() const override { return true; }
    http::Error change_response(::http::base::Request *request) const override {
      request->get_output_headers().add(kWwwAuthenticate, schema_.c_str());

      return http::Error{HttpStatusCode::Unauthorized};
    }

    const std::string schema_;
  };

  throw ErrorAddWwwBasicAuth(schema);
}

}  // namespace authentication
}  // namespace mrs
