/*
  Copyright (c) 2023, 2024, Oracle and/or its affiliates.

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

#include "client/authentication.h"

#include "helper/container/map.h"
#include "helper/http/url.h"
#include "helper/json/rapid_json_to_struct.h"
#include "helper/json/serializer_to_text.h"
#include "helper/json/text_to.h"
#include "helper/string/hex.h"
#include "helper/string/random.h"

#include "mysqlrouter/base64.h"
#include "mysqlrouter/component/http_auth_method_basic.h"

namespace mrs_client {

namespace {
void add_authorization_header(HttpClientRequest *request,
                              const std::string &user,
                              const std::string &password) {
  HttpAuthMethodBasic basic;
  const char *kAuthorization = "Authorization";
  std::string auth_string{basic.kMethodName};
  auth_string += " " + basic.encode_authorization({user, password});
  request->add_header(kAuthorization, auth_string.c_str());
}

std::string get_authorization_json(
    const std::string &user, const std::string &password, const bool use_jwt,
    const std::map<std::string, std::string> &url_query) {
  helper::json::SerializerToText stt;

  {
    auto obj = stt.add_object();
    obj->member_add_value("sessionType", (use_jwt ? "bearer" : "cookie"));
    obj->member_add_value("username", user);
    obj->member_add_value("password", password);

    for (const auto &[k, v] : url_query) {
      obj->member_add_value(k, v);
    }
  }
  return stt.get_result();
}

struct JsonResponse {
  std::optional<std::string> access_token;
  std::optional<std::string> session_id;
};

namespace cvt {
using std::to_string;
const std::string &to_string(const std::string &str) { return str; }
}  // namespace cvt

class ParseJsonResponse
    : public helper::json::RapidReaderHandlerToStruct<JsonResponse> {
 public:
  template <typename ValueType>
  void handle_object_value(const std::string &key, const ValueType &vt) {
    if (key == "accessToken") {
      result_.access_token = cvt::to_string(vt);
    } else if (key == "sessionId") {
      result_.session_id = cvt::to_string(vt);
    }
  }

  template <typename ValueType>
  void handle_value(const ValueType &vt) {
    const auto &key = get_current_key();
    if (is_object_path()) {
      handle_object_value(key, vt);
    }
  }

  bool String(const Ch *v, rapidjson::SizeType v_len, bool) override {
    handle_value(std::string{v, v_len});
    return true;
  }

  bool RawNumber(const Ch *v, rapidjson::SizeType v_len, bool) override {
    handle_value(std::string{v, v_len});
    return true;
  }

  bool Bool(bool v) override {
    handle_value(v);
    return true;
  }
};

}  // namespace

Result Authentication::do_basic_flow(HttpClientRequest *request,
                                     std::string url, const std::string &user,
                                     const std::string &password,
                                     const SessionType st) {
  add_authorization_header(request, user, password);

  if (st == SessionType::kJWT) {
    url = url + "?sessionType=bearer";
  }

  bool set_new_cookies = st == SessionType::kCookie;
  auto result = request->do_request(HttpMethod::Get, url, {}, set_new_cookies);

  if (result.status == HttpStatusCode::NotFound) return result;

  if (result.status != HttpStatusCode::TemporaryRedirect) {
    return result;
    //    throw std::runtime_error(
    //        "Expected redirection flow, received other status code.");
  }

  auto location = find_in_headers(result.headers, "Location");
  if (location.empty())
    throw std::runtime_error(
        "HTTP redirect, doesn't contain `Location` header.");

  // Parameter value
  std::string pvalue;
  http::base::Uri u{location};
  std::map<std::string, std::string> parameters;

  helper::http::Url helper_uri(u);

  if (!helper_uri.get_if_query_parameter("login", &pvalue))
    throw std::runtime_error(
        "HTTP redirect, doesn't contain `login` query parameter.");

  if (pvalue != "success")
    throw std::runtime_error("HTTP redirect, points that login failed.");

  if (st == SessionType::kJWT) {
    pvalue.clear();
    if (!helper_uri.get_if_query_parameter("accessToken", &pvalue)) {
      throw std::runtime_error(
          "HTTP redirect, doesn't contain `accessToken` query parameter.");
    }

    if (pvalue.empty())
      throw std::runtime_error(
          "HTTP redirect, doesn't contain valid JWT token.");
    std::string header{"Authorization:Bearer "};
    header += pvalue;
    request->get_session()->add_header(&header[0]);
  }

  return {HttpStatusCode::Ok, {}, {}};
}

Result Authentication::do_basic_json_flow(HttpClientRequest *request,
                                          std::string url,
                                          const std::string &user,
                                          const std::string &password,
                                          const SessionType st) {
  http::base::Uri u{url};
  bool use_jwt = st == SessionType::kJWT;
  bool use_cookies = !use_jwt;

  auto body =
      get_authorization_json(user, password, use_jwt, u.get_query_elements());
  auto result = request->do_request(HttpMethod::Post, url, body, use_cookies);

  if (result.status != HttpStatusCode::Ok) return result;

  auto [access_token, session_id] =
      helper::json::text_to_handler<ParseJsonResponse>(result.body);

  if (session_id.has_value() && access_token.has_value())
    throw std::runtime_error(
        "Response contains both `session_id` and `access_token` which is not "
        "allowed.");

  if (!session_id.has_value() && !access_token.has_value())
    throw std::runtime_error(
        "Response doesn't contains neither `session_id` nor `access_token`.");

  if (use_jwt && session_id.has_value()) {
    throw std::runtime_error(
        "Application requested JWT token, but received a cookie-session-id.");
  }

  if (use_cookies && access_token.has_value()) {
    throw std::runtime_error(
        "Application requested cookie-session-id, but received a JWT token.");
  }

  if (use_jwt) {
    std::string header{"Authorization:Bearer "};
    header += access_token.value();
    request->get_session()->add_header(&header[0]);
  }

  // Ignore the `use_cookies`, the cookie should be already set in headers.
  // The session_id - cookie name is service dependent, thus we would also need
  // to transfer service_id.

  return {HttpStatusCode::Ok, {}, {}};
}

class Scram {
 public:
  using Base64NoPadd =
      Base64Base<Base64Alphabet::Base64Url, Base64Endianess::BIG, true, '='>;

  std::string get_initial_auth_data(const std::string &user) {
    const static std::string kParameterAuthData = "data";
    using namespace std::string_literals;
    client_first_ = "n="s + user + ",r=" + generate_nonce(10);
    return kParameterAuthData + "=" +
           Base64NoPadd::encode(as_array("n,," + client_first_));
  }

  std::string as_string(const std::vector<unsigned char> &c) {
    return std::string(c.begin(), c.end());
  }

  std::vector<uint8_t> as_array(const std::string &s) {
    return std::vector<uint8_t>(s.begin(), s.end());
  }

  std::string generate_nonce(std::size_t size) {
    return helper::string::hex(
        helper::generate_string<helper::Generator8bitsValues>(size));
  }

  std::string client_first_;
};

Result Authentication::do_scram_flow(HttpClientRequest *request,
                                     std::string url, const std::string &user,
                                     const std::string &,
                                     const SessionType st) {
  Scram scram;

  url = url + "?" + scram.get_initial_auth_data(user);
  //  if (st == SessionType::kJWT) {
  //    url = url + "?sessionType=bearer";
  //  }

  bool set_new_cookies = st == SessionType::kCookie;
  auto result = request->do_request(HttpMethod::Get, url, {}, set_new_cookies);

  if (result.status == HttpStatusCode::NotFound) return result;

  if (result.status != HttpStatusCode::TemporaryRedirect)
    throw std::runtime_error(
        "Expected redirection flow, received other status code.");

  auto location = find_in_headers(result.headers, "Location");
  if (location.empty())
    throw std::runtime_error(
        "HTTP redirect, doesn't contain `Location` header.");

  // Parameter value
  std::string pvalue;
  http::base::Uri u(location);
  std::map<std::string, std::string> parameters;

  helper::http::Url helper_uri(u);

  if (!helper_uri.get_if_query_parameter("login", &pvalue))
    throw std::runtime_error(
        "HTTP redirect, doesn't contain `login` query parameter.");

  if (pvalue != "success")
    throw std::runtime_error("HTTP redirect, points that login failed.");

  if (st == SessionType::kJWT) {
    pvalue.clear();
    if (!helper_uri.get_if_query_parameter("accessToken", &pvalue)) {
      throw std::runtime_error(
          "HTTP redirect, doesn't contain `accessToken` query parameter.");
    }

    if (pvalue.empty())
      throw std::runtime_error(
          "HTTP redirect, doesn't contain valid JWT token.");
    std::string header{"Authorization:Bearer "};
    header += pvalue;
    request->get_session()->add_header(&header[0]);
  }

  return {HttpStatusCode::Ok, {}, {}};
}

}  // namespace mrs_client
