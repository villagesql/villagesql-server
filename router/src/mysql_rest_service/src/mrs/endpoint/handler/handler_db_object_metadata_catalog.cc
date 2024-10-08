/*
  Copyright (c) 2021, 2024, Oracle and/or its affiliates.

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

#include "mrs/endpoint/handler/handler_db_object_metadata_catalog.h"

#ifdef RAPIDJSON_NO_SIZETYPEDEFINE
#include "my_rapidjson_size_t.h"
#endif

#include <rapidjson/document.h>
#include <rapidjson/writer.h>

#include "mrs/endpoint/handler/url_paths.h"
#include "mrs/endpoint/handler/utilities.h"
#include "mrs/http/error.h"
#include "mrs/rest/request_context.h"

#include "helper/mysql_column_types.h"

namespace mrs {
namespace endpoint {
namespace handler {

using HttpResult = mrs::rest::Handler::HttpResult;
using Authorization = mrs::rest::Handler::Authorization;

namespace {

auto get_regex_path_object_metadata_catalog(
    std::weak_ptr<DbObjectEndpoint> endpoint) {
  using namespace std::string_literals;

  auto endpoint_obj = lock(endpoint);
  if (!endpoint_obj) return ""s;

  auto endpoint_sch = endpoint_obj->get_parent_ptr();
  if (!endpoint_sch) return ""s;

  return regex_path_obj_metadata_catalog(endpoint_sch->get_url_path(),
                                         endpoint_obj->get()->request_path);
}

}  // namespace

HandlerDbObjectMetadataCatalog::HandlerDbObjectMetadataCatalog(
    std::weak_ptr<DbObjectEndpoint> endpoint,
    mrs::interface::AuthorizeManager *auth_manager)
    : Handler(get_endpoint_host(endpoint),
              /*regex-path: ^/service/schema/metadata-catalog/object$*/
              {get_regex_path_object_metadata_catalog(endpoint)},
              get_endpoint_options(lock(endpoint)), auth_manager),
      endpoint_{endpoint} {
  auto ep = lock(endpoint_);
  auto ep_parent = lock_parent(ep);
  assert(ep_parent);
  entry_ = ep->get();
  schema_entry_ = ep_parent->get();
  url_obj_ = ep->get_url().join();
  url_obj_metadata_catalog_ =
      url_obj_metadata_catalog(ep_parent->get_url(), entry_->request_path);
  url_sch_metadata_catalog_ = url_sch_metadata_catalog(ep_parent->get_url());
}

HttpResult HandlerDbObjectMetadataCatalog::handle_get(rest::RequestContext *) {
  auto endpoint = lock_or_throw_unavail(endpoint_);
  rapidjson::Document json_doc;
  {
    rapidjson::Document::AllocatorType &allocator = json_doc.GetAllocator();
    rapidjson::Value primary_key(rapidjson::kArrayType);
    rapidjson::Value links(rapidjson::kArrayType);
    rapidjson::Value members(rapidjson::kArrayType);
    auto obj = entry_->object_description;

    for (auto &c : obj->fields) {
      auto column = std::dynamic_pointer_cast<mrs::database::entry::Column>(c);
      if (!column || !column->enabled) continue;
      auto data_type =
          helper::from_mysql_txt_column_type(column->datatype.c_str())
              .type_json;

      rapidjson::Value json_column(rapidjson::kObjectType);
      json_column.AddMember(
          "name", rapidjson::Value(c->name.c_str(), allocator), allocator);
      json_column.AddMember(
          "type",
          rapidjson::Value(helper::to_string(data_type).c_str(), allocator),
          allocator);

      members.PushBack(json_column, allocator);

      if (column->is_primary) {
        primary_key.PushBack(rapidjson::Value(c->name.c_str(), allocator),
                             allocator);
      }
    }

    rapidjson::Value json_link_coll(rapidjson::kObjectType);
    rapidjson::Value json_link_can(rapidjson::kObjectType);
    rapidjson::Value json_link_desc(rapidjson::kObjectType);

    json_link_coll.AddMember("rel", "collection", allocator);
    json_link_coll.AddMember(
        "href", rapidjson::Value(url_sch_metadata_catalog_.c_str(), allocator),
        allocator);
    json_link_coll.AddMember("mediaType", "application/json", allocator);

    json_link_can.AddMember("rel", "canonical", allocator);
    json_link_can.AddMember(
        "href", rapidjson::Value(url_obj_metadata_catalog_.c_str(), allocator),
        allocator);

    json_link_desc.AddMember("rel", "describes", allocator);
    json_link_desc.AddMember(
        "href", rapidjson::Value(url_obj_.c_str(), allocator), allocator);

    links.PushBack(json_link_coll, allocator);
    links.PushBack(json_link_can, allocator);
    links.PushBack(json_link_desc, allocator);

    json_doc.SetObject()
        .AddMember("name",
                   rapidjson::Value(entry_->request_path.c_str(), allocator),
                   allocator)
        .AddMember("primaryKey", primary_key, allocator)
        .AddMember("members", members, allocator)
        .AddMember("links", links, allocator);
  }
  rapidjson::StringBuffer json_buf;
  {
    rapidjson::Writer<rapidjson::StringBuffer> json_writer(json_buf);

    json_doc.Accept(json_writer);
  }
  return std::string(json_buf.GetString(), json_buf.GetLength());
}

HttpResult HandlerDbObjectMetadataCatalog::handle_post(
    [[maybe_unused]] rest::RequestContext *ctxt,
    [[maybe_unused]] const std::vector<uint8_t> &document) {
  assert(false && "Should be never called. Stubbing parent class.");
  throw http::Error(HttpStatusCode::Forbidden);
}

HttpResult HandlerDbObjectMetadataCatalog::handle_delete(
    [[maybe_unused]] rest::RequestContext *ctxt) {
  assert(false && "Should be never called. Stubbing parent class.");
  throw http::Error(HttpStatusCode::Forbidden);
}

HttpResult HandlerDbObjectMetadataCatalog::handle_put(
    [[maybe_unused]] rest::RequestContext *ctxt) {
  assert(false && "Should be never called. Stubbing parent class.");
  throw http::Error(HttpStatusCode::Forbidden);
}

uint32_t HandlerDbObjectMetadataCatalog::get_access_rights() const {
  using Operation = mrs::database::entry::Operation;

  return Operation::valueRead;
}

Authorization HandlerDbObjectMetadataCatalog::requires_authentication() const {
  bool requires_auth =
      entry_->requires_authentication || schema_entry_->requires_auth;
  return requires_auth ? Authorization::kCheck : Authorization::kNotNeeded;
}

UniversalId HandlerDbObjectMetadataCatalog::get_service_id() const {
  return schema_entry_->service_id;
}

UniversalId HandlerDbObjectMetadataCatalog::get_db_object_id() const {
  return entry_->id;
}

UniversalId HandlerDbObjectMetadataCatalog::get_schema_id() const {
  return schema_entry_->id;
}

void HandlerDbObjectMetadataCatalog::authorization(rest::RequestContext *ctxt) {
  throw_unauthorize_when_check_auth_fails(ctxt);
}

}  // namespace handler
}  // namespace endpoint
}  // namespace mrs
