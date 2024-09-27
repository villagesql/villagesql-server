/*
  Copyright (c) 2024, Oracle and/or its affiliates.

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

#ifndef ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_OPTION_ENDPOINT_H_
#define ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_OPTION_ENDPOINT_H_

#include <memory>
#include <vector>

#include "mrs/interface/endpoint_base.h"
#include "mrs/interface/handler_factory.h"
#include "mrs/interface/rest_handler.h"

namespace mrs {
namespace endpoint {

class OptionEndpoint : public mrs::interface::EndpointBase {
 public:
  using HandlerFactoryPtr = std::shared_ptr<mrs::interface::HandlerFactory>;
  using Handler = mrs::interface::RestHandler;
  using HandlerPtr = std::shared_ptr<Handler>;

 public:
  OptionEndpoint(UniversalId service_id, EndpointConfigurationPtr configuration,
                 HandlerFactoryPtr factory);

 protected:
  void update() override;

  UniversalId service_id_;
  std::vector<HandlerPtr> handlers_;
  HandlerFactoryPtr factory_;
};

}  // namespace endpoint
}  // namespace mrs

#endif /* ROUTER_SRC_MYSQL_REST_SERVICE_SRC_MRS_ENDPOINT_OPTION_ENDPOINT_H_ */
