/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.metadata.config;

import co.cask.cdap.api.Config;
import co.cask.cdap.metadata.NavigatorApp;

/**
 * Application Config for {@link NavigatorApp}.
 */
public class NavigatorAppConfig extends Config {

  private final NavigatorConfig navigatorConfig;
  private final AuditKafkaConfig auditKafkaConfig;

  public NavigatorAppConfig(NavigatorConfig navigatorConfig, AuditKafkaConfig auditKafkaConfig) {
    this.navigatorConfig = navigatorConfig;
    this.auditKafkaConfig = auditKafkaConfig;
  }

  public NavigatorConfig getNavigatorConfig() {
    return navigatorConfig;
  }

  public AuditKafkaConfig getAuditKafkaConfig() {
    return auditKafkaConfig;
  }
}
