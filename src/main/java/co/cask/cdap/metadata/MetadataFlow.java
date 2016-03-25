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

package co.cask.cdap.metadata;

import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.metadata.config.NavigatorAppConfig;

/**
 * Metadata Flow that contains two flowlets - metadataConsumer subscribes to Metadata Kafka messages and forwards it to
 * navigatorPublisher flowlet that writes that metadata info to Navigator.
 */
public final class MetadataFlow extends AbstractFlow {
  public static final String FLOW_NAME = "MetadataFlow";
  private final NavigatorAppConfig navigatorAppConfig;

  public MetadataFlow(NavigatorAppConfig navigatorAppConfig) {
    this.navigatorAppConfig = navigatorAppConfig;
  }

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("Flow that subscribes to Metadata changes and propagates the same to Navigator");
    addFlowlet("auditLogConsumer", new AuditLogConsumer(navigatorAppConfig.getAuditKafkaConfig()));
    addFlowlet("navigatorPublisher", new NavigatorPublisher(navigatorAppConfig.getNavigatorConfig()));
    connect("auditLogConsumer", "navigatorPublisher");
  }
}
