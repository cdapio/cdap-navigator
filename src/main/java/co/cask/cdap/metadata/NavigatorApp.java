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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * CDAP Application that subscribes to Metadata information that are published to a Kafka Broker/Topic, by the CDAP
 * platform, and pushes it to Cloudera Navigator.
 */
public class NavigatorApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("NavigatorIntegration");
    setDescription("Application that pushes metadata to Navigator");
    addFlow(new MetadataFlow());
    // TODO: Get the name of the kvTable from appConfig
    createDataset("kafkaOffsets", KeyValueTable.class);
  }

}
