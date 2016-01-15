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

import co.cask.cdap.metadata.config.MetadataKafkaConfig;
import co.cask.cdap.metadata.config.NavigatorAppConfig;
import co.cask.cdap.metadata.config.NavigatorConfig;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test for {@link NavigatorApp}.
 */
public class NavigatorAppTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void test() throws Exception {
    // Deploy the Navigator Application
    NavigatorConfig navigatorConfig = new NavigatorConfig("naviclus.dev.continuuity.net", "user", "pass");
    MetadataKafkaConfig metadataKafkaConfig = new MetadataKafkaConfig(null, "source.dev.continuuity.net:9092",
                                                                      "cdap-metadata-updates", 10, null);
    NavigatorAppConfig appConfig = new NavigatorAppConfig(navigatorConfig, metadataKafkaConfig);
    ApplicationManager appManager = deployApplication(NavigatorApp.class, appConfig);
    appManager.stopAll();
  }
}
