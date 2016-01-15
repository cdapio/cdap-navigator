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

import co.cask.cdap.metadata.config.NavigatorConfig;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Utility class to convert {@link NavigatorConfig} to config map expected by {@link NavigatorPlugin}.
 */
public class NavigatorConfigConverter {

  public static Map<String, Object> convert(NavigatorConfig navigatorConfig) {
    return ImmutableMap.<String, Object>builder()
      .put("application_url", navigatorConfig.getApplicationURL())
      .put("file_format", navigatorConfig.getFileFormat())
      .put("navigator_url", navigatorConfig.getNavigatorURL())
      .put("metadata_parent_uri", navigatorConfig.getMetadataParentURI())
      .put("username", navigatorConfig.getUsername())
      .put("password", navigatorConfig.getPassword())
      .put("namespace", navigatorConfig.getNamespace())
      .put("autocommit", Boolean.toString(navigatorConfig.getAutocommit()))
      .build();
  }

  private NavigatorConfigConverter() {
    // no-op
  }
}
