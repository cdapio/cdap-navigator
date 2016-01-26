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

import com.google.common.base.Strings;

/**
 * Configuration for Navigator.
 */
public class NavigatorConfig {

  private static final int DEFAULT_NAVIGATOR_PORT = 7187;
  private static final String DEFAULT_NAVIGATOR_NAMESPACE = "CDAP";
  private static final String DEFAULT_FILE_FORMAT = "JSON";

  private final String navigatorHostName;
  private final String username;
  private final String password;

  // Optional parameters
  private final Integer navigatorPort;
  private final boolean autocommit;
  private final String namespace;
  private final String applicationURL;
  private final String fileFormat;
  private final String navigatorURL;
  private final String metadataParentURI;

  public NavigatorConfig(String navigatorHostName, String username, String password, Integer navigatorPort,
                         Boolean autocommit, String namespace, String applicationURL, String fileFormat,
                         String navigatorURL, String metadataParentURI) {
    this.navigatorHostName = navigatorHostName;
    this.username = username;
    this.password = password;
    this.navigatorPort = navigatorPort;
    this.autocommit = autocommit;
    this.namespace = namespace;
    this.applicationURL = applicationURL;
    this.fileFormat = fileFormat;
    this.navigatorURL = navigatorURL;
    this.metadataParentURI = metadataParentURI;
  }

  public NavigatorConfig(String navigatorHostName, String username, String password) {
    this(navigatorHostName, username, password, DEFAULT_NAVIGATOR_PORT, false, DEFAULT_NAVIGATOR_NAMESPACE,
         generateApplicationURL(navigatorHostName), DEFAULT_FILE_FORMAT,
         generateNavigatorURL(navigatorHostName, DEFAULT_NAVIGATOR_PORT),
         generateMetadataParentURI(navigatorHostName, DEFAULT_NAVIGATOR_PORT));
  }

  private static String generateApplicationURL(String navigatorHostName) {
    return String.format("http://%s", navigatorHostName);
  }

  private static String generateNavigatorURL(String navigatorHostName, Integer navigatorPort) {
    return String.format("http://%s:%d/api/v8", navigatorHostName, navigatorPort);
  }

  private static String generateMetadataParentURI(String navigatorHostName, Integer navigatorPort) {
    return String.format("http://%s:%d/api/v8/metadata/plugin", navigatorHostName, navigatorPort);
  }

  private static Integer getNavigatorPort(Integer navigatorPort) {
    return navigatorPort == null ? DEFAULT_NAVIGATOR_PORT : navigatorPort;
  }

  public String getNavigatorHostName() {
    return navigatorHostName;
  }

  public String getApplicationURL() {
    return Strings.isNullOrEmpty(applicationURL) ? generateApplicationURL(navigatorHostName) : applicationURL;
  }

  public String getFileFormat() {
    return Strings.isNullOrEmpty(fileFormat) ? DEFAULT_FILE_FORMAT : fileFormat;
  }

  public String getNavigatorURL() {
    return Strings.isNullOrEmpty(navigatorURL) ?
      generateNavigatorURL(navigatorHostName, getNavigatorPort(navigatorPort)) : navigatorURL;
  }

  public String getMetadataParentURI() {
    return Strings.isNullOrEmpty(metadataParentURI) ?
      generateMetadataParentURI(navigatorHostName, getNavigatorPort(navigatorPort)) : metadataParentURI;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getNamespace() {
    return namespace == null ? DEFAULT_NAVIGATOR_NAMESPACE : namespace;
  }

  public Boolean getAutocommit() {
    return autocommit;
  }
}
