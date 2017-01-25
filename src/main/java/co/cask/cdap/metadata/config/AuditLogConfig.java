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

import com.google.common.base.Objects;
import com.google.common.base.Strings;

/**
 * Configuration for Metadata Kafka subscription.
 */
public class AuditLogConfig {
  private static final String DEFAULT_NAMESPACE = "system";
  private static final String DEFAULT_TOPIC = "audit";
  private static final String DEFAULT_OFFSET_DATASET = "auditOffset";
  private static final int DEFAULT_LIMIT = 100;

  private final String namespace;
  private final String topic;
  private final String offsetDataset;
  private final Integer limit;

  public AuditLogConfig() {
    this.namespace = null;
    this.topic = null;
    this.offsetDataset = null;
    this.limit = null;
  }

  public AuditLogConfig(String namespace, String topic, String offsetDataset, Integer limit) {
    this.namespace = namespace;
    this.topic = topic;
    this.offsetDataset = offsetDataset;
    this.limit = limit;
  }

  public String getNamespace() {
    return Strings.isNullOrEmpty(namespace) ? DEFAULT_NAMESPACE : namespace;
  }

  public String getTopic() {
    return Strings.isNullOrEmpty(topic) ? DEFAULT_TOPIC : topic;
  }

  public String getOffsetDataset() {
    return Strings.isNullOrEmpty(offsetDataset) ? DEFAULT_OFFSET_DATASET : offsetDataset;
  }

  public int getLimit() {
    return limit != null ? limit : DEFAULT_LIMIT;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("namespace", getNamespace())
      .add("topic", getTopic())
      .add("offsetDataset", getOffsetDataset())
      .add("limit", getLimit())
      .toString();
  }
}
