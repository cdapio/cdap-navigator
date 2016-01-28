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
public class MetadataKafkaConfig {

  private static final String DEFAULT_OFFSET_DATASET = "kafkaOffset";
  private static final String DEFAULT_TOPIC = "cdap-metadata-updates";
  private static final Integer DEFAULT_PARTITIONS = 10;

  private final String zookeeperString;
  private final String brokerString;
  private final Integer numPartitions;
  private final String topic;

  private final String offsetDataset;

  public MetadataKafkaConfig(String zookeeperString, String brokerString,
                             String topic, int numPartitions, String offsetDataset) {
    this.zookeeperString = zookeeperString;
    this.brokerString = brokerString;
    this.topic = topic;
    this.numPartitions = numPartitions;
    this.offsetDataset = offsetDataset;
  }

  public String getZookeeperString() {
    return zookeeperString;
  }

  public String getBrokerString() {
    return brokerString;
  }

  public String getTopic() {
    return Strings.isNullOrEmpty(topic) ? DEFAULT_TOPIC : topic;
  }

  public Integer getNumPartitions() {
    return Objects.firstNonNull(numPartitions, DEFAULT_PARTITIONS);
  }

  public String getOffsetDataset() {
    return Strings.isNullOrEmpty(offsetDataset) ? DEFAULT_OFFSET_DATASET : offsetDataset;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("zookeeperString", getZookeeperString())
      .add("brokerString", getBrokerString())
      .add("numPartitions", getNumPartitions())
      .add("topic", getTopic())
      .add("offsetDataset", getOffsetDataset())
      .toString();
  }
}
