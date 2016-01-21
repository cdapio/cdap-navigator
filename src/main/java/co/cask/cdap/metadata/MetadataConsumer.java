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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet;
import co.cask.cdap.kafka.flow.KafkaConfigurer;
import co.cask.cdap.kafka.flow.KafkaConsumerConfigurer;
import co.cask.cdap.metadata.config.MetadataKafkaConfig;
import co.cask.cdap.metadata.config.NavigatorAppConfig;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Subscribes to Kafka messages published for the CDAP Platform that contains the Metadata Change records.
 */
public final class MetadataConsumer extends Kafka08ConsumerFlowlet<ByteBuffer, ByteBuffer> {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataConsumer.class);
  private static final Gson GSON = new Gson();

  // TODO: Add a way to reset the offset
  private KeyValueTable offsetStore;

  private OutputEmitter<String> emitter;
  private MetadataKafkaConfig metadataKafkaConfig;

  private String offsetDatasetName;

  public MetadataConsumer(MetadataKafkaConfig metadataKafkaConfig) {
    this.offsetDatasetName = metadataKafkaConfig.getOffsetDataset();
  }

  public MetadataConsumer() {
  }

  public static void verifyConfig(MetadataKafkaConfig metadataKafkaConfig) {
    // Verify if the configuration is right
    if (Strings.isNullOrEmpty(metadataKafkaConfig.getBrokerString()) &&
      Strings.isNullOrEmpty(metadataKafkaConfig.getZookeeperString())) {
      throw new IllegalArgumentException("Should provide either a broker string or a zookeeper string for " +
                                           "Kafka Metadata subscription!");
    }

    if (Strings.isNullOrEmpty(metadataKafkaConfig.getTopic())) {
      throw new IllegalArgumentException("Should provide a Kafka Topic for Kafka Metadata subscription!");
    }

    if (metadataKafkaConfig.getNumPartitions() <= 0) {
      throw new IllegalArgumentException("Kafka Partitions should be > 0.");
    }
  }

  @Override
  protected void configure() {
    super.configure();
    createDataset(offsetDatasetName, KeyValueTable.class);
  }

  @Override
  protected KeyValueTable getOffsetStore() {
    return offsetStore;
  }

  @Override
  protected void configureKafka(KafkaConfigurer kafkaConfigurer) {
    NavigatorAppConfig appConfig = GSON.fromJson(getContext().getApplicationSpecification().getConfiguration(),
                                                 NavigatorAppConfig.class);
    metadataKafkaConfig = appConfig.getMetadataKafkaConfig();
    offsetStore = getContext().getDataset(metadataKafkaConfig.getOffsetDataset());
    if (!Strings.isNullOrEmpty(metadataKafkaConfig.getZookeeperString())) {
      kafkaConfigurer.setZooKeeper(metadataKafkaConfig.getZookeeperString());
    } else if (!Strings.isNullOrEmpty(metadataKafkaConfig.getBrokerString())) {
      kafkaConfigurer.setBrokers(metadataKafkaConfig.getBrokerString());
    }
    setupTopicPartitions(kafkaConfigurer);
  }

  @Override
  protected void handleInstancesChanged(KafkaConsumerConfigurer configurer) {
    setupTopicPartitions(configurer);
  }

  private void setupTopicPartitions(KafkaConsumerConfigurer configurer) {
    int partitions = metadataKafkaConfig.getNumPartitions();
    int instanceId = getContext().getInstanceId();
    int instances = getContext().getInstanceCount();
    for (int i = 0; i < partitions; i++) {
      if ((i % instances) == instanceId) {
        configurer.addTopicPartition(metadataKafkaConfig.getTopic(), i);
      }
    }
  }

  @Override
  protected void processMessage(ByteBuffer metadataKafkaMessage) throws Exception {
    String data = Bytes.toString(metadataKafkaMessage);
    emitter.emit(data);
  }
}
