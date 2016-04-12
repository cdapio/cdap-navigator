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
import co.cask.cdap.metadata.config.AuditKafkaConfig;
import co.cask.cdap.metadata.config.NavigatorAppConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Subscribes to Kafka messages published for the CDAP Platform that contains the Audit messages.
 */
public final class AuditLogConsumer extends Kafka08ConsumerFlowlet<ByteBuffer, ByteBuffer> {
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogConsumer.class);
  private static final Gson GSON = new Gson();

  // TODO: Add a way to reset the offset
  private KeyValueTable offsetStore;

  private OutputEmitter<String> emitter;
  private AuditKafkaConfig auditKafkaConfig;

  private String offsetDatasetName;

  public AuditLogConsumer(AuditKafkaConfig auditKafkaConfig) {
    this.offsetDatasetName = auditKafkaConfig.getOffsetDataset();
    verifyConfig(auditKafkaConfig);
  }

  public AuditLogConsumer() {
    // no-op
  }

  @VisibleForTesting
  static void verifyConfig(AuditKafkaConfig auditKafkaConfig) {
    // Verify if the configuration is right
    if (Strings.isNullOrEmpty(auditKafkaConfig.getBrokerString()) &&
      Strings.isNullOrEmpty(auditKafkaConfig.getZookeeperString())) {
      throw new IllegalArgumentException("Should provide either a broker string or a zookeeper string for " +
                                           "Kafka Audit Messages subscription!");
    }

    if (Strings.isNullOrEmpty(auditKafkaConfig.getTopic())) {
      throw new IllegalArgumentException("Should provide a Kafka Topic for Kafka Audit Message subscription!");
    }

    if (auditKafkaConfig.getNumPartitions() <= 0) {
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
    auditKafkaConfig = appConfig.getAuditKafkaConfig();
    LOG.info("Configuring Audit Kafka Consumer : {}", auditKafkaConfig);
    offsetStore = getContext().getDataset(auditKafkaConfig.getOffsetDataset());
    if (!Strings.isNullOrEmpty(auditKafkaConfig.getZookeeperString())) {
      kafkaConfigurer.setZooKeeper(auditKafkaConfig.getZookeeperString());
    } else if (!Strings.isNullOrEmpty(auditKafkaConfig.getBrokerString())) {
      kafkaConfigurer.setBrokers(auditKafkaConfig.getBrokerString());
    }
    setupTopicPartitions(kafkaConfigurer);
  }

  @Override
  protected void handleInstancesChanged(KafkaConsumerConfigurer configurer) {
    setupTopicPartitions(configurer);
  }

  private void setupTopicPartitions(KafkaConsumerConfigurer configurer) {
    int partitions = auditKafkaConfig.getNumPartitions();
    int instanceId = getContext().getInstanceId();
    int instances = getContext().getInstanceCount();
    for (int i = 0; i < partitions; i++) {
      if ((i % instances) == instanceId) {
        configurer.addTopicPartition(auditKafkaConfig.getTopic(), i);
      }
    }
  }

  @Override
  protected void processMessage(ByteBuffer auditMessage) throws Exception {
    String data = Bytes.toString(auditMessage);
    emitter.emit(data);
  }
}
