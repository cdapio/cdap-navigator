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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes {@link MetadataChangeRecord} and creates Navigator {@link Entity}s
 * and pushes them to Navigator.
 */
public final class NavigatorPublisher extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorPublisher.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  @ProcessInput
  public void process(String serializedMetaData) {
    MetadataChangeRecord record = GSON.fromJson(serializedMetaData, MetadataChangeRecord.class);
    LOG.info("Got this Metadata Change Record : {}", record);
  }
}
