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
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.metadata.config.NavigatorAppConfig;
import co.cask.cdap.metadata.config.NavigatorConfig;
import co.cask.cdap.metadata.entity.ApplicationEntity;
import co.cask.cdap.metadata.entity.DatasetEntity;
import co.cask.cdap.metadata.entity.ProgramEntity;
import co.cask.cdap.metadata.entity.StreamEntity;
import co.cask.cdap.metadata.entity.UnsupportedEntityException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Deserializes {@link MetadataChangeRecord} and creates Navigator {@link Entity}s
 * and pushes them to Navigator.
 */
public final class NavigatorPublisher extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorPublisher.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  private NavigatorConfig navigatorConfig;
  private NavigatorPlugin navigatorPlugin;

  public static void verifyConfig(NavigatorConfig navigatorConfig) {
    if (Strings.isNullOrEmpty(navigatorConfig.getNavigatorHostName())) {
      throw new IllegalArgumentException("Navigator Hostname should be provided!");
    }

    if (Strings.isNullOrEmpty(navigatorConfig.getNamespace())) {
      throw new IllegalArgumentException("Navigator Namespace should be provided!");
    }

    if (navigatorConfig.getUsername() == null) {
      throw new IllegalArgumentException("Navigator User Name should be provided!");
    }

    if (navigatorConfig.getPassword() == null) {
      throw new IllegalArgumentException("Navigator Password should be provided!");
    }
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    NavigatorAppConfig appConfig = GSON.fromJson(context.getApplicationSpecification().getConfiguration(),
                                                 NavigatorAppConfig.class);
    navigatorConfig = appConfig.getNavigatorConfig();
    navigatorPlugin = NavigatorPlugin.fromConfigMap(NavigatorConfigConverter.convert(navigatorConfig));
  }

  @ProcessInput
  public void process(String serializedMetaData) {
    MetadataChangeRecord record = GSON.fromJson(serializedMetaData, MetadataChangeRecord.class);
    MetadataChangeRecord.MetadataDiffRecord metadataDiffRecord = record.getChanges();
    MetadataRecord addition = metadataDiffRecord.getAdditions();
    MetadataRecord deletion = metadataDiffRecord.getDeletions();
    Id.NamespacedId entityId = addition.getEntityId();
    try {
      navigatorPlugin.write(convertToEntity(entityId, addition.getTags(), addition.getProperties(),
                                            deletion.getTags(), deletion.getProperties()));
    } catch (UnsupportedEntityException ex) {
      LOG.warn("EntityType {} of Entity {} not supported. So ignoring this record.", entityId.getIdType(), entityId);
    }
  }

  private Entity convertToEntity(Id.NamespacedId entityId, Set<String> addTags, Map<String, String> addProperties,
                                 Set<String> deleteTags, Map<String, String> deleteProperties)
    throws UnsupportedEntityException {
    Entity entity;
    EntityType entityType = entityId.toEntityId().getEntity();
    switch (entityType) {
      case APPLICATION:
        Id.Application appId = (Id.Application) entityId;
        entity = new ApplicationEntity(new ApplicationId(appId.getNamespaceId(), appId.getId()));
        break;
      case PROGRAM:
        Id.Program programId = (Id.Program) entityId;
        entity = new ProgramEntity(new ProgramId(programId.getNamespaceId(), programId.getApplicationId(),
                                                 programId.getType(), programId.getId()));
        break;
      case DATASET:
        Id.DatasetInstance datasetId = (Id.DatasetInstance) entityId;
        entity = new DatasetEntity(new DatasetId(datasetId.getNamespaceId(), datasetId.getId()));
        break;
      case STREAM:
        Id.Stream streamId = (Id.Stream) entityId;
        entity = new StreamEntity(new StreamId(streamId.getNamespaceId(), streamId.getId()));
        break;
      default:
        throw new UnsupportedEntityException(entityType);
    }
    entity.setNamespace(navigatorConfig.getNamespace());
    entity.addTags(addTags);
    entity.addProperties(addProperties);
    entity.removeTags(deleteTags);
    entity.removeProperties(deleteProperties.keySet());
    LOG.trace("Navigator Entity {} : AdditionTags = {}, DeletionTags = {}, NewProperties = {}, DelProperties = {}",
              entity.getName(), entity.getTags().getNewTags(), entity.getTags().getDelTags(),
              entity.getProperties().getNewProperties(), entity.getProperties().getRemoveProperties());
    return entity;
  }
}
