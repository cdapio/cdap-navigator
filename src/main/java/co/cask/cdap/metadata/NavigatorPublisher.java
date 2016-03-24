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
import co.cask.cdap.metadata.entity.ArtifactEntity;
import co.cask.cdap.metadata.entity.DatasetEntity;
import co.cask.cdap.metadata.entity.NavigatorClientWriteException;
import co.cask.cdap.metadata.entity.ProgramEntity;
import co.cask.cdap.metadata.entity.StreamEntity;
import co.cask.cdap.metadata.entity.StreamViewEntity;
import co.cask.cdap.metadata.entity.UnsupportedEntityException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.Metadata;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Deserializes {@link MetadataChangeRecord} and creates Navigator {@link Entity}s and writes them to Navigator.
 */
public final class NavigatorPublisher extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorPublisher.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  private NavigatorConfig navigatorConfig;
  private NavigatorPlugin navigatorPlugin;

  public NavigatorPublisher(NavigatorConfig navigatorConfig) {
    verifyConfig(navigatorConfig);
  }

  @VisibleForTesting
  static void verifyConfig(NavigatorConfig navigatorConfig) {
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
    Map<String, Object> naviConfig = NavigatorConfigConverter.convert(navigatorConfig);
    LOG.info("Starting Navigator Plugin with configuration : {}", naviConfig);
    navigatorPlugin = NavigatorPlugin.fromConfigMap(naviConfig);
  }

  @ProcessInput
  public void process(String serializedMetaData) throws NavigatorClientWriteException {
    AuditMessage record = GSON.fromJson(serializedMetaData, AuditMessage.class);
    if (record.getType() != AuditType.METADATA_CHANGE) {
      // TODO: CDAP-5394 utilize DELETE messages to remove entities from Navigator
      return;
    }

    EntityId entityId = record.getEntityId();

    // All the AuditPayloads will be of MetadataPayload since we skip other types of Audit messages
    MetadataPayload payload = (MetadataPayload) record.getPayload();

    Map<MetadataScope, Metadata> additions = payload.getAdditions();
    Map<MetadataScope, Metadata> deletions = payload.getDeletions();

    // Navigator client does not differentiate between user and system tags/properties. Hence add/delete them without
    // any classification, one after the other.
    for (MetadataScope scope : additions.keySet()) {
      try {
        Entity entity = convertToEntity(entityId, additions.get(scope).getTags(), additions.get(scope).getProperties(),
                                        deletions.get(scope).getTags(), deletions.get(scope).getProperties());
        ResultSet resultSet = navigatorPlugin.write(entity);
        if (resultSet.hasErrors()) {
          throw new NavigatorClientWriteException(entity, resultSet);
        }
      } catch (UnsupportedEntityException ex) {
        LOG.warn("EntityType {} of Entity {} not supported. Ignoring this record.", entityId.getEntity(), entityId);
      }
    }
  }

  private Entity convertToEntity(EntityId entityId, Set<String> addTags, Map<String, String> addProperties,
                                 Set<String> deleteTags, Map<String, String> deleteProperties)
    throws UnsupportedEntityException {
    Entity entity;
    EntityType entityType = entityId.getEntity();
    switch (entityType) {
      case APPLICATION:
        entity = new ApplicationEntity((ApplicationId) entityId);
        break;
      case PROGRAM:
        entity = new ProgramEntity((ProgramId) entityId);
        break;
      case DATASET:
        entity = new DatasetEntity((DatasetId) entityId);
        break;
      case STREAM:
        entity = new StreamEntity((StreamId) entityId);
        break;
      case ARTIFACT:
        entity = new ArtifactEntity((ArtifactId) entityId);
        break;
      case STREAM_VIEW:
        entity = new StreamViewEntity((StreamViewId) entityId);
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
