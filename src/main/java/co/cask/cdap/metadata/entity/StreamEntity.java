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

package co.cask.cdap.metadata.entity;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.proto.id.StreamId;
import com.cloudera.nav.sdk.model.MD5IdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;

/**
 * CDAP {@link Stream} {@link Entity}
 */
@MClass(model = "cdap_stream_entity")
public class StreamEntity extends Entity {
  private final StreamId streamId;

  @MProperty
  private final String metaClassName;

  public StreamEntity(StreamId streamId) {
    this.streamId = streamId;
    this.metaClassName = "streamMetaClassName";
    setName(streamId.toString());
  }

  public String getMetaClassName() {
    return metaClassName;
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.DATASET;
  }

  @Override
  public String generateId() {
    return MD5IdGenerator.generateIdentity(streamId.toString());
  }
}
