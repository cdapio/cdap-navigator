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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.proto.id.DatasetId;
import com.cloudera.nav.sdk.model.MD5IdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;

/**
 * CDAP {@link Dataset} {@link Entity}
 */
public class DatasetEntity extends Entity {

  private final DatasetId datasetId;

  public DatasetEntity(DatasetId datasetId) {
    this.datasetId = datasetId;
    setName(datasetId.toString());
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.HIVE;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.DATASET;
  }

  @Override
  public String generateId() {
    return MD5IdGenerator.generateIdentity(datasetId.getNamespace(), datasetId.getDataset());
  }
}
