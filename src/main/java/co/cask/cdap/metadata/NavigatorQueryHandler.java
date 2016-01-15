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

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.metadata.config.NavigatorAppConfig;
import com.cloudera.nav.sdk.client.MetadataQuery;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.ResultsBatch;
import com.google.gson.Gson;

import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Service to query Navigator Metadata. This is primarily used for testing as users are expected to use Navigator UI
 * to query Navigator metadata.
 */
public class NavigatorQueryHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new Gson();

  private NavApiCient navigatorClient;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    NavigatorAppConfig appConfig = GSON.fromJson(context.getApplicationSpecification().getConfiguration(),
                                                 NavigatorAppConfig.class);
    NavigatorPlugin navigatorPlugin = NavigatorPlugin.fromConfigMap(
      NavigatorConfigConverter.convert(appConfig.getNavigatorConfig()));
    navigatorClient = navigatorPlugin.getClient();
  }

  @POST
  @Path("/search/{query}")
  public void search(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("query") String queryString,
                     @QueryParam("limit") @DefaultValue("10") Integer limit,
                     @QueryParam("cursorMark") @DefaultValue("") String cursorMark) {
    ResultsBatch<Map<String, Object>> queryResults = navigatorClient.getEntityBatch(
      new MetadataQuery(queryString, limit, cursorMark));
    responder.sendJson(200, queryResults);
  }
}
