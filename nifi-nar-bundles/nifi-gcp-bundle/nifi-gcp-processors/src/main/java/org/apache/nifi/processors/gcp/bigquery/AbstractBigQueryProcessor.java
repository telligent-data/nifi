/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.gcp.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RetryParams;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.spi.BigQueryRpc;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;

import java.util.*;

public abstract class AbstractBigQueryProcessor extends AbstractGCPProcessor<BigQuery, BigQueryRpc, BigQueryOptions> {
    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship upon successful execution of the BigQuery operation.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship if the BigQuery operation fails.")
                    .build();

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(
                REL_SUCCESS, REL_FAILURE
        );
    }

    @Override
    protected BigQueryOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        final String projectId = context.getProperty(PROJECT_ID).getValue();
        final Integer retryCount = Integer.valueOf(context.getProperty(RETRY_COUNT).getValue());

        return BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .setRetryParams(RetryParams.newBuilder()
                    .setRetryMaxAttempts(retryCount).setRetryMinAttempts(retryCount)
                    .build())
                .build();
    }
}
