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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.junit.Assert.*;

public abstract class AbstractBigQueryTest {
    protected static final String PROJECT_ID = System.getProperty("test.gcp.project.id", "nifi-test-gcp-project");
    protected static final Integer RETRIES = 9;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    public TestRunner buildNewRunner(Processor processor) throws Exception {
        final GCPCredentialsService credentialsService = new GCPCredentialsControllerService();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("gcpCredentialsControllerService", credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(AbstractBigQueryProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, "gcpCredentialsControllerService");
        runner.setProperty(AbstractBigQueryProcessor.PROJECT_ID, PROJECT_ID);
        runner.setProperty(AbstractBigQueryProcessor.RETRY_COUNT, String.valueOf(RETRIES));

        runner.assertValid(credentialsService);

        return runner;
    }

    public abstract AbstractBigQueryProcessor getProcessor();

    @Mock
    protected BigQuery bigQuery;

    @Test
    public void testBigQueryStorageConfiguration() throws Exception {
        reset(bigQuery);
        final TestRunner runner = buildNewRunner(getProcessor());

        final AbstractBigQueryProcessor processor = getProcessor();
        final GoogleCredentials mockCredentials = mock(GoogleCredentials.class);

        final BigQueryOptions options = processor.getServiceOptions(runner.getProcessContext(),
                mockCredentials);

        assertEquals("Project IDs should match",
                PROJECT_ID, options.getProjectId());

        assertEquals("Retry counts should match",
                RETRIES.intValue(), options.getRetryParams().getRetryMinAttempts());

        assertEquals("Retry counts should match",
                RETRIES.intValue(), options.getRetryParams().getRetryMaxAttempts());

        assertSame("Credentials should be configured correctly",
                mockCredentials, options.getCredentials());
    }
}
