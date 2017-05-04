package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BigQueryQueryJobTest extends AbstractBigQueryJobTest {
    private final static JobInfo.CreateDisposition CREATE_DISPOSITION = JobInfo.CreateDisposition.CREATE_NEVER;
    private final static JobInfo.WriteDisposition WRITE_DISPOSITION = JobInfo.WriteDisposition.WRITE_TRUNCATE;

    private final static String DESTINATION_DATASET = "test-dataset";
    private final static String DESTINATION_TABLE = "test-table";
    private final static String QUERY = "test-query";
    private final static Boolean USE_LEGACY_SQL = true;
    private final static Boolean ALLOW_LARGE_RESULTS = false;
    private final static Boolean USE_QUERY_CACHE = true;
    private final static Boolean FLATTEN_RESULTS = true;
    private final static QueryJobConfiguration.Priority PRIORITY = QueryJobConfiguration.Priority.BATCH;
    private final static Boolean DRY_RUN = true;

    @Override
    public BigQueryQueryJob getProcessor() {
        return new BigQueryQueryJob() {
            @Override
            protected BigQuery getCloudService() {
                return bigQuery;
            }
        };
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(BigQueryQueryJob.CREATE_DISPOSITION, CREATE_DISPOSITION.name());
        runner.setProperty(BigQueryQueryJob.WRITE_DISPOSITION, WRITE_DISPOSITION.name());
        runner.setProperty(BigQueryQueryJob.DESTINATION_DATASET, DESTINATION_DATASET);
        runner.setProperty(BigQueryQueryJob.DESTINATION_TABLE, DESTINATION_TABLE);
        runner.setProperty(BigQueryQueryJob.QUERY, QUERY);
        runner.setProperty(BigQueryQueryJob.USE_LEGACY_SQL, String.valueOf(USE_LEGACY_SQL));
        runner.setProperty(BigQueryQueryJob.ALLOW_LARGE_RESULTS, String.valueOf(ALLOW_LARGE_RESULTS));
        runner.setProperty(BigQueryQueryJob.USE_QUERY_CACHE, String.valueOf(USE_QUERY_CACHE));
        runner.setProperty(BigQueryQueryJob.FLATTEN_RESULTS, String.valueOf(FLATTEN_RESULTS));
        runner.setProperty(BigQueryQueryJob.PRIORITY, PRIORITY.name());
        runner.setProperty(BigQueryQueryJob.DRY_RUN, String.valueOf(DRY_RUN));
    }


    @Test
    public void testQueryJobConfiguration() throws Exception {
        reset(bigQuery);

        final BigQueryQueryJob queryJobProcessor = getProcessor();
        final TestRunner runner = buildNewRunner(queryJobProcessor);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();

        final FlowFile mockFlowFile = new MockFlowFile(1234L);
        final QueryJobConfiguration configuration = queryJobProcessor.buildJobConfiguration(
                runner.getProcessContext(), runner.getProcessSessionFactory().createSession(), mockFlowFile);

        assertEquals(WRITE_DISPOSITION, configuration.getWriteDisposition());
        assertEquals(CREATE_DISPOSITION, configuration.getCreateDisposition());
        assertEquals(DESTINATION_DATASET, configuration.getDestinationTable().getDataset());
        assertEquals(DESTINATION_TABLE, configuration.getDestinationTable().getTable());
        assertEquals(QUERY, configuration.getQuery());
        assertEquals(USE_LEGACY_SQL, configuration.useLegacySql());
        assertEquals(ALLOW_LARGE_RESULTS, configuration.allowLargeResults());
        assertEquals(USE_QUERY_CACHE, configuration.useQueryCache());
        assertEquals(FLATTEN_RESULTS, configuration.flattenResults());
        assertEquals(PRIORITY, configuration.getPriority());
        assertEquals(DRY_RUN, configuration.dryRun());
    }

    @Test
    public void testQueryJobConfigurationWithNoQueryAttribute() throws Exception {
        reset(bigQuery);

        final BigQueryQueryJob queryJobProcessor = getProcessor();
        final TestRunner runner = buildNewRunner(queryJobProcessor);
        addRequiredPropertiesToRunner(runner);
        runner.removeProperty(BigQueryQueryJob.QUERY);
        runner.assertValid();

        FlowFile testFlowFile = new MockFlowFile(1L);

        final ProcessSession mockSession = mock(ProcessSession.class);
        when(mockSession.read(any())).thenReturn(new ByteArrayInputStream(QUERY.getBytes()));
        final QueryJobConfiguration configuration = queryJobProcessor.buildJobConfiguration(
                runner.getProcessContext(), mockSession, testFlowFile);

        assertEquals(QUERY, configuration.getQuery());
    }

    @Test(expected = ProcessException.class)
    public void testQueryJobConfigurationWithNoQueryAttributeAndBadRead() throws Exception {
        reset(bigQuery);

        final BigQueryQueryJob queryJobProcessor = getProcessor();
        final TestRunner runner = buildNewRunner(queryJobProcessor);
        addRequiredPropertiesToRunner(runner);
        runner.removeProperty(BigQueryQueryJob.QUERY);
        runner.assertValid();

        FlowFile testFlowFile = new MockFlowFile(1L);

        final ProcessSession mockSession = mock(ProcessSession.class);
        final InputStream mockInputStream = mock(InputStream.class, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                throw new IOException("test exception");
            }
        });
        when(mockSession.read(any())).thenReturn(mockInputStream);
        queryJobProcessor.buildJobConfiguration(runner.getProcessContext(), mockSession, testFlowFile);
    }

    @Test
    public void testSuccessfulQueryJob() throws Exception {
        reset(bigQuery, job);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        ArgumentCaptor<JobInfo> jobInfoArgumentCaptor = ArgumentCaptor.forClass(JobInfo.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);

        when(job.waitFor(any(), any())).thenReturn(job);


        final JobStatus mockStatus = mock(JobStatus.class);
        when(job.getStatus()).thenReturn(mockStatus);
        when(mockStatus.getError()).thenReturn(null);

        runner.enqueue("testFlowFile", ImmutableMap.of(
                "testAttribute", "testAttributeValue"
        ));
        runner.assertValid();
        runner.run();

        verify(bigQuery).create(jobInfoArgumentCaptor.capture());

        final JobConfiguration configuration = jobInfoArgumentCaptor.getValue().getConfiguration();
        assertTrue(configuration instanceof QueryJobConfiguration);

        assertEquals(
                DESTINATION_TABLE,
                ((QueryJobConfiguration) configuration).getDestinationTable().getTable());

        runner.assertTransferCount(BigQueryQueryJob.REL_SUCCESS, 1);

        final MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(BigQueryQueryJob.REL_SUCCESS).get(0);

        assertEquals(
                DESTINATION_DATASET,
                successFlowFile.getAttribute("bigquery.dataset")
        );

        assertEquals(
                DESTINATION_TABLE,
                successFlowFile.getAttribute("bigquery.table")
        );

    }
}
