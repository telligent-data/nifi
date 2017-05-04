package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BigQueryCopyJobTest extends AbstractBigQueryJobTest {
    final static String SOURCE_DATASET = "test-source-dataset";
    final static String SOURCE_TABLE = "test-source-table";
    final static String DESTINATION_DATASET = "test-destination-dataset";
    final static String DESTINATION_TABLE = "test-destination-table";
    final static JobInfo.CreateDisposition CREATE_DISPOSITION = JobInfo.CreateDisposition.CREATE_NEVER;
    final static JobInfo.WriteDisposition WRITE_DISPOSITION = JobInfo.WriteDisposition.WRITE_TRUNCATE;


    @Mock
    Job job;

    @Override
    public BigQueryCopyJob getProcessor() {
        return new BigQueryCopyJob() {
            @Override
            protected BigQuery getCloudService() {
                return bigQuery;
            }
        };
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(BigQueryCopyJob.CREATE_DISPOSITION, CREATE_DISPOSITION.name());
        runner.setProperty(BigQueryCopyJob.WRITE_DISPOSITION, WRITE_DISPOSITION.name());
        runner.setProperty(BigQueryCopyJob.SOURCE_DATASET, SOURCE_DATASET);
        runner.setProperty(BigQueryCopyJob.SOURCE_TABLE, SOURCE_TABLE);
        runner.setProperty(BigQueryCopyJob.DESTINATION_DATASET, DESTINATION_DATASET);
        runner.setProperty(BigQueryCopyJob.DESTINATION_TABLE, DESTINATION_TABLE);
    }

    @Test
    public void testCopyJobConfiguration() throws Exception {
        reset(bigQuery);

        final BigQueryCopyJob copyJobProcessor = getProcessor();
        final TestRunner runner = buildNewRunner(copyJobProcessor);
        addRequiredPropertiesToRunner(runner);

        final FlowFile mockFlowFile = new MockFlowFile(1234L);
        final CopyJobConfiguration configuration = copyJobProcessor.buildJobConfiguration(
                runner.getProcessContext(), runner.getProcessSessionFactory().createSession(), mockFlowFile);

        assertEquals(1, configuration.getSourceTables().size());
        assertEquals(SOURCE_DATASET, configuration.getSourceTables().get(0).getDataset());
        assertEquals(SOURCE_TABLE, configuration.getSourceTables().get(0).getTable());
        assertEquals(DESTINATION_DATASET, configuration.getDestinationTable().getDataset());
        assertEquals(DESTINATION_TABLE, configuration.getDestinationTable().getTable());
        assertEquals(WRITE_DISPOSITION, configuration.getWriteDisposition());
        assertEquals(CREATE_DISPOSITION, configuration.getCreateDisposition());
    }

    @Test
    public void testSuccessfulCopyJob() throws Exception {
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
        assertTrue(configuration instanceof CopyJobConfiguration);

        assertEquals(
                DESTINATION_TABLE,
                ((CopyJobConfiguration) configuration).getDestinationTable().getTable());

        runner.assertTransferCount(BigQueryCopyJob.REL_SUCCESS, 1);
        runner.assertTransferCount(BigQueryCopyJob.REL_COPIED, 1);

        final MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(BigQueryCopyJob.REL_SUCCESS).get(0);

        assertEquals(
                SOURCE_DATASET,
                successFlowFile.getAttribute("bigquery.dataset")
        );

        assertEquals(
                SOURCE_TABLE,
                successFlowFile.getAttribute("bigquery.table")
        );

        final MockFlowFile copiedFlowFile = runner.getFlowFilesForRelationship(BigQueryCopyJob.REL_COPIED).get(0);

        assertEquals(
                DESTINATION_DATASET,
                copiedFlowFile.getAttribute("bigquery.dataset")
        );

        assertEquals(
                DESTINATION_TABLE,
                copiedFlowFile.getAttribute("bigquery.table")
        );

        assertEquals(
                "Copied FlowFiles should have the same UUID",
                successFlowFile.getAttribute(CoreAttributes.UUID.key()),
                copiedFlowFile.getAttribute(CoreAttributes.UUID.key())
        );

        assertArrayEquals(
                "Copied FlowFiles should have the same content",
                successFlowFile.toByteArray(),
                copiedFlowFile.toByteArray()
        );

        successFlowFile.assertAttributeEquals("testAttribute", "testAttributeValue");
        copiedFlowFile.assertAttributeEquals("testAttribute", "testAttributeValue");

    }
}
