package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public abstract class AbstractBigQueryJobTest extends AbstractBigQueryTest {
    @Mock
    protected Job job;

    protected abstract void addRequiredPropertiesToRunner(TestRunner runner);

    /*
    Oh, the things we do for 100% line coverage.
     */
    @Test
    public void testNoFlowfileForJob() throws Exception {
        reset(bigQuery);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        runner.assertValid();
        runner.run();

        runner.assertTransferCount(AbstractBigQueryJobProcessor.REL_WARNING, 0);
        runner.assertTransferCount(AbstractBigQueryJobProcessor.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractBigQueryJobProcessor.REL_FAILURE, 0);
    }

    @Test
    public void testFailedOnTimeoutJob() throws Exception {
        reset(bigQuery, job);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);

        when(job.waitFor(any(), any())).thenThrow(new TimeoutException("Test timeout exception"));

        runner.enqueue("testFlowFile");
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(BigQueryLoadJob.REL_FAILURE);
        runner.assertTransferCount(BigQueryLoadJob.REL_FAILURE, 1);
    }


    @Test
    public void testFailedOnJobNoLongerExists() throws Exception {
        reset(bigQuery, job);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.waitFor(any(), any())).thenReturn(null);

        runner.enqueue("testFlowFile");
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(BigQueryLoadJob.REL_FAILURE);
        runner.assertTransferCount(BigQueryLoadJob.REL_FAILURE, 1);
    }

    @Test
    public void testWarnedJob() throws Exception {
        reset(bigQuery, job);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);

        when(job.waitFor(any(), any())).thenReturn(job);

        final JobStatus mockStatus = mock(JobStatus.class);
        when(job.getStatus()).thenReturn(mockStatus);

        final BigQueryError e = new BigQueryError("test reason", "test location", "test message");
        when(mockStatus.getError()).thenReturn(e);
        when(mockStatus.getExecutionErrors()).thenReturn(ImmutableList.of(e));
        runner.enqueue("testFlowFile");
        runner.assertValid();
        runner.run();

        runner.assertAllFlowFilesTransferred(BigQueryLoadJob.REL_WARNING);
        runner.assertTransferCount(BigQueryLoadJob.REL_WARNING, 1);

        final FlowFile flowFile = runner.getFlowFilesForRelationship(BigQueryLoadJob.REL_WARNING).get(0);
        assertEquals(
                "[{\"location\":\"test location\", \"reason\":\"test reason\", \"message\":\"test message\"}]",
                flowFile.getAttribute("bigquery.errors")
        );
    }

}
