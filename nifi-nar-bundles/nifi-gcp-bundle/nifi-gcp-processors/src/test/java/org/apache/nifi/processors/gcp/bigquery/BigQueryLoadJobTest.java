package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BigQueryLoadJobTest extends AbstractBigQueryJobTest {
    final static JobInfo.CreateDisposition CREATE_DISPOSITION = JobInfo.CreateDisposition.CREATE_NEVER;
    final static JobInfo.WriteDisposition WRITE_DISPOSITION = JobInfo.WriteDisposition.WRITE_TRUNCATE;

    final static String URI = "gs://test-bucket/test-key";
    final static String DATASET = "test-dataset";
    final static String TABLE = "test-table";
    final static String SOURCE_FORMAT = "CSV";
    final static Integer MAX_BAD_RECORDS = 14;
    final static Boolean IGNORE_UNKNOWN_VALUES = true;


    @Override
    public BigQueryLoadJob getProcessor() {
        return new BigQueryLoadJob() {
            @Override
            protected BigQuery getCloudService() {
                return bigQuery;
            }
        };
    }

    @Override
    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(BigQueryLoadJob.CREATE_DISPOSITION, CREATE_DISPOSITION.name());
        runner.setProperty(BigQueryLoadJob.WRITE_DISPOSITION, WRITE_DISPOSITION.name());
        runner.setProperty(BigQueryLoadJob.MAX_BAD_RECORDS, String.valueOf(MAX_BAD_RECORDS));
        runner.setProperty(BigQueryLoadJob.IGNORE_UNKNOWN_VALUES, String.valueOf(IGNORE_UNKNOWN_VALUES));
        runner.setProperty(BigQueryLoadJob.SOURCE_FORMAT, SOURCE_FORMAT);
        runner.setProperty(BigQueryLoadJob.DATASET, DATASET);
        runner.setProperty(BigQueryLoadJob.TABLE, TABLE);
        runner.setProperty(BigQueryLoadJob.URI, URI);
    }

    @Test
    public void testCSVFormatOptions() throws Exception {
        reset(bigQuery);
        final TestRunner runner = buildNewRunner(getProcessor());

        runner.setProperty(BigQueryLoadJob.SOURCE_FORMAT, "CSV");
        runner.setProperty(BigQueryLoadJob.ALLOW_JAGGED_ROWS, "true");
        runner.setProperty(BigQueryLoadJob.ALLOW_QUOTED_NEWLINES, "false");
        runner.setProperty(BigQueryLoadJob.CHARACTER_ENCODING, "UTF-16");
        runner.setProperty(BigQueryLoadJob.FIELD_DELIMITER, "-");
        runner.setProperty(BigQueryLoadJob.QUOTE, "'");
        runner.setProperty(BigQueryLoadJob.SKIP_LEADING_ROWS, "24");

        final FormatOptions options = BigQueryLoadJob.buildFormatOptions(runner.getProcessContext());

        assertEquals("FormatOptions should have a class of CsvOptions",
                CsvOptions.class,
                options.getClass());

        assertEquals("FormatOptions internal type should be 'CSV'",
                "CSV",
                options.getType());

        assertTrue(((CsvOptions) options).allowJaggedRows());
        assertFalse(((CsvOptions) options).allowQuotedNewLines());
        assertEquals("UTF-16", ((CsvOptions) options).getEncoding());
        assertEquals("-", ((CsvOptions) options).getFieldDelimiter());
        assertEquals("'",  ((CsvOptions) options).getQuote());
        assertEquals(24L,  ((CsvOptions) options).getSkipLeadingRows().longValue());
    }

    @Test
    public void testJsonFormatOptions() throws Exception {
        reset(bigQuery);
        final TestRunner runner = buildNewRunner(getProcessor());

        runner.setProperty(BigQueryLoadJob.SOURCE_FORMAT, "NEWLINE_DELIMITED_JSON");

        final FormatOptions options = BigQueryLoadJob.buildFormatOptions(runner.getProcessContext());

        assertEquals("FormatOptions internal type should be 'NEWLINE_DELIMITED_JSON'",
                "NEWLINE_DELIMITED_JSON",
                options.getType());
    }

    @Test
    public void testDataStoreBackupFormatOptions() throws Exception {
        reset(bigQuery);
        final TestRunner runner = buildNewRunner(getProcessor());

        runner.setProperty(BigQueryLoadJob.SOURCE_FORMAT, "DATASTORE_BACKUP");

        final FormatOptions options = BigQueryLoadJob.buildFormatOptions(runner.getProcessContext());

        assertEquals("FormatOptions internal type should be 'DATASTORE_BACKUP'",
                "DATASTORE_BACKUP",
                options.getType());
    }

    @Test
    public void testLoadJobConfiguration() throws Exception {
        reset(bigQuery);

        final BigQueryLoadJob loadJobProcessor = getProcessor();

        final TestRunner runner = buildNewRunner(loadJobProcessor);
        addRequiredPropertiesToRunner(runner);

        final FlowFile mockflowfile = new MockFlowFile(1234L);

        final LoadJobConfiguration configuration = loadJobProcessor.buildJobConfiguration(runner.getProcessContext(),
                runner.getProcessSessionFactory().createSession(), mockflowfile);

        assertEquals(CREATE_DISPOSITION, configuration.getCreateDisposition());
        assertEquals(WRITE_DISPOSITION, configuration.getWriteDisposition());
        assertEquals(MAX_BAD_RECORDS, configuration.getMaxBadRecords());
        assertEquals(IGNORE_UNKNOWN_VALUES, configuration.ignoreUnknownValues());
        assertEquals(TableId.of(DATASET, TABLE), configuration.getDestinationTable());

        assertEquals(URI, configuration.getSourceUris().get(0));
    }


    @Test
    public void testSuccessfulLoadJob() throws Exception {
        reset(bigQuery, job);

        final TestRunner runner = buildNewRunner(getProcessor());
        addRequiredPropertiesToRunner(runner);

        ArgumentCaptor<JobInfo> jobInfoArgumentCaptor = ArgumentCaptor.forClass(JobInfo.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);

        when(job.waitFor(any(), any())).thenReturn(job);

        final JobStatus mockStatus = mock(JobStatus.class);
        when(job.getStatus()).thenReturn(mockStatus);
        when(mockStatus.getError()).thenReturn(null);

        runner.enqueue("testFlowFile");
        runner.assertValid();
        runner.run();

        verify(bigQuery).create(jobInfoArgumentCaptor.capture());
        assertEquals(SOURCE_FORMAT,
                ((LoadJobConfiguration) jobInfoArgumentCaptor.getValue().getConfiguration()).getFormat());
        assertEquals(CREATE_DISPOSITION,
                ((LoadJobConfiguration) jobInfoArgumentCaptor.getValue().getConfiguration()).getCreateDisposition());

        runner.assertAllFlowFilesTransferred(BigQueryLoadJob.REL_SUCCESS);
        runner.assertTransferCount(BigQueryLoadJob.REL_SUCCESS, 1);

        final FlowFile flowFile = runner.getFlowFilesForRelationship(BigQueryLoadJob.REL_SUCCESS).get(0);
        assertEquals(
                DATASET,
                flowFile.getAttribute("bigquery.dataset")
        );

        assertEquals(
                TABLE,
                flowFile.getAttribute("bigquery.table")
        );

        assertEquals(
                URI,
                flowFile.getAttribute("bigquery.uri")
        );
    }


}
