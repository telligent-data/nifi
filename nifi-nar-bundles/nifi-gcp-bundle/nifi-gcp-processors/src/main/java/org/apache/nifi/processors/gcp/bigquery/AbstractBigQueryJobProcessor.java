package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Abstract class which runs BigQuery jobs and waits for their results.
 */
@WritesAttribute(attribute="bigquery.errors", description="Comma separated list of errors raised during BigQuery Job execution.")
public abstract class AbstractBigQueryJobProcessor extends AbstractBigQueryProcessor {
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor
            .Builder().name("bq-timeout")
            .displayName("Timeout (sec)")
            .description("After the specified number of seconds post-submittal, the BigQuery job (and flowfile) " +
                    "will be marked as a failure.")
            .required(true)
            .defaultValue("300")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final AllowableValue CD_CREATE_IF_NEEDED = new AllowableValue(
            JobInfo.CreateDisposition.CREATE_IF_NEEDED.name(), "Create If Needed", "Configures the job to create the " +
            "table if it does not exist."
    );

    public static final AllowableValue CD_CREATE_NEVER = new AllowableValue(
            JobInfo.CreateDisposition.CREATE_NEVER.name(), "Create Never", "Configures the job to fail with a not-" +
            "found error if the table does not exist."
    );

    public static final PropertyDescriptor CREATE_DISPOSITION = new PropertyDescriptor
            .Builder().name("bq-create-disposition")
            .displayName("Create Disposition")
            .description("Specifies whether the job is allowed to create new tables.")
            .required(true)
            .allowableValues(CD_CREATE_IF_NEEDED, CD_CREATE_NEVER)
            .defaultValue(JobInfo.CreateDisposition.CREATE_IF_NEEDED.name())
            .build();

    public static final AllowableValue WD_WRITE_APPEND = new AllowableValue(
            JobInfo.WriteDisposition.WRITE_APPEND.name(), "Append", "Configures the job to append data to the table " +
            "if it already exists."
    );

    public static final AllowableValue WD_WRITE_TRUNCATE = new AllowableValue(
            JobInfo.WriteDisposition.WRITE_TRUNCATE.name(), "Truncate", "Configures the job to overwrite the table " +
            "data if table already exists."
    );

    public static final AllowableValue WD_WRITE_EMPTY = new AllowableValue(
            JobInfo.WriteDisposition.WRITE_EMPTY.name(), "Only if Empty", "Configures the job to fail with a duplicate " +
            "error if the table already exists."
    );

    public static final PropertyDescriptor WRITE_DISPOSITION = new PropertyDescriptor
            .Builder().name("bq-write-disposition")
            .displayName("Write Disposition")
            .description("Specifies the action that occurs if the destination table already exists.")
            .required(true)
            .allowableValues(WD_WRITE_APPEND, WD_WRITE_TRUNCATE, WD_WRITE_EMPTY)
            .defaultValue(JobInfo.WriteDisposition.WRITE_APPEND.name())
            .build();

    public static final Relationship REL_WARNING =
            new Relationship.Builder().name("warning")
                    .description("FlowFiles are routed to this relationship if the BigQuery operation completes with warnings.")
                    .build();

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.<Relationship>builder().addAll(super.getRelationships())
                .add(REL_WARNING)
                .build();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(TIMEOUT)
                .add(CREATE_DISPOSITION)
                .add(WRITE_DISPOSITION)
                .build();
    }

    public abstract JobConfiguration buildJobConfiguration(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException;

    public abstract String getJobName();

    protected abstract void handleSuccess(JobConfiguration configuration, ProcessSession session, FlowFile flowFile);

    protected void handleWarning(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_WARNING);
    }

    protected void handleFailure(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final JobConfiguration configuration = buildJobConfiguration(context, session, flowFile);
        final Integer timeout = context.getProperty(TIMEOUT).asInteger();

        final BigQuery bigQuery = getCloudService();

        try {

            final Job job = bigQuery.create(JobInfo.of(configuration));

            Job completedJob = job.waitFor(WaitForOption.checkEvery(1, TimeUnit.SECONDS),
                    WaitForOption.timeout(timeout, TimeUnit.SECONDS));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                handleSuccess(configuration, session, flowFile);

                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                getLogger().info("Successfully completed {} to Google BigQuery in {} milliseconds",
                        new Object[]{getJobName(), millis});
            } else {
                if (completedJob == null) {
                    throw new BigQueryException(404, String.format("Job %s no longer exists.", job.getGeneratedId()));
                }

                // Execution errors are considered to be "nonfatal" and can be written as a JSON attribute.
                // This allows for programmatic error handling by the user downstream.
                final String errorList = completedJob.getStatus().getExecutionErrors().stream()
                        .map(x ->
                                "[{" +
                                        "\"location\":\"" + x.getLocation() + "\", " +
                                        "\"reason\":\"" + x.getReason() + "\", " +
                                        "\"message\":\"" + x.getMessage() + "\"" +
                                "}]")
                        .collect(Collectors.joining(","));

                flowFile = session.putAttribute(flowFile, "bigquery.errors", errorList);
                handleWarning(context, session, flowFile);
            }
        } catch (final BigQueryException | TimeoutException | InterruptedException e) {
            getLogger().error("Failed to complete BigQuery {} with configuration of {}: {}",
                    new Object[]{getJobName(), configuration, e});
            handleFailure(context, session, flowFile);
        }
    }
}
