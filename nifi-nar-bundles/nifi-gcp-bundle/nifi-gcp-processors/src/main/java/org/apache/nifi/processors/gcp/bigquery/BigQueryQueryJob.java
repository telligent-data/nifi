package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Execute a query on a BigQuery table and then send the results to another table.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bigquery", "query", "table"})
@CapabilityDescription("Queries a BigQuery table and exports the results to another table.")
@WritesAttributes({
        @WritesAttribute(attribute="bigquery.dataset", description="Destination dataset of the query"),
        @WritesAttribute(attribute="bigquery.table", description="Destination table of the query"),
        @WritesAttribute(attribute="bigquery.query", description="Query which was executed.")
})
public class BigQueryQueryJob extends AbstractBigQueryJobProcessor {
    public static final PropertyDescriptor DESTINATION_DATASET = new PropertyDescriptor
            .Builder().name("bq-destination-dataset")
            .displayName("Destination dataset")
            .description("Destination dataset which will be loaded into.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION_TABLE = new PropertyDescriptor
            .Builder().name("bq-destination-table")
            .displayName("Destination table")
            .description("Sets the table where to put query results. If not provided a new table is created. This " +
                    "value is required if \"Allow large results\" is set to true.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor
            .Builder().name("bq-query")
            .displayName("Query")
            .description("Query to be executed.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USE_LEGACY_SQL = new PropertyDescriptor.Builder()
            .name("bq-use-legacy-sql")
            .displayName("Use Legacy SQL")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use BigQuery legacy SQL.")
            .build();

    public static final PropertyDescriptor ALLOW_LARGE_RESULTS = new PropertyDescriptor.Builder()
            .name("bq-allow-large-results")
            .displayName("Allow large results")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .description("Sets whether the job is enabled to create arbitrarily large results. If true the query is " +
                    "allowed to create large results at a slight cost in performance. If true \"Destination Table\"" +
                    " must be provided.")
            .build();

    public static final PropertyDescriptor USE_QUERY_CACHE = new PropertyDescriptor.Builder()
            .name("bq-use-query-cache")
            .displayName("Use query cache")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Sets whether to look for the result in the query cache. The query cache is a best-effort " +
                    "cache that will be flushed whenever tables in the query are modified. Moreover, the query cache " +
                    "is only available when \"Destination table\" is not set.")
            .build();

    public static final PropertyDescriptor FLATTEN_RESULTS = new PropertyDescriptor.Builder()
            .name("bq-flatten-results")
            .displayName("Flatten results")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Sets whether nested and repeated fields should be flattened. If set to false, \"Allow large " +
                    "results\" must be true.")
            .build();

    public static final AllowableValue PR_INTERACTIVE = new AllowableValue(
            QueryJobConfiguration.Priority.INTERACTIVE.name(), "Interactive", "Query is executed as soon as possible " +
            "and count towards the concurrent rate limit and the daily rate limit"
    );


    public static final AllowableValue PR_BATCH = new AllowableValue(
            QueryJobConfiguration.Priority.BATCH.name(), "Batch", "Query is queued and started as soon as idle " +
            "resources are available, usually within a few minutes. If the query hasn't started within 3 hours, its " +
            "priority is changed to INTERACTIVE."
    );

    public static final PropertyDescriptor PRIORITY = new PropertyDescriptor.Builder()
            .name("bq-priority")
            .displayName("Priority")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(PR_INTERACTIVE, PR_BATCH)
            .required(false)
            .description("Sets a priority for the query. If not specified the priority is assumed to be INTERACTIVE.")
            .build();

    public static final PropertyDescriptor DRY_RUN = new PropertyDescriptor.Builder()
            .name("bq-dry-run")
            .displayName("Dry run")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .description("Sets whether the job has to be dry run or not. If set, the job is not executed. A valid " +
                    "query will return a mostly empty response with some processing statistics, while an invalid " +
                    "query will return the same error it would if it wasn't a dry run.")
            .build();



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(DESTINATION_DATASET)
                .add(DESTINATION_TABLE)
                .add(QUERY)
                .add(USE_LEGACY_SQL)
                .add(ALLOW_LARGE_RESULTS)
                .add(USE_QUERY_CACHE)
                .add(FLATTEN_RESULTS)
                .add(PRIORITY)
                .add(DRY_RUN)
                .build();
    }

    @Override
    protected void handleSuccess(JobConfiguration configuration, ProcessSession session, FlowFile flowFile) {
        final Map<String, String> attributes = ImmutableMap.of(
                "bigquery.dataset", ((QueryJobConfiguration) configuration).getDestinationTable().getDataset(),
                "bigquery.table", ((QueryJobConfiguration) configuration).getDestinationTable().getTable()
        );

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    public QueryJobConfiguration buildJobConfiguration(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final JobInfo.CreateDisposition createDisposition = JobInfo.CreateDisposition.valueOf(
                context.getProperty(CREATE_DISPOSITION).getValue());
        final JobInfo.WriteDisposition writeDisposition = JobInfo.WriteDisposition.valueOf(
                context.getProperty(WRITE_DISPOSITION).getValue());

        final String destinationDataset = context.getProperty(DESTINATION_DATASET)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final String destinationTable = context.getProperty(DESTINATION_TABLE)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        String query = context.getProperty(QUERY)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        if (query == null) {
            try (final InputStream in = session.read(flowFile)) {
                query = CharStreams.toString(new InputStreamReader(in));
            } catch (IOException e) {
                throw new ProcessException("Query cannot be read from FlowFile", e);
            }
        }

        final Boolean useLegacySql = context.getProperty(USE_LEGACY_SQL)
                .asBoolean();
        final Boolean allowLargeResults = context.getProperty(ALLOW_LARGE_RESULTS)
                .asBoolean();
        final Boolean useQueryCache = context.getProperty(USE_QUERY_CACHE)
                .asBoolean();
        final Boolean flattenResults = context.getProperty(FLATTEN_RESULTS)
                .asBoolean();
        final QueryJobConfiguration.Priority priority = QueryJobConfiguration.Priority.valueOf(
                context.getProperty(PRIORITY).getValue());
        final Boolean dryRun = context.getProperty(DRY_RUN).asBoolean();

        return QueryJobConfiguration.newBuilder(query)
                .setCreateDisposition(createDisposition)
                .setWriteDisposition(writeDisposition)
                .setDestinationTable(TableId.of(destinationDataset, destinationTable))
                .setUseLegacySql(useLegacySql)
                .setAllowLargeResults(allowLargeResults)
                .setUseQueryCache(useQueryCache)
                .setFlattenResults(flattenResults)
                .setPriority(priority)
                .setDryRun(dryRun)
                .build();
    }

    @Override
    public final String getJobName() {
        return "QueryJob";
    }
}
