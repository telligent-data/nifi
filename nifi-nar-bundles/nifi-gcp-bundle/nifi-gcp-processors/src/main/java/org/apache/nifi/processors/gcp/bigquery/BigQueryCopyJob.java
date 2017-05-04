package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;


/**
 * Copy data from one BigQuery table to another.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bigquery", "copy", "table"})
@CapabilityDescription("Copies a BigQuery table from one to the other.")
@WritesAttributes({
        @WritesAttribute(attribute="bigquery.dataset", description="Dataset upon which the copy was executed"),
        @WritesAttribute(attribute="bigquery.table", description="Table upon which the copy was executed")
})
public class BigQueryCopyJob extends AbstractBigQueryJobProcessor {
    public static final PropertyDescriptor SOURCE_DATASET = new PropertyDescriptor
            .Builder().name("bq-source-dataset")
            .displayName("Source dataset")
            .description("Dataset from which data will be copied.")
            .required(true)
            .defaultValue("${bigquery.dataset}")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_TABLE = new PropertyDescriptor
            .Builder().name("bq-source-table")
            .displayName("Source table")
            .description("Table from which data will be copied.")
            .required(true)
            .defaultValue("${bigquery.table}")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION_DATASET = new PropertyDescriptor
            .Builder().name("bq-destination-dataset")
            .displayName("Destination dataset")
            .description("Destination dataset which will be copied into.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION_TABLE = new PropertyDescriptor
            .Builder().name("bq-destination-table")
            .displayName("Destination table")
            .description("Destination table which will be copied into.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SOURCE_DATASET)
                .add(SOURCE_TABLE)
                .add(DESTINATION_DATASET)
                .add(DESTINATION_TABLE)
                .build();
    }


    public static final Relationship REL_COPIED = new Relationship.Builder().name("copied")
            .description("The copy of the flowfile is routed to this relationship.")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.<Relationship>builder()
                .addAll(super.getRelationships())
                .add(REL_COPIED)
                .build();
    }

    @Override
    protected void handleSuccess(JobConfiguration configuration, ProcessSession session, FlowFile flowFile) {
        final Map<String, String> attributes = ImmutableMap.of(
                "bigquery.dataset", ((CopyJobConfiguration) configuration).getSourceTables().get(0).getDataset(),
                "bigquery.table", ((CopyJobConfiguration) configuration).getSourceTables().get(0).getTable()
        );

        FlowFile copiedFlowFile = session.clone(flowFile);
        final Map<String, String> copiedAttributes = ImmutableMap.of(
                "bigquery.dataset", ((CopyJobConfiguration) configuration).getDestinationTable().getDataset(),
                "bigquery.table", ((CopyJobConfiguration) configuration).getDestinationTable().getTable()
        );

       copiedFlowFile = session.putAllAttributes(copiedFlowFile, copiedAttributes);
       flowFile = session.putAllAttributes(flowFile, attributes);

       session.transfer(flowFile, REL_SUCCESS);
       session.transfer(copiedFlowFile, REL_COPIED);

       session.getProvenanceReporter().clone(flowFile, copiedFlowFile);
    }

    @Override
    protected void handleFailure(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        super.handleFailure(context, session, flowFile);
    }

    @Override
    public CopyJobConfiguration buildJobConfiguration(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final JobInfo.CreateDisposition createDisposition = JobInfo.CreateDisposition.valueOf(
                context.getProperty(CREATE_DISPOSITION).getValue());
        final JobInfo.WriteDisposition writeDisposition = JobInfo.WriteDisposition.valueOf(
                context.getProperty(WRITE_DISPOSITION).getValue());

        final String sourceDataset = context.getProperty(SOURCE_DATASET)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final String sourceTable = context.getProperty(SOURCE_TABLE)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        final String destinationDataset = context.getProperty(DESTINATION_DATASET)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final String destinationTable = context.getProperty(DESTINATION_TABLE)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        return CopyJobConfiguration.newBuilder(TableId.of(destinationDataset, destinationTable),
                TableId.of(sourceDataset, sourceTable))
                .setCreateDisposition(createDisposition)
                .setWriteDisposition(writeDisposition)
                .build();
    }

    @Override
    public final String getJobName() {
        return "CopyJob";
    }
}
