package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.KEY_ATTR;

/**
 * Load data into BigQuery.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bigquery", "load", "table"})
@CapabilityDescription("Loads a BigQuery table from a location in Google Cloud Storage")
@WritesAttributes({
        @WritesAttribute(attribute="bigquery.dataset", description="Dataset upon which the load was performed"),
        @WritesAttribute(attribute="bigquery.table", description="Table upon which the load was performed"),
        @WritesAttribute(attribute="bigquery.uri", description="URI from which the data was loaded"),
})
public class BigQueryLoadJob extends AbstractBigQueryJobProcessor {
    public static final PropertyDescriptor URI = new PropertyDescriptor
            .Builder().name("bq-uri")
            .displayName("URI")
            .description("URI from which to load data into BigQuery.")
            .required(true)
            .defaultValue("gs://{" + BUCKET_ATTR + "}/{" + KEY_ATTR + "}")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATASET = new PropertyDescriptor
            .Builder().name("bq-dataset")
            .displayName("Dataset")
            .description("Dataset on which to conduct BigQuery operations.")
            .required(true)
            .defaultValue("${bigquery.dataset}")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE = new PropertyDescriptor
            .Builder().name("bq-table")
            .displayName("Table")
            .description("Table to perform the load.")
            .required(true)
            .defaultValue("${bigquery.table}")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    //TODO: implement this schema parsing
    /*
    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("bq-schema")
            .displayName("Schema")
            .description("The schema for the destination table in JSON / text format. The schema can be omitted if " +
                    "the destination table already exists, or if you're loading data from a Google Cloud Datastore " +
                    "backup")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    */

    public static final PropertyDescriptor MAX_BAD_RECORDS = new PropertyDescriptor
            .Builder().name("bq-max-bad-records")
            .displayName("Max Bad Records")
            .description("If there are more than the specified number of bad rows in the load job, fail the job.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNORE_UNKNOWN_VALUES = new PropertyDescriptor
            .Builder().name("bq-ignore-unknown-values")
            .displayName("Ignore unknown values")
            .description("If true, any additional fields present in the loading source will be ignored. If false, " +
                    "unknown fields will cause the load to fail.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_FORMAT = new PropertyDescriptor
            .Builder().name("bq-source-format")
            .displayName("Source format")
            .description("Format of the data being loaded.")
            .required(true)
            .allowableValues("CSV", "NEWLINE_DELIMITED_JSON", "DATASTORE_BACKUP")
            .build();

    public static final PropertyDescriptor ALLOW_JAGGED_ROWS = new PropertyDescriptor
            .Builder().name("bq-allow-jagged-rows")
            .displayName("Allow jagged rows (CSV)")
            .description("Set whether BigQuery should accept rows that are missing trailing optional columns. If true, " +
                    "BigQuery treats missing trailing columns as null values. If false, records with missing trailing " +
                    "columns are treated as bad records, and if there are too many bad records, an invalid error is " +
                    "returned in the job result. By default, rows with missing trailing columns are considered bad " +
                    "records.")
            .required(false)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALLOW_QUOTED_NEWLINES = new PropertyDescriptor
            .Builder().name("bq-allow-quoted-newlines")
            .displayName("Allow quoted newlines (CSV)")
            .description("Sets whether BigQuery should allow quoted data sections that contain newline characters in " +
                    "a CSV file. By default quoted newline are not allowed.")
            .required(false)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHARACTER_ENCODING = new PropertyDescriptor
            .Builder().name("bq-character-encoding")
            .displayName("Character encoding (CSV)")
            .description("Sets the character encoding of the data. The supported values are UTF-8 or ISO-8859-1. " +
                    "The default value is UTF-8.")
            .required(false)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELD_DELIMITER = new PropertyDescriptor
            .Builder().name("bq-field-delimiter")
            .displayName("Field delimiter (CSV)")
            .description("Sets the separator for fields in a CSV file. BigQuery also supports the escape sequence " +
                    "\"\\t\" to specify a tab separator. The default value is a comma (',').")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUOTE = new PropertyDescriptor
            .Builder().name("bq-quote")
            .displayName("Quote  (CSV)")
            .description("Sets the value that is used to quote data sections in a CSV file.  The default value is a " +
                    "double-quote ('\"'). If your data does not contain quoted sections, set the property value to " +
                    "an empty string. If your data contains quoted newline characters, you must also set " +
                    "'Allow quoted newlines' property to true.")
            .required(false)
            .build();

    public static final PropertyDescriptor SKIP_LEADING_ROWS = new PropertyDescriptor
            .Builder().name("bq-skip-leading-rows")
            .displayName("Skip leading rows  (CSV)")
            .description("Sets the number of rows at the top of a CSV file that BigQuery will skip when reading the " +
                    "data. The default value is 0. This property is useful if you have header rows in the file that " +
                    "should be skipped.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(URI)
                .add(DATASET)
                .add(TABLE)
                .add(MAX_BAD_RECORDS)
                .add(IGNORE_UNKNOWN_VALUES)
                .add(SOURCE_FORMAT)
                .add(ALLOW_JAGGED_ROWS)
                .add(ALLOW_QUOTED_NEWLINES)
                .add(CHARACTER_ENCODING)
                .add(FIELD_DELIMITER)
                .add(QUOTE)
                .add(SKIP_LEADING_ROWS)
                .build();
    }

    @Override
    protected void handleSuccess(JobConfiguration configuration, ProcessSession session, FlowFile flowFile) {
        final Map<String, String> attributes = ImmutableMap.of(
                "bigquery.dataset", ((LoadJobConfiguration) configuration).getDestinationTable().getDataset(),
                "bigquery.table", ((LoadJobConfiguration) configuration).getDestinationTable().getTable(),
                "bigquery.uri", ((LoadJobConfiguration) configuration).getSourceUris().get(0)
        );

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }

    protected static FormatOptions buildFormatOptions(ProcessContext context) {
        final String sourceFormatString = context.getProperty(SOURCE_FORMAT)
                .getValue();

        if ("CSV".equals(sourceFormatString)) {
            final Boolean allowJaggedRows = context.getProperty(ALLOW_JAGGED_ROWS)
                    .asBoolean();

            final Boolean allowQuotedNewlines = context.getProperty(ALLOW_QUOTED_NEWLINES)
                    .asBoolean();

            final String characterEncoding = context.getProperty(CHARACTER_ENCODING).getValue();
            final String fieldDelimiter = context.getProperty(FIELD_DELIMITER).getValue();
            final String quote = context.getProperty(QUOTE).getValue();
            final Long skipLeadingRows = context.getProperty(SKIP_LEADING_ROWS).asLong();

            final CsvOptions.Builder builder = CsvOptions.newBuilder();

            if (allowJaggedRows != null) {
                builder.setAllowJaggedRows(allowJaggedRows);
            }

            if (allowQuotedNewlines != null) {
                builder.setAllowQuotedNewLines(allowQuotedNewlines);
            }

            if (characterEncoding != null) {
                builder.setEncoding(characterEncoding);
            }

            if (fieldDelimiter != null) {
                builder.setFieldDelimiter(fieldDelimiter);
            }

            if (quote != null) {
                builder.setQuote(quote);
            }

            if (skipLeadingRows != null) {
                builder.setSkipLeadingRows(skipLeadingRows);
            }

            return builder.build();
        } else {
            return FormatOptions.of(sourceFormatString);
        }
    }

    @Override
    public LoadJobConfiguration buildJobConfiguration(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final JobInfo.CreateDisposition createDisposition = JobInfo.CreateDisposition.valueOf(
                context.getProperty(CREATE_DISPOSITION).getValue());
        final JobInfo.WriteDisposition writeDisposition = JobInfo.WriteDisposition.valueOf(
                context.getProperty(WRITE_DISPOSITION).getValue());

        final String dataset = context.getProperty(DATASET)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final String table = context.getProperty(TABLE)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        final Integer maxBadRecords = context.getProperty(MAX_BAD_RECORDS).asInteger();
        final Boolean ignoreUnknownValues = context.getProperty(IGNORE_UNKNOWN_VALUES).asBoolean();

        final FormatOptions sourceFormat = buildFormatOptions(context);

        final String uri = context.getProperty(URI)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        final TableId tableId = TableId.of(dataset, table);

        return LoadJobConfiguration.newBuilder(tableId, uri, sourceFormat)
                .setCreateDisposition(createDisposition)
                .setWriteDisposition(writeDisposition)
                .setMaxBadRecords(maxBadRecords)
                .setIgnoreUnknownValues(ignoreUnknownValues)
                .build();
    }

    @Override
    public final String getJobName() {
        return "LoadJob";
    }
}
