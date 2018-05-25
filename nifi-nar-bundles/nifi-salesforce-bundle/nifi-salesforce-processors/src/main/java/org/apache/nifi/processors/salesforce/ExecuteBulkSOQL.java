package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.core.JsonFactory;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gene on 5/1/17.
 */
@EventDriven
@DefaultSchedule(period = "30 sec")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Stateful(scopes = Scope.CLUSTER, description = "")
@Tags({"sql", "select", "salesforce", "query", "soql"})
@CapabilityDescription("Execute provided SOQL query on a Salesforce instance. Query result will be converted to JSON format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executesoql.row.count' indicates how many rows were selected.")
@WritesAttribute(attribute="executesql.row.count", description = "Contains the number of rows returned in the select query")
public class ExecuteBulkSOQL extends AbstractProcessor {
    private static final String NO_RECORDS_FOR_QUERY = "Records not found for this query";
    private static final DateTimeFormatter SF_DATETIME = DateTimeFormatter.ISO_INSTANT;
    private static final long DEFAULT_LAST_RUN_TIME = 1L;
    private static final Pattern LAST_RUN_TIME_REGEX = Pattern.compile("#LAST_RUN_TIME#");
    private static final Pattern OBJECT_REGEX = Pattern.compile("FROM\\s+(\\S+)\\s+", Pattern.CASE_INSENSITIVE);
    private static final String SOAP_API_ENDPOINT = "https://login.salesforce.com/services/Soap/u/";


    public static final PropertyDescriptor USER = new PropertyDescriptor
            .Builder().name("sf.user")
            .displayName("User")
            .description("User account with which to login to Salesforce.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("sf.password")
            .displayName("Password")
            .description("Password for authentication.")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor API_VERSION = new PropertyDescriptor
            .Builder().name("sf.api_version")
            .displayName("API Version")
            .description("Version of the Salesforce API to use.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("40.0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("salesforce-query")
            .displayName("Query")
            .description("The query to execute through the Salesforce API.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_HEADER = new PropertyDescriptor.Builder()
            .name("salesforce-include-header")
            .displayName("Include header")
            .description("Whether or not to include CSV headers in the result set.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SANITIZE_HEADER = new PropertyDescriptor.Builder()
            .name("salesforce-sanitize-header")
            .displayName("Sanitize header")
            .description("If true, '.' characters in the Salesforce header will be replaced with underscores '_'.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the query is successfully executed.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original source flowfile, if there is one, is routed to this relationship.")
            .build();


    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(USER);
        _propertyDescriptors.add(PASSWORD);
        _propertyDescriptors.add(API_VERSION);
        _propertyDescriptors.add(INCLUDE_HEADER);
        _propertyDescriptors.add(SANITIZE_HEADER);
        _propertyDescriptors.add(QUERY);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(_relationships);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private static class ConnectionPair {
        private final PartnerConnection partnerConnection;
        private final BulkConnection bulkConnection;

        private ConnectionPair(PartnerConnection partnerConnection, BulkConnection bulkConnection) {
            this.partnerConnection = partnerConnection;
            this.bulkConnection = bulkConnection;
        }
    }

    private static ConnectionPair buildConnection(String username, String password, String apiVersion) throws ConnectionException, AsyncApiException {
        final ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(username);
        partnerConfig.setPassword(password);

        final String soapAuthEndpoint = SOAP_API_ENDPOINT + apiVersion;
        partnerConfig.setAuthEndpoint(soapAuthEndpoint);
        partnerConfig.setTraceMessage(false);
        final PartnerConnection partnerConnection = new PartnerConnection(partnerConfig);

        // Now build bulk connection
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        final String soapEndpoint = partnerConfig.getServiceEndpoint();
        final String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);
        config.setTraceMessage(false);

        return new ConnectionPair(partnerConnection, new BulkConnection(config));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();


            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                context.yield();
            }
        }


        final String user = context.getProperty(USER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(fileToProcess).getValue();
        final String apiVersion = context.getProperty(API_VERSION).evaluateAttributeExpressions(fileToProcess).getValue();
        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        final Boolean includeHeader = context.getProperty(INCLUDE_HEADER).asBoolean();
        final Boolean sanitizeHeader = context.getProperty(SANITIZE_HEADER).asBoolean();
        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve last succesful runtime from the State Manager. Will not perform "
                    + "api call until this is accomplished.", ioe);
            context.yield();
            return;
        }

        final Map<String, String> stateMapCopy = new HashMap<>(stateMap.toMap());
        final Long lastRunTimeFromMap = Util.parseLongOrNull(stateMapCopy.get(query));
        final long lastRunTime = (lastRunTimeFromMap == null) ? DEFAULT_LAST_RUN_TIME : lastRunTimeFromMap;
        final String sfFormattedLastRun = Instant.ofEpochMilli(lastRunTime).atZone(ZoneOffset.UTC).format(SF_DATETIME);
        final String queryReplaced = LAST_RUN_TIME_REGEX.matcher(query).replaceAll(sfFormattedLastRun);
        final Matcher queryObjectMatcher = OBJECT_REGEX.matcher(query);

        if (!queryObjectMatcher.find()) {
            throw new ProcessException("Could not extract out object name from query -- are you using an expression with FROM?");
        }

        final String queryObject = queryObjectMatcher.group(1);

        try {
            getLogger().info("Connecting to Salesforce.");
            final ConnectionPair connectionPair = buildConnection(user, password, apiVersion);

            final GregorianCalendar currentServerTime = (GregorianCalendar) connectionPair.partnerConnection.getServerTimestamp().getTimestamp();

            final long currentServerTimeEpochMilli = currentServerTime.getTimeInMillis();

            JobInfo job = new JobInfo(new JobInfo.Builder()
                    .object(queryObject)
                    .operation(OperationEnum.query)
                    .concurrencyMode(ConcurrencyMode.Parallel)
                    .contentType(ContentType.CSV));
            job = connectionPair.bulkConnection.createJob(job);
            job = connectionPair.bulkConnection.getJobStatus(job.getId());
            BatchInfo info = null;

            ByteArrayInputStream bout = new ByteArrayInputStream(queryReplaced.getBytes());
            info = connectionPair.bulkConnection.createBatchFromStream(job, bout);
            Thread.sleep(1000);
            String[] queryResults = null;

            for(int i=0; i<30000; i++) {

                info = connectionPair.bulkConnection.getBatchInfo(job.getId(),
                        info.getId());

                if (info.getState() == BatchStateEnum.Completed) {
                    QueryResultList list =
                            connectionPair.bulkConnection.getQueryResultList(job.getId(),
                                    info.getId());
                    queryResults = list.getResult();
                    break;
                } else if (info.getState() == BatchStateEnum.Failed) {
                    throw new ProcessException("Bulk API Query Failed: " + info.getStateMessage());
                } else {
                    getLogger().info("Waiting for Bulk API Query to finish: " + info);
                    Thread.sleep(10000);
                }
            }

            if (queryResults != null) {
                for (String resultId : queryResults) {
                    final Map<String, String> attributeMap = new HashMap<>();
                    attributeMap.put("executesoql.row.count", String.valueOf(info.getNumberRecordsProcessed()));
                    attributeMap.put("executesoql.query", String.valueOf(queryReplaced));
                    attributeMap.put("executesoql.sf.timestamp", String.valueOf(currentServerTimeEpochMilli));
                    attributeMap.put("executesoql.lastrun.timestamp", String.valueOf(lastRunTime));

                    final InputStream in = connectionPair.bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId);

                    String header;
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try {
                        header = reader.readLine();
                        if (header.equals(NO_RECORDS_FOR_QUERY)) {
                            continue;
                        }
                    } catch (IOException e) {
                        getLogger().error("Could not decode header.", e);
                        continue;
                    }

                    FlowFile flowFile = (fileToProcess != null) ? session.create(fileToProcess) : session.create();
                    flowFile = session.write(flowFile, (out) -> {
                        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

                        if(includeHeader && header != null) {
                            writer.write(header);
                            writer.write('\n');
                        }

                        int i;
                        while ((i = reader.read()) != -1) {
                            writer.write(i);
                        }
                        writer.close();
                    });

                    try {
                        reader.close();
                    } catch (IOException e) {
                        getLogger().error("Could not close the input datastream; data may be truncated", e);
                    }

                    flowFile = session.putAllAttributes(flowFile, attributeMap);
                    session.getProvenanceReporter().create(flowFile, "SALESFORCE_QUERY: " + query);
                    session.transfer(flowFile, REL_SUCCESS);

                    stateMapCopy.put(query, String.valueOf(currentServerTimeEpochMilli));

                }
            }

            try {
                stateManager.setState(stateMapCopy, Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Could not set state after processor execution. Duplicate data may result.", e);
            }

            if (fileToProcess != null) {
                session.transfer(fileToProcess, REL_ORIGINAL);
            }

            session.commit();

        } catch (ConnectionException | AsyncApiException e) {
            getLogger().error("Could not connect to Salesforce", e);
        } catch (InterruptedException e) {
            getLogger().error("Interrupted while waiting for bulk results to be ready", e);
        }
    }
}
