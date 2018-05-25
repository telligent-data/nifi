package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
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

import java.io.IOException;
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
public class ExecuteSOQL extends AbstractProcessor {
    private static final DateTimeFormatter SF_DATETIME = DateTimeFormatter.ISO_INSTANT;
    private static final long DEFAULT_LAST_RUN_TIME = 1L;
    private static final Pattern LAST_RUN_TIME_REGEX = Pattern.compile("#LAST_RUN_TIME#");

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

    public static final PropertyDescriptor AUTH_ENDPOINT = new PropertyDescriptor
            .Builder().name("sf.auth-endpoint")
            .displayName("Authentication endpoint")
            .description("Endpoint for authentication with the SOAP API.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("https://login.salesforce.com/services/Soap/u/39.0")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("salesforce-query")
            .displayName("Query")
            .description("The query to execute through the Salesforce API.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_ALL = new PropertyDescriptor.Builder()
            .name("salesforce-query-all")
            .displayName("Query all data")
            .description("Determines whether or not all data (including deleted objects) will be queried.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the query is successfully executed.")
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
        _propertyDescriptors.add(AUTH_ENDPOINT);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(QUERY_ALL);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
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

    private static final JsonFactory jsonFactory = new JsonFactory();

    private static PartnerConnection buildConnection(String username, String password, String authEndpoint) throws ConnectionException {
        final ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(username);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(authEndpoint);
        return new PartnerConnection(partnerConfig);
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
                return;
            }
        }

        final String user = context.getProperty(USER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(fileToProcess).getValue();
        final String authEndpoint = context.getProperty(AUTH_ENDPOINT).evaluateAttributeExpressions(fileToProcess).getValue();
        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(fileToProcess).getValue();

        final Boolean queryAll = context.getProperty(QUERY_ALL).asBoolean();
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

        try {
            final PartnerConnection connection = buildConnection(user, password, authEndpoint);

            final GregorianCalendar currentServerTime = (GregorianCalendar) connection.getServerTimestamp().getTimestamp();

            final long currentServerTimeEpochMilli = currentServerTime.getTimeInMillis();

            boolean done = false;
            QueryResult queryResult = (queryAll) ? connection.queryAll(queryReplaced) : connection.query(queryReplaced);
            final int size = queryResult.getSize();
            while (!done) {
                for (SObject sObject : queryResult.getRecords()) {
                    final Map<String, String> attributeMap = new HashMap<>();
                    attributeMap.put("executesoql.row.count", String.valueOf(size));
                    attributeMap.put("executesoql.query", String.valueOf(queryReplaced));
                    attributeMap.put("executesoql.sf.timestamp", String.valueOf(currentServerTimeEpochMilli));
                    attributeMap.put("executesoql.lastrun.timestamp", String.valueOf(lastRunTime));

                    FlowFile flowFile = (fileToProcess != null) ? session.create(fileToProcess) : session.create();

                    flowFile = session.write(flowFile, (out) -> {
                        final JsonGenerator jsonGenerator = jsonFactory.createGenerator(out);

                        Util.writeXmlObject(sObject, jsonGenerator);
                        jsonGenerator.close();
                    });

                    flowFile = session.putAllAttributes(flowFile, attributeMap);
                    session.getProvenanceReporter().create(flowFile, "SALESFORCE_QUERY: " + query);
                    session.transfer(flowFile, REL_SUCCESS);
                }

                if (queryResult.isDone()) {
                    done = true;
                } else {
                    queryResult = connection.queryMore(queryResult.getQueryLocator());
                }
            }

            stateMapCopy.put(query, String.valueOf(currentServerTimeEpochMilli));
            try {
                stateManager.setState(stateMapCopy, Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Could not set state after processor execution. Duplicate data may result.", e);
            }

            if (fileToProcess != null) {
                session.remove(fileToProcess);
            }

            session.commit();

        } catch (ConnectionException e) {
            getLogger().error("Could not connect to Salesforce", e);
        }
    }
}
