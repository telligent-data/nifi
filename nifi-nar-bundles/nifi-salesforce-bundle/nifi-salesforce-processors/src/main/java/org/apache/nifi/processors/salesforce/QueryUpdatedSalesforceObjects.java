package org.apache.nifi.processors.salesforce;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.salesforce.SalesforceConnectorService;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Created by gene on 4/27/17.
 */
public abstract class QueryUpdatedSalesforceObjects extends AbstractSessionFactoryProcessor {
    private static final DateTimeFormatter SF_DATETIME = DateTimeFormatter.ISO_INSTANT;

    public static final PropertyDescriptor SALESFORCE_CONNECTOR_SERVICE = new PropertyDescriptor.Builder()
            .name("salesforce-connector-service")
            .displayName("Salesforce Connector Service")
            .description("The service to use to connect to Salesforce.")
            .required(true)
            .identifiesControllerService(SalesforceConnectorService.class)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("salesforce-query")
            .displayName("Query")
            .description("The SOSQL SELECT query to execute")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor LAG = new PropertyDescriptor.Builder()
            .name("salesforce-lag")
            .displayName("Lag")
            .description("How much to lag behind the Salesforce stream.")
            .required(true)
            .defaultValue("5 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Maximum-value Columns")
            .description("A comma-separated list of column names. The processor will keep track of the maximum value "
                    + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
                    + "to the column list, and each column's values are expected to increase more slowly than the previous columns' values. Thus, "
                    + "using multiple columns implies a hierarchical structure of columns, which is usually used for partitioning tables. This processor "
                    + "can be used to retrieve only those rows that have been added/updated since the last retrieval. Note that some "
                    + "JDBC types such as bit/boolean are not conducive to maintaining maximum value, so columns of these "
                    + "types should not be listed in this property, and will result in error(s) during processing. If no columns "
                    + "are provided, all rows from the table will be considered, which could have a performance impact. NOTE: It is important "
                    + "to use consistent max-value column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    protected final Map<String, String> columnTypeMap = new HashMap<>();

    @OnScheduled
    public void setup(final ProcessContext context) {
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();

        // If there are no max-value column names specified, we don't need to perform this processing
        if (StringUtils.isEmpty(maxValueColumnNames)) {
            return;
        }

        final SalesforceConnectorService salesforceConnectorService = context.getProperty(SALESFORCE_CONNECTOR_SERVICE).asControllerService(SalesforceConnectorService.class);
        final String query = context.getProperty(QUERY).getValue();

        final PartnerConnection connection = salesforceConnectorService.getConnection();

        try {
            final SObject[] sObjects = connection.query(query + " LIMIT 1").getRecords();
            if (sObjects.length != 1) {
                throw new ProcessException("Could not return any results from the corresponding query.");
            }



        } catch (ConnectionException e) {
            throw new ProcessException("Could not connect to Salesforce", e);
        }

    }

    protected static String getMaxValueFromSobject(SObject sObject, String type) {
        return "";
    }

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SALESFORCE_CONNECTOR_SERVICE);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(LAG);
        _propertyDescriptors.add(MAX_VALUE_COLUMN_NAMES);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }


    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
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
        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());
        final SalesforceConnectorService salesforceConnectorService = context.getProperty(SALESFORCE_CONNECTOR_SERVICE).asControllerService(SalesforceConnectorService.class);

        final long endLagInSeconds = context.getProperty(LAG).asTimePeriod(TimeUnit.SECONDS);

        //final Long startTimeFromMap = Util.parseLongOrNull(statePropertyMap.get(objectType));

    }
}
