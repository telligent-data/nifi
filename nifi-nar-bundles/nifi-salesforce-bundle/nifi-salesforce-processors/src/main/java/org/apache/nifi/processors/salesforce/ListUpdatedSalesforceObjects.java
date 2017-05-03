package org.apache.nifi.processors.salesforce;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.InvalidSObjectFault;
import com.sforce.ws.ConnectionException;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.salesforce.SalesforceConnectorService;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@DefaultSchedule(period = "5 min")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"salesforce", "updated", "get"})
@CapabilityDescription("")
@Stateful(scopes = Scope.CLUSTER, description = "")
public class ListUpdatedSalesforceObjects extends AbstractProcessor {
    public static final long DEFAULT_INITIAL_LAG_SECONDS = 28 * 24 * 60 * 60;


    public static final PropertyDescriptor SALESFORCE_CONNECTOR_SERVICE = new PropertyDescriptor.Builder()
            .name("salesforce-connector-service")
            .displayName("Salesforce Connector Service")
            .description("The service to use to connect to Salesforce.")
            .required(true)
            .identifiesControllerService(SalesforceConnectorService.class)
            .build();

    public static final PropertyDescriptor OBJECT_TYPE = new PropertyDescriptor.Builder()
            .name("salesforce-object-type")
            .displayName("Salesforce object type")
            .description("The Salesforce object type which will be queried for updated data.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INITIAL_START_LAG = new PropertyDescriptor.Builder()
            .name("salesforce-initial-start-time")
            .displayName("Initial start time")
            .description("The beginning time to check for updates. By default, Salesforce only returns up to a maximum of 30 days ")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor LAG = new PropertyDescriptor.Builder()
            .name("salesforce-lag")
            .displayName("Lag")
            .description("How much to lag behind the Salesforce stream.")
            .required(true)
            .defaultValue("5 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SALESFORCE_CONNECTOR_SERVICE);
        _propertyDescriptors.add(OBJECT_TYPE);
        _propertyDescriptors.add(LAG);
        _propertyDescriptors.add(INITIAL_START_LAG);
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


    @OnScheduled
    public void setup(final ProcessContext context) {
        final SalesforceConnectorService salesforceConnectorService = context.getProperty(SALESFORCE_CONNECTOR_SERVICE).asControllerService(SalesforceConnectorService.class);
        final String objectType = context.getProperty(OBJECT_TYPE).getValue();

        try {
            salesforceConnectorService.getConnection().describeSObjects(new String[]{objectType});
        } catch (InvalidSObjectFault e) {
            throw new ProcessException("Invalid object type of " + objectType + " provided.", e);
        } catch (ConnectionException e) {
            throw new ProcessException("Could not connect to Salesforce", e);
        }

    }

    @Override
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
        final String objectType = context.getProperty(OBJECT_TYPE).getValue();

        final long endLagInSeconds = context.getProperty(LAG).asTimePeriod(TimeUnit.SECONDS);

        final Long startTimeFromMap = Util.parseLongOrNull(statePropertyMap.get(objectType));

        try {
            final PartnerConnection connection = salesforceConnectorService.getConnection();

            // End time is Server Timestamp - {endLagInSeconds} seconds
            final GregorianCalendar endTime = GregorianCalendar.from(
                    ((GregorianCalendar) connection.getServerTimestamp().getTimestamp())
                            .toZonedDateTime()
                            .minus(endLagInSeconds, ChronoUnit.SECONDS)
            );

            // For start time: If the state isn't null, pull it from state. Otherwise, if INITIAL_START_LAG is set,
            // subtract that from the end time and use that. Otherwise, subtract the default from the end time.
            GregorianCalendar startTime = (startTimeFromMap != null) ?
                    GregorianCalendar.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimeFromMap), ZoneOffset.UTC))
                    : GregorianCalendar.from(
                    (context.getProperty(INITIAL_START_LAG) != null) ?
                            endTime.toZonedDateTime().minusSeconds(context.getProperty(INITIAL_START_LAG).asTimePeriod(TimeUnit.SECONDS))
                            : endTime.toZonedDateTime().minusSeconds(DEFAULT_INITIAL_LAG_SECONDS));

            final String[] ids = connection.getUpdated(objectType, startTime, endTime).getIds();

            for (String id : ids) {
                final Map<String, String> attributes = new HashMap<>();
                attributes.put(CoreAttributes.FILENAME.key(), objectType + "-" + id);
                attributes.put(SalesforceAttributes.ATTR_OBJECT_TYPE, objectType);
                attributes.put(SalesforceAttributes.ATTR_OBJECT_ID, id);

                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);
            }

            if (ids.length > 0) {
                session.commit();
                statePropertyMap.put(objectType, String.valueOf(endTime.toInstant().toEpochMilli()));
                try {
                    context.getStateManager().setState(statePropertyMap, Scope.CLUSTER);
                } catch (IOException e) {
                    getLogger().error("Could not set state after processor execution. Duplicate data may result.", e);
                }
            }

        } catch (ConnectionException e) {
            getLogger().error("Could not connect to Salesforce", e);
        }
    }
}
