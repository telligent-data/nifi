package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.salesforce.SalesforceConnectorService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gene on 5/1/17.
 */
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "salesforce", "query", "soql"})
@CapabilityDescription("Execute provided SOQL query. Query result will be converted to JSON format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executesoql.row.count' indicates how many rows were selected.")
@WritesAttribute(attribute="executesql.row.count", description = "Contains the number of rows returned in the select query")
public class ExecuteSOQL extends AbstractProcessor {

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
            .description("The query to execute through the Salesforce API.")
            .required(true)
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
        _propertyDescriptors.add(SALESFORCE_CONNECTOR_SERVICE);
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


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            final String query = context.getProperty(QUERY).getValue();
            final SalesforceConnectorService connectorService = context.getProperty(SALESFORCE_CONNECTOR_SERVICE).asControllerService(SalesforceConnectorService.class);
            final Boolean queryAll = context.getProperty(QUERY_ALL).asBoolean();

            final PartnerConnection connection = connectorService.getConnection();

            boolean done = false;
            QueryResult queryResult = (queryAll) ? connection.queryAll(query) : connection.query(query);
            final int size = queryResult.getSize();
            while (!done) {
                for (SObject sObject : queryResult.getRecords()) {
                    final Map<String, String> attributeMap = new HashMap<>();
                    attributeMap.put("executesoql.row.count", String.valueOf(size));

                    FlowFile flowFile = session.create();

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

            session.commit();

        } catch (ConnectionException e) {
            getLogger().error("Could not connect to Salesforce", e);
        }
    }
}
