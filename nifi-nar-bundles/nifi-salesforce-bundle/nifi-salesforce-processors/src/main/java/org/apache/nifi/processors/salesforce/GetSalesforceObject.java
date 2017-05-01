package org.apache.nifi.processors.salesforce;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.services.salesforce.SalesforceConnectorService;

/**
 * Created by gene on 4/27/17.
 */
public class GetSalesforceObject extends AbstractProcessor {
    public static final PropertyDescriptor SALESFORCE_CONNECTOR_SERVICE = new PropertyDescriptor.Builder()
            .name("salesforce-connector-service")
            .displayName("Salesforce Connector Service")
            .description("The service to use to connect to Salesforce.")
            .required(true)
            .identifiesControllerService(SalesforceConnectorService.class)
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        
    }
}
