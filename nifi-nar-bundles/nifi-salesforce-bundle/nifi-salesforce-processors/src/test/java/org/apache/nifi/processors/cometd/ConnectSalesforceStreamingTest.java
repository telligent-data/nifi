package org.apache.nifi.processors.cometd;

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.services.salesforce.StandardSalesforceConnectorService;
import org.apache.nifi.services.salesforce.TestProcessor;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by gene on 5/2/17.
 */
public class ConnectSalesforceStreamingTest {
    private static final String USERNAME = "gene@telligent-data.com";
    private static final String PASSWORD = "Queenmum.1761";
    private static final String SECURITY_TOKEN = "GJCKMZCOtHLGBobIbNundm50J";
    private static final String AUTH_ENDPOINT = "https://login.salesforce.com/services/Soap/u/39.0";

    private static final String TRUSTSTORE_PATH = "src/test/resources/ca-truststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "nifitrust";


    @Test
    public void testConnectToSalesforceStreaming() throws Exception {

        final ConnectSalesforceStreaming processor = new ConnectSalesforceStreaming();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        final StandardSalesforceConnectorService service = new StandardSalesforceConnectorService();
        runner.addControllerService("salesforce-connector", service);

        runner.setProperty(service, StandardSalesforceConnectorService.USER, USERNAME);
        runner.setProperty(service, StandardSalesforceConnectorService.PASSWORD, PASSWORD);
        runner.setProperty(service, StandardSalesforceConnectorService.SECURITY_TOKEN, SECURITY_TOKEN);
        runner.setProperty(service, StandardSalesforceConnectorService.AUTH_ENDPOINT, AUTH_ENDPOINT);
        runner.assertValid(service);

        runner.enableControllerService(service);

        runner.setProperty(ConnectSalesforceStreaming.SALESFORCE_CONNECTOR_SERVICE, "salesforce-connector");

        runner.setProperty("success", "/topic/InvoiceStatementUpdates");
        /*
        final StandardSSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, TRUSTSTORE_PATH);
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, StandardSSLContextService.STORE_TYPE_JKS);
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD);


        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");



        runner.assertValid(sslContextService);
        runner.enableControllerService(sslContextService);



        runner.setProperty(ConnectSalesforceStreaming.SSL_CONTEXT_SERVICE, "ssl-context");

        */

        runner.getStateManager().setState(ImmutableMap.of(
                "/topic/InvoiceStatementUpdates",
                String.valueOf(2L)
        ), Scope.CLUSTER);

        final ProcessContext context = runner.getProcessContext();
        final ProcessSessionFactory factory = runner.getProcessSessionFactory();
        processor.onScheduled(context);

        processor.onTrigger(context, factory);

        Thread.sleep(10000L);
        processor.onTrigger(context, factory);
        processor.cleanup();

        System.out.println("State map:");
        System.out.println(runner.getStateManager().getState(Scope.CLUSTER).toMap());

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");

        for (MockFlowFile flowFile : flowFiles) {
            System.out.println("--------------");
            System.out.println(new String(flowFile.toByteArray()));
            System.out.println(flowFile.getAttributes());
        }
    }

}