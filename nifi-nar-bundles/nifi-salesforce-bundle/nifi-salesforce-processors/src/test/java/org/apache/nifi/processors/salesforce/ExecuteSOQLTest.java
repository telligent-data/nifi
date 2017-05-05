package org.apache.nifi.processors.salesforce;

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processors.cometd.ConnectSalesforceStreaming;
import org.apache.nifi.services.salesforce.StandardSalesforceConnectorService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by gene on 5/2/17.
 */
public class ExecuteSOQLTest {
    private static final String USERNAME = "gene@telligent-data.com";
    private static final String PASSWORD = "Queenmum.1761";
    private static final String SECURITY_TOKEN = "GJCKMZCOtHLGBobIbNundm50J";
    private static final String AUTH_ENDPOINT = "https://login.salesforce.com/services/Soap/u/39.0";

    @Test
    public void testExecuteSOQL() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(ExecuteSOQL.class);
        final StandardSalesforceConnectorService service = new StandardSalesforceConnectorService();
        runner.addControllerService("salesforce-connector", service);

        runner.setProperty(service, StandardSalesforceConnectorService.USER, USERNAME);
        runner.setProperty(service, StandardSalesforceConnectorService.PASSWORD, PASSWORD);
        runner.setProperty(service, StandardSalesforceConnectorService.SECURITY_TOKEN, SECURITY_TOKEN);
        runner.setProperty(service, StandardSalesforceConnectorService.AUTH_ENDPOINT, AUTH_ENDPOINT);
        runner.assertValid(service);

        runner.enableControllerService(service);

        runner.setProperty(ExecuteSOQL.SALESFORCE_CONNECTOR_SERVICE, "salesforce-connector");

        final String baseQuery = "SELECT Account.Id, Account.Name, Account.CreatedDate, Account.Industry, Account.SystemModstamp, Id, FirstName, LastName, Phone, SystemModstamp FROM Contact WHERE SystemModstamp > #LAST_RUN_TIME# OR Account.SystemModstamp > #LAST_RUN_TIME#";
        runner.setProperty(ExecuteSOQL.QUERY, baseQuery);

        runner.assertValid();

        //runner.getStateManager().setState(ImmutableMap.of(baseQuery, String.valueOf(1493946784000L)), Scope.CLUSTER);

        runner.run();

        System.out.println(runner.getStateManager().getState(Scope.CLUSTER).toMap());

        //runner.assertAllFlowFilesTransferred(ExecuteSOQL.REL_SUCCESS, 2);
        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ExecuteSOQL.REL_SUCCESS);

        for (MockFlowFile mockFlowFile : mockFlowFiles) {
            System.out.println(new String(mockFlowFile.toByteArray()));
        }

        runner.disableControllerService(service);
    }
}