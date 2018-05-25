package org.apache.nifi.processors.salesforce;

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.services.salesforce.StandardSalesforceConnectorService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.List;

/**
 * Created by gene on 5/2/17.
 */
public class ExecuteBulkSOQLTest {
    private static final String USERNAME = "gene@telligent-data.com";
    private static final String PASSWORD = "1761.Queenmumnvg4kVdnttN74pyX748K0mafi";
    private static final String SECURITY_TOKEN = "GJCKMZCOtHLGBobIbNundm50J";
    private static final String AUTH_ENDPOINT = "https://login.salesforce.com/services/Soap/u/39.0";

    public void _testExecuteSOQL() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(ExecuteBulkSOQL.class);
        runner.setProperty(ExecuteBulkSOQL.USER, USERNAME);
        runner.setProperty(ExecuteBulkSOQL.PASSWORD, PASSWORD);
        runner.setProperty(ExecuteBulkSOQL.INCLUDE_HEADER, "true");

        final String baseQuery = "SELECT Account.Id, Account.Name, Account.CreatedDate, Account.Industry, Account.SystemModstamp, Id, FirstName, LastName, Phone, SystemModstamp FROM Contact WHERE SystemModstamp > #LAST_RUN_TIME# OR Account.SystemModstamp > #LAST_RUN_TIME#";
        runner.setProperty(ExecuteBulkSOQL.QUERY, baseQuery);

        runner.assertValid();

        //runner.getStateManager().setState(ImmutableMap.of(baseQuery, String.valueOf(1493946784000L)), Scope.CLUSTER);
        //runner.getStateManager().setState(ImmutableMap.of(baseQuery, String.valueOf(1500472891000L)), Scope.CLUSTER);

        runner.run();

        System.out.println(runner.getStateManager().getState(Scope.CLUSTER).toMap());

        //runner.assertAllFlowFilesTransferred(ExecuteSOQL.REL_SUCCESS, 2);
        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ExecuteSOQL.REL_SUCCESS);

        for (MockFlowFile mockFlowFile : mockFlowFiles) {
            System.out.println(new String(mockFlowFile.toByteArray()));
        }
    }


    @Test
    public void testExecuteWithIncomingFlowfiles() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(ExecuteBulkSOQL.class);
        runner.setProperty(ExecuteBulkSOQL.USER, "${user}");
        runner.setProperty(ExecuteBulkSOQL.PASSWORD, "${password}");
        runner.setProperty(ExecuteBulkSOQL.INCLUDE_HEADER, "true");




        runner.setProperty(ExecuteBulkSOQL.QUERY, "${query}");

        runner.assertValid();

        //runner.getStateManager().setState(ImmutableMap.of(baseQuery, String.valueOf(1493946784000L)), Scope.CLUSTER);
        //runner.getStateManager().setState(ImmutableMap.of(baseQuery, String.valueOf(1500472891000L)), Scope.CLUSTER);

        final String baseQuery = "SELECT Account.Id, Account.Name, Account.CreatedDate, Account.Industry, Account.SystemModstamp, Id, FirstName, LastName, Phone, SystemModstamp FROM Contact WHERE SystemModstamp > #LAST_RUN_TIME# OR Account.SystemModstamp > #LAST_RUN_TIME#";
        final MockFlowFile mockFlowFile1 = new MockFlowFile(1L);

        runner.enqueue("", ImmutableMap.of(
                "user", USERNAME,
                "password", PASSWORD,
                "query", baseQuery
        ));

        runner.enqueue("", ImmutableMap.of(
                "user", USERNAME,
                "password", PASSWORD,
                "query", baseQuery
        ));

        runner.run(2);

        System.out.println(runner.getStateManager().getState(Scope.CLUSTER).toMap());

        //runner.assertAllFlowFilesTransferred(ExecuteSOQL.REL_SUCCESS, 2);
        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ExecuteSOQL.REL_SUCCESS);

        for (MockFlowFile mockFlowFile : mockFlowFiles) {
            System.out.println(new String(mockFlowFile.toByteArray()));
        }
    }
}