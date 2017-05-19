package org.apache.nifi.processors.salesforce;

import com.google.common.collect.ImmutableMap;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.GetServerTimestampResult;
import com.sforce.soap.partner.GetUpdatedResult;
import com.sforce.soap.partner.PartnerConnection;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.services.salesforce.SalesforceConnectorService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.GregorianCalendar;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

/**
 * Created by gene on 4/26/17.
 */
public class TestListUpdatedSalesforceObjects {

    private class MockSalesforceConnectorService extends AbstractControllerService implements SalesforceConnectorService {
        private final PartnerConnection partnerConnection;

        private MockSalesforceConnectorService(final PartnerConnection partnerConnection) {
            this.partnerConnection = partnerConnection;
        }

        @Override
        public PartnerConnection getConnection() {
            return this.partnerConnection;
        }
    }

    @Mock
    PartnerConnection partnerConnection;

    @Mock
    GetUpdatedResult mockUpdatedResult;

    @Mock
    GetServerTimestampResult mockServerTimestampResult;

    private static final String[] IDS = {"test1", "test2", "test3"};
    private static final String OBJECT_TYPE = "testObjectType";
    private static final String LAG = "5 min";
    private static final String START_LAG = "25 days";
    private static final GregorianCalendar SERVER_TS = GregorianCalendar.from(ZonedDateTime.now(ZoneOffset.UTC).minusDays(5).minusHours(12).minusMinutes(4).minusSeconds(15));
    private static final GregorianCalendar END_TIME = GregorianCalendar.from(SERVER_TS.toZonedDateTime().minusMinutes(5));
    private static final GregorianCalendar START_TIME = GregorianCalendar.from(END_TIME.toZonedDateTime().minusDays(25));

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testProcessorWithNoState() throws Exception {
        reset(partnerConnection, mockUpdatedResult, mockServerTimestampResult);
        final TestRunner runner = TestRunners.newTestRunner(ListUpdatedSalesforceObjects.class);

        when(partnerConnection.getServerTimestamp()).thenReturn(mockServerTimestampResult);
        when(mockServerTimestampResult.getTimestamp()).thenReturn(SERVER_TS);
        when(partnerConnection.getUpdated(eq(OBJECT_TYPE), eq(START_TIME), eq(END_TIME))).thenReturn(mockUpdatedResult);
        when(mockUpdatedResult.getIds()).thenReturn(IDS);

        final SalesforceConnectorService connectorService = new MockSalesforceConnectorService(partnerConnection);
        runner.addControllerService("mock-service", connectorService);
        runner.setProperty(ListUpdatedSalesforceObjects.SALESFORCE_CONNECTOR_SERVICE, "mock-service");
        runner.setProperty(ListUpdatedSalesforceObjects.OBJECT_TYPE, OBJECT_TYPE);
        runner.setProperty(ListUpdatedSalesforceObjects.LAG, LAG);
        runner.setProperty(ListUpdatedSalesforceObjects.INITIAL_START_LAG, START_LAG);
        runner.enableControllerService(connectorService);
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListUpdatedSalesforceObjects.REL_SUCCESS, IDS.length);
        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListUpdatedSalesforceObjects.REL_SUCCESS);
        assertEquals(IDS.length, mockFlowFiles.size());

        for (int i = 0; i < IDS.length; i++) {
            final MockFlowFile mockFlowFile = mockFlowFiles.get(i);
            mockFlowFile.assertAttributeEquals(SalesforceAttributes.ATTR_OBJECT_ID, IDS[i]);
            mockFlowFile.assertAttributeEquals(SalesforceAttributes.ATTR_OBJECT_TYPE, OBJECT_TYPE);
        }

        final String stateMapEntry = runner.getStateManager().getState(Scope.CLUSTER).get(OBJECT_TYPE);
        assertNotNull(stateMapEntry);
        assertEquals("State should have been updated after a successful run",
                END_TIME.toInstant().toEpochMilli(),
                Long.valueOf(stateMapEntry).longValue());

    }

    @Test
    public void testProcessorWithState() throws Exception {
        reset(partnerConnection, mockUpdatedResult, mockServerTimestampResult);
        final GregorianCalendar stateCalendar = GregorianCalendar.from(ZonedDateTime.now(ZoneOffset.UTC).minusDays(14).minusHours(4).minusMinutes(10));
        final TestRunner runner = TestRunners.newTestRunner(ListUpdatedSalesforceObjects.class);

        when(partnerConnection.getServerTimestamp()).thenReturn(mockServerTimestampResult);
        when(mockServerTimestampResult.getTimestamp()).thenReturn(SERVER_TS);
        when(partnerConnection.getUpdated(eq(OBJECT_TYPE), eq(stateCalendar), eq(END_TIME))).thenReturn(mockUpdatedResult);
        when(mockUpdatedResult.getIds()).thenReturn(IDS);

        final SalesforceConnectorService connectorService = new MockSalesforceConnectorService(partnerConnection);
        runner.addControllerService("mock-service", connectorService);
        runner.setProperty(ListUpdatedSalesforceObjects.SALESFORCE_CONNECTOR_SERVICE, "mock-service");
        runner.setProperty(ListUpdatedSalesforceObjects.OBJECT_TYPE, OBJECT_TYPE);
        runner.setProperty(ListUpdatedSalesforceObjects.LAG, LAG);
        runner.enableControllerService(connectorService);
        runner.assertValid();



        runner.getStateManager().setState(ImmutableMap.of(
                OBJECT_TYPE, String.valueOf(stateCalendar.toZonedDateTime().toInstant().toEpochMilli())
        ), Scope.CLUSTER);

        runner.run();


        runner.assertAllFlowFilesTransferred(ListUpdatedSalesforceObjects.REL_SUCCESS, IDS.length);
        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListUpdatedSalesforceObjects.REL_SUCCESS);
        assertEquals(IDS.length, mockFlowFiles.size());

        for (int i = 0; i < IDS.length; i++) {
            final MockFlowFile mockFlowFile = mockFlowFiles.get(i);
            mockFlowFile.assertAttributeEquals(SalesforceAttributes.ATTR_OBJECT_ID, IDS[i]);
            mockFlowFile.assertAttributeEquals(SalesforceAttributes.ATTR_OBJECT_TYPE, OBJECT_TYPE);
        }

        final String stateMapEntry = runner.getStateManager().getState(Scope.CLUSTER).get(OBJECT_TYPE);
        assertNotNull(stateMapEntry);
        assertEquals("State should have been updated after a successful run",
                END_TIME.toInstant().toEpochMilli(),
                Long.valueOf(stateMapEntry).longValue());

    }

}
