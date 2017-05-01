package org.apache.nifi.processors.cometd;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by gene on 4/18/17.
 */
public class ConnectBayeuxTest {
    private final static Logger LOG = LoggerFactory.getLogger(ConnectBayeuxTest.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    private static final int HTTP_PORT = 8080;
    private static final int HTTPS_PORT = 8081;

    private static final String HOSTNAME = "localhost";
    private static final String HTTP_ENDPOINT = "http://" + HOSTNAME + ":" + HTTP_PORT + "/cometd/";
    private static final String HTTPS_ENDPOINT = "https://" + HOSTNAME + ":" + HTTPS_PORT + "/cometd/";

    private TestRunner testRunner;
    private CometDTestServer testServer;
    private ConnectBayeux proc;

    private static String generateServerChannel() {
        return "/" + UUID.randomUUID().toString();
    }

    @Before
    public void init() throws Exception{
        proc = new ConnectBayeux();
        testRunner = TestRunners.newTestRunner(proc);

        // By default, the test server runs on HTTP.
        this.testServer = new CometDTestServer(HTTP_PORT, false);
        this.testServer.run();
    }

    @After
    public void destroy() throws Exception {
        this.testServer.stop();
    }

    private void startClientAndSendMessages(final List<Map<String, Object>> messages, String channel) throws Exception {
        final HttpClient httpClient = new HttpClient();
        httpClient.start();

        final BayeuxClient client = new BayeuxClient(HTTP_ENDPOINT, new LongPollingTransport(new HashMap<>(), httpClient));
        client.handshake();
        boolean connected = client.waitFor(10000, BayeuxClient.State.CONNECTED);

        assertTrue(
                "Bayeux client should be able to connect to the test server",
                connected
        );

        Runnable sendMessageToCometD = () -> {
            try {
                for (final Map<String, Object> message: messages) {
                    client.getChannel(channel).publish(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Failure sending messages to cometD");
            }
        };

        new Thread(sendMessageToCometD).start();
    }

    private static boolean dataPresentInFlowfiles(List<MockFlowFile> flowFiles, Map<String, Object> expectedData, String channel) {
        final Map<String, Object> expectedMap = ImmutableMap.of(
                "data", expectedData,
                "channel", channel
        );

        boolean isPresent = false;
        for (MockFlowFile mockFlowFile : flowFiles) {
            try {
                Map<String, Object> actualMap = mapper.readValue(mockFlowFile.toByteArray(), new TypeReference<Map<String, Object>>() {});
                if (expectedMap.equals(actualMap)) {
                    isPresent = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
                fail("Could not read contents of flowfile as JSON");
            }
        }

        return isPresent;
    }

    @Test
    public void testSubscribedEventsReceived() throws Exception {
        final String channel = generateServerChannel();
        testRunner.setProperty(ConnectBayeux.ENDPOINT, HTTP_ENDPOINT);
        testRunner.setProperty("success", channel);

        testRunner.assertValid();

        final ProcessSessionFactory processSessionFactory = testRunner.getProcessSessionFactory();
        final ProcessContext context = testRunner.getProcessContext();
        proc.onScheduled(context);

        final List<Map<String, Object>> expected = ImmutableList.of(
                ImmutableMap.of("expected1key", "expected1value"),
                ImmutableMap.of("expected2key", ""),
                ImmutableMap.of("expected3key", "expected3value"),
                ImmutableMap.of("expected4key1", "expected4keyvalue1",
                        "expected4key2", new HashMap<String, Object>()),
                ImmutableMap.of("expected5key", ImmutableMap.of(
                        "expected5subkey", "expected5subvalue"))
        );

        proc.onTrigger(context, processSessionFactory);

        startClientAndSendMessages(expected, channel);

        long responseTimeout = 10000;

        int numTransferred = 0;
        long startTime = System.currentTimeMillis();
        while (numTransferred < expected.size()  && (System.currentTimeMillis() - startTime < responseTimeout)) {
            proc.onTrigger(context, processSessionFactory);
            numTransferred = testRunner.getFlowFilesForRelationship("success").size();
            Thread.sleep(100);
        }

        proc.cleanup();

        testRunner.assertTransferCount("success", expected.size());

        List<MockFlowFile> mockFlowFiles = testRunner.getFlowFilesForRelationship("success");

        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected1key", "expected1value"), channel));
        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected2key", ""), channel));
        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected3key", "expected3value"), channel));
        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected4key1", "expected4keyvalue1",
                "expected4key2", new HashMap<String, Object>()), channel));
        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected5key", ImmutableMap.of(
                "expected5subkey", "expected5subvalue")), channel));
    }

    @Test
    public void testSubscriptionUpdateOnConfigChange() throws Exception {
        final String channel = generateServerChannel();
        testRunner.setProperty(ConnectBayeux.ENDPOINT, HTTP_ENDPOINT);
        testRunner.setProperty("success", channel);

        testRunner.assertValid();

        final ProcessSessionFactory processSessionFactory = testRunner.getProcessSessionFactory();
        final ProcessContext context = testRunner.getProcessContext();
        proc.onScheduled(context);

        final List<Map<String, Object>> expected = ImmutableList.of(
                ImmutableMap.of("expected1key", "expected1value")
        );

        proc.onTrigger(context, processSessionFactory);

        startClientAndSendMessages(expected, channel);

        long responseTimeout = 10000;

        int numTransferred = 0;
        long startTime = System.currentTimeMillis();
        while (numTransferred < expected.size()  && (System.currentTimeMillis() - startTime < responseTimeout)) {
            proc.onTrigger(context, processSessionFactory);
            numTransferred = testRunner.getFlowFilesForRelationship("success").size();
            Thread.sleep(100);
        }

        proc.cleanup();

        testRunner.assertTransferCount("success", expected.size());

        List<MockFlowFile> mockFlowFiles = testRunner.getFlowFilesForRelationship("success");

        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected1key", "expected1value"), channel));

        testRunner.removeProperty(new PropertyDescriptor.Builder().name("success").build());
        testRunner.setProperty("other", channel);
        testRunner.clearTransferState();

        proc.onScheduled(context);
        proc.onTrigger(context, processSessionFactory);

        startClientAndSendMessages(expected, channel);

        responseTimeout = 10000;

        numTransferred = 0;
        startTime = System.currentTimeMillis();
        while (numTransferred < expected.size()  && (System.currentTimeMillis() - startTime < responseTimeout)) {
            proc.onTrigger(context, processSessionFactory);
            numTransferred = testRunner.getFlowFilesForRelationship("other").size();
            Thread.sleep(100);
        }

        proc.cleanup();

        testRunner.assertAllFlowFilesTransferred("other", 1);
        mockFlowFiles = testRunner.getFlowFilesForRelationship("other");
        assertTrue(dataPresentInFlowfiles(mockFlowFiles, ImmutableMap.of("expected1key", "expected1value"), channel));

    }


    private SSLContextService configureProcessorSslContextService() throws InitializationException {
        final SSLContextService sslContextService = new StandardSSLContextService();
        testRunner.addControllerService("ssl-context", sslContextService);
        testRunner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        testRunner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        testRunner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        testRunner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        testRunner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        testRunner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        testRunner.enableControllerService(sslContextService);

        testRunner.setProperty(ConnectBayeux.SSL_CONTEXT_SERVICE, "ssl-context");
        return sslContextService;
    }

    @Test
    public void testSubscribedEventsReceivedSSL() throws Exception {
        final CometDTestServer httpsTestServer = new CometDTestServer(HTTPS_PORT, true);
        httpsTestServer.run();

        final String channel = generateServerChannel();
        testRunner.setProperty(ConnectBayeux.ENDPOINT, HTTPS_ENDPOINT);
        testRunner.setProperty("success", channel);
        configureProcessorSslContextService();

        testRunner.assertValid();
    }
}
