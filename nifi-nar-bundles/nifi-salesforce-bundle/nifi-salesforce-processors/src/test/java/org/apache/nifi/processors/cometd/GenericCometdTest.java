package org.apache.nifi.processors.cometd;

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

/**
 * Created by gene on 5/3/17.
 */
public class GenericCometdTest {
    private static final int HTTP_PORT = 9999;
    private CometDTestServer testServer;

    @Before
    public void init() throws Exception{

        // By default, the test server runs on HTTP.
        this.testServer = new CometDTestServer(HTTP_PORT, false);
        this.testServer.run();
    }

    @After
    public void destroy() throws Exception {
        this.testServer.stop();
    }

    @Test
    public void sendMessages() throws Exception {


        final HttpClient httpClient = new HttpClient();
        httpClient.start();

        final BayeuxClient client = new BayeuxClient("http://localhost:9999/cometd/", new LongPollingTransport(new HashMap<>(), httpClient));
        client.handshake();
        boolean connected = client.waitFor(10000, BayeuxClient.State.CONNECTED);


        assertTrue(
                "Bayeux client should be able to connect to the test server",
                connected
        );

        final String channel = "/topic/test";
        this.testServer.createPersistentChannel("/topic/test");

        while(true) {
            client.getChannel(channel).publish(ImmutableMap.of("testKey", "testValue"));

            Thread.sleep(10000);
        }
    }
}
