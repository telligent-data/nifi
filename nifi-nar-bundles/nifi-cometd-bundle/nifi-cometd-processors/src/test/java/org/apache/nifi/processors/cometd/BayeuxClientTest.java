/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.cometd;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


public class BayeuxClientTest {
    private static final int HTTP_PORT = 8080;
    private static final int HTTPS_PORT = 8081;

    private CometDTestServer testServer;
    private CometDTestServer testServerHttps;

    public static class NoopMessageListener implements ClientSessionChannel.MessageListener {
        @Override
        public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
            //NOOP
        }
    }

    @Before
    public void init() throws Exception {
        this.testServer = new CometDTestServer(HTTP_PORT, false);
        //this.testServerHttps = new CometDTestServer(HTTPS_PORT, true);
        this.testServer.run();
        //this.testServerHttps.run();
    }

    @After
    public void destroy() throws Exception {
        this.testServer.stop();
        //this.testServerHttps.stop();
    }

    private static String generateServerChannel() {
        return "/" + UUID.randomUUID().toString();
    }

    private static HttpClient configureHttpsClient() throws FileNotFoundException {
        Path keyStoreFile = Paths.get("src/test/resources/keystore.jks");
        if (!Files.exists(keyStoreFile)) {
            throw new FileNotFoundException(keyStoreFile.toString());
        }
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(keyStoreFile.toString());
        sslContextFactory.setKeyStorePassword("testing");
        sslContextFactory.setKeyManagerPassword("testing");
        return new HttpClient(sslContextFactory);
    }

    private void _testHandshake(BayeuxClient client) throws InterruptedException {
        client.handshake();
        final boolean successfulConnect = client.waitFor(10000L, BayeuxClient.State.CONNECTED);
        assertTrue("Client must successfully perform a handshake with the test server", successfulConnect);
    }

    private void _testDisconnect(BayeuxClient client) throws Exception {
        client.disconnect();
        final boolean successfulDisconnect = client.waitFor(10000L, BayeuxClient.State.DISCONNECTED);
        assertTrue("Client must successfully perform a clean disconnect with the test server", successfulDisconnect);
    }

    private void _testSendMessage(BayeuxClient client) throws Exception {
        assert client.isConnected() && client.isHandshook();
        final String channelName = generateServerChannel();

        final Map<String, Object> data = ImmutableMap.of("testKey", "testValue");

        final CountDownLatch latch = new CountDownLatch(1);
        client.getChannel(channelName).publish(data, new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                if (message.isSuccessful()) {
                    latch.countDown();
                }
            }
        });

        final boolean successful = latch.await(10L, TimeUnit.SECONDS);
        assertTrue(
                "Client should be able to publish data to the server",
                successful
        );

    }

    private void _testMessageNotification(BayeuxClient client) throws Exception {
        assert client.isConnected() && client.isHandshook();
        final String channelName = generateServerChannel();

        final Map<String, Object> expectedData = ImmutableMap.of("testKey", "testValue");
        final SettableFuture<Map<String, Object>> dataFuture = SettableFuture.create();
        final CountDownLatch subscribeLatch = new CountDownLatch(1);

        final ClientSessionChannel.MessageListener dataListener = new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                dataFuture.set(message.getDataAsMap());
            }
        };

        client.getChannel(channelName).subscribe(dataListener, new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                if (message.isSuccessful()) {
                    subscribeLatch.countDown();
                }
            }
        });

        final boolean successfulSubscribe = subscribeLatch.await(10L, TimeUnit.SECONDS);
        assertTrue(
                "Client should be able to subscribe to arbitrary channels on the server",
                successfulSubscribe
        );

        client.getChannel(channelName).publish(expectedData);

        final Map<String, Object> actualData = dataFuture.get(10L, TimeUnit.SECONDS);

        assertEquals(
                "Client should be able to receive data from a subscription, and the data should match what is being sent",
                expectedData,
                actualData
        );

        final CountDownLatch unsubscribeLatch = new CountDownLatch(1);
        client.getChannel(channelName).unsubscribe(dataListener, new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                if (message.isSuccessful()) {
                    unsubscribeLatch.countDown();
                }
            }
        });

        final boolean successfulUnsubscribe = unsubscribeLatch.await(10L, TimeUnit.SECONDS);
        assertTrue(
                "Client should be able to unsubscribe from arbitrary channels that it's subscribed to",
                successfulUnsubscribe
        );



    }

    public void Https() throws Exception {
        assert this.testServerHttps.isRunning();
        assert this.testServerHttps.isTls();

        ClientTransport transport = new LongPollingTransport(new HashMap<>(),configureHttpsClient());
        BayeuxClient client = new BayeuxClient("https://localhost:" + HTTPS_PORT + "/cometd/", transport);

        this._testHandshake(client);

        this._testDisconnect(client);

        //final String serverChannel = generateServerChannel();

        //this.testServer.createPersistentChannel(serverChannel);

    }

    @Test
    public void testHttpProcessor() throws Exception {
        assert this.testServer.isRunning();

        final String serverChannel = generateServerChannel();

        this.testServer.createPersistentChannel(serverChannel);

        final HttpClient httpClient = new HttpClient();
        httpClient.start();
        final Map<String, Object> config = new HashMap<>();
        ClientTransport transport = new LongPollingTransport(config, httpClient);
        BayeuxClient client = new BayeuxClient("http://localhost:" + HTTP_PORT + "/cometd/", transport);

        this._testHandshake(client);

        this._testSendMessage(client);
        this._testMessageNotification(client);

        this._testDisconnect(client);

        /*
        final CountDownLatch latch = new CountDownLatch(1);

        client.handshake(new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                if (message.isSuccessful()) {
                    latch.countDown();
                }
            }
        });

        final boolean successfulConnect = latch.await(10L, TimeUnit.SECONDS);
        assertTrue("Client must successfully perform a handshake with the test server", successfulConnect);
        System.out.println(successfulConnect);

        */

        //this._testHandshake(client);

        //this._testDisconnect(client);

        /*
        CountDownLatch lock = new CountDownLatch(1);

        client.getChannel("/logs").subscribe(new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                System.out.println("log received:" + message);
            }
        }, new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                System.out.println("Subscribed");
                lock.countDown();
            }
        });

        lock.await();

        CountDownLatch publishLock = new CountDownLatch(1);

        Map<String, Object> data = new HashMap<>();
        data.put("testKey", "testValue");

        client.getChannel("/logs").publish(data, new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                System.out.println("data published");
                publishLock.countDown();
            }
        });

        publishLock.await();
        */

    }

}
