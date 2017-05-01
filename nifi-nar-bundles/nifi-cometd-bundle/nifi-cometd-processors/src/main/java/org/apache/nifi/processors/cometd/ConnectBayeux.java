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

import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"cometd", "bayeux"})
@CapabilityDescription("Listens to one or more channels on a " +
        "Bayeux Protocol enabled server.")
@DynamicProperty(name = "Channel", value = "Destination Relationship", supportsExpressionLanguage = true, description = "Routes FlowFiles received from a particular channel subscription to the specified relationship.")
@DynamicRelationship(name = "Value from Dynamic Property", description = "FlowFiles that are created from a particular Bayeux subscription")
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConnectBayeux extends AbstractSessionFactoryProcessor {
    final Logger LOG = LoggerFactory.getLogger(ConnectBayeux.class);

    private static final long HANDSHAKE_TIMEOUT = 10000L;
    private static final long DISCONNECT_TIMEOUT = 10000L;

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("bayeux-endpoint")
            .displayName("Endpoint")
            .description("Bayeux-enabled endpoint to connect to. For example: https://localhost:8080/cometd")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private List<PropertyDescriptor> descriptors;

    private final ConcurrentMap<Relationship, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
    private volatile HttpClient httpClient;
    private volatile BayeuxClient client;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = ImmutableList.of(
                ENDPOINT,
                SSL_CONTEXT_SERVICE
        );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.subscriptions.keySet();
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();
            if (newValue == null) {
                subscriptions.remove(relationship);
            } else {
                subscriptions.put(relationship, new Subscription(newValue, relationship, this.sessionFactoryReference));
            }
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(false)
                .build();
    }

    private final ClientSessionChannel.MessageListener metaHandshakeListener = new ClientSessionChannel.MessageListener() {
        @Override
        public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
            LOG.info("Meta handshake callback reached");
            if (message.isSuccessful()) {
                LOG.info("Meta handshake successful, starting subscriptions for " + subscriptions);
                final ClientSession session = clientSessionChannel.getSession();
                for (Subscription subscription : subscriptions.values()) {
                    subscription.subscribe(session);
                }
            }
        }
    };

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {
        createHttpClient(context);
        createBayeuxClient(context);
    }

    private void createHttpClient(ProcessContext context) throws Exception {
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final boolean needClientAuth = sslContextService != null && sslContextService.getTrustStoreFile() != null;

        final SslContextFactory contextFactory = new SslContextFactory();
        contextFactory.setNeedClientAuth(needClientAuth);

        if (needClientAuth) {
            contextFactory.setTrustStorePath(sslContextService.getTrustStoreFile());
            contextFactory.setTrustStoreType(sslContextService.getTrustStoreType());
            contextFactory.setTrustStorePassword(sslContextService.getTrustStorePassword());
        }

        final String keystorePath = sslContextService == null ? null : sslContextService.getKeyStoreFile();
        if (keystorePath != null) {
            final String keystorePassword = sslContextService.getKeyStorePassword();
            final String keyStoreType = sslContextService.getKeyStoreType();

            contextFactory.setKeyStorePath(keystorePath);
            contextFactory.setKeyManagerPassword(keystorePassword);
            contextFactory.setKeyStorePassword(keystorePassword);
            contextFactory.setKeyStoreType(keyStoreType);
        }

        final HttpClient client = (keystorePath == null) ? new HttpClient() : new HttpClient(contextFactory);

        try {
            client.start();
        } catch (Exception e) {
            shutdownHttpClient(client);
            throw e;
        }

        this.httpClient = client;
    }

    private void createBayeuxClient(ProcessContext context) {
        final String url = context.getProperty(ENDPOINT).getValue();

        // Todo: add support for websocket transport
        final ClientTransport transport = new LongPollingTransport(new HashMap<>(), this.httpClient);
        final BayeuxClient bayeuxClient = new BayeuxClient(url, transport);

        //bayeuxClient.getChannel(Channel.META_HANDSHAKE).subscribe(metaHandshakeListener);
        //LOG.info("Subscribed to meta handshake channel");
        this.client = bayeuxClient;
    }

    @OnStopped
    public void cleanup() {
        shutdownBayeuxClient(this.client);
        shutdownHttpClient(this.httpClient);
    }

    private void shutdownBayeuxClient(BayeuxClient client) {
        if (client == null) {
            return;
        }

        unsubscribeAll(client);
        boolean didDisconnect = client.disconnect(DISCONNECT_TIMEOUT);

        if(!didDisconnect) {
            getLogger().warn("unable to cleanly shutdown BayeuxClient, unexpected behavior may result");
        }

        this.client = null;
    }

    private void unsubscribeAll(BayeuxClient client) {
        if (client == null || !client.isHandshook()) {
            return;
        }

        for(Subscription subscription : this.subscriptions.values()) {
            subscription.unsubscribe(client);
        }
    }

    private void shutdownHttpClient(HttpClient toShutdown) {
        if (toShutdown == null) {
            return;
        }

        try {
            toShutdown.stop();
            toShutdown.destroy();
        } catch (final Exception ex) {
            getLogger().warn("unable to cleanly shutdown embedded server due to {}", new Object[] {ex});
            this.httpClient = null;
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);

        try {
            if (!this.client.isHandshook()) {
                LOG.info("Performing handshake");
                this.client.handshake(metaHandshakeListener);
                boolean handshaken = this.client.waitFor(HANDSHAKE_TIMEOUT, BayeuxClient.State.CONNECTED);
                if (!handshaken) {
                    throw new TimeoutException("Timeout of " + HANDSHAKE_TIMEOUT + " ms was reached by the client.");
                }
                LOG.info("Handshake successful.");
            }
        } catch (TimeoutException e) {
            LOG.error("Could not perform Bayeux handshake",e);
            getLogger().error("Could not perform Bayeux handshake", e);
        }

        context.yield();
    }

}
