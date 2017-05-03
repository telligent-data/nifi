package org.apache.nifi.processors.cometd;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by gene on 5/1/17.
 */
public abstract class AbstractBayeuxListenerProcessor extends AbstractSessionFactoryProcessor {
    protected static final long HANDSHAKE_TIMEOUT = 10000L;
    protected static final long DISCONNECT_TIMEOUT = 10000L;

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    protected final ConcurrentMap<Relationship, Subscription> subscriptions = new ConcurrentHashMap<>();
    protected final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();

    //protected volatile HttpClient httpClient;

    protected volatile BayeuxClient client;
    protected final HttpClient httpClient = new HttpClient(new SslContextFactory(true));

    @Override
    public Set<Relationship> getRelationships() {
        return this.subscriptions.keySet();
    }



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                SSL_CONTEXT_SERVICE
        );
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();
            if (newValue == null) {
                subscriptions.remove(relationship);
            } else {
                subscriptions.put(relationship, buildSubscription(newValue, relationship, this.sessionFactoryReference));
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

    protected final ClientSessionChannel.MessageListener metaHandshakeListener = (clientSessionChannel, message) -> {
        if (message.isSuccessful()) {
            getLogger().info("Meta handshake successful, starting subscriptions for " + subscriptions);
            final ClientSession session = clientSessionChannel.getSession();
            for (Subscription subscription : subscriptions.values()) {
                subscription.subscribe(session);
            }
        }
    };

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {

        //final HttpClient httpClient = createHttpClient(context);

        try {
            httpClient.start();
        } catch (Exception e) {
            shutdownHttpClient(httpClient);
            throw e;
        }

        //this.httpClient = httpClient;
        synchronized (this) {
            this.client = createBayeuxClient(context, this.httpClient);
        }
    }

    protected HttpClient createHttpClient(ProcessContext context) throws Exception {
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

        //return (sslContextService == null) ? new HttpClient() : new HttpClient(contextFactory);
        return new HttpClient(new SslContextFactory(true));
    }


    protected void unsubscribeAll(BayeuxClient client) {
        if (client == null || !client.isHandshook()) {
            return;
        }

        for(Subscription subscription : this.subscriptions.values()) {
            subscription.unsubscribe(client);
        }
    }

    protected void shutdownHttpClient(HttpClient toShutdown) {
        if (toShutdown == null) {
            return;
        }

        try {
            toShutdown.stop();
            //toShutdown.destroy();
        } catch (final Exception ex) {
            getLogger().warn("unable to cleanly shutdown embedded server due to {}", new Object[] {ex});
            //this.httpClient = null;
        }
    }


    @OnStopped
    public void cleanup() {
        shutdownBayeuxClient(this.client);
        shutdownHttpClient(this.httpClient);
    }

    protected Subscription getSubscriptionForRelationship(Relationship relationship) {
        return this.subscriptions.get(relationship);
    }


    protected void shutdownBayeuxClient(BayeuxClient client) {
        if (client == null) {
            return;
        }

        unsubscribeAll(client);
        boolean didDisconnect = client.disconnect(DISCONNECT_TIMEOUT);

        if(!didDisconnect) {
            getLogger().warn("Unable to cleanly shutdown BayeuxClient, unexpected behavior may result");
        }

        this.client = null;
    }

    protected abstract BayeuxClient createBayeuxClient(ProcessContext context, HttpClient client);
    protected abstract Subscription buildSubscription(String channel, Relationship destinationRelationship, AtomicReference<ProcessSessionFactory> sessionFactoryReference);
}
