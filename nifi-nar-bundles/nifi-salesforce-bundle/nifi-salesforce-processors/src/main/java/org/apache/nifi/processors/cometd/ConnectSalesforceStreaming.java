package org.apache.nifi.processors.cometd;

import com.google.common.collect.ImmutableList;
import com.sforce.soap.partner.PartnerConnection;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.salesforce.SalesforceConnectorService;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Created by gene on 5/1/17.
 */
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"stream", "bayeux", "salesforce", "cometd"})
@CapabilityDescription("Connects to a remote Salesforce streaming cometd server, subscribes to particular channels, and emits the results as a raw JSON message. Supports \"replaying\" the stream from a particular point in time.")
@Stateful(scopes = {Scope.CLUSTER}, description = "Stores maximum value of replayId for each channel as cluster-wide state.")
@DynamicProperty(name = "Relationship Name", value = "channel", description = "Routes data emitted in a particular channel to a set relationship.")
@DynamicRelationship(name = "Name from Dynamic Property", description = "Flowfiles are routed to this relationship from the configured channel.")
@WritesAttributes({
        @WritesAttribute(attribute="cometd.channel", description = "Channel from which the Flowfile data was emitted"),
        @WritesAttribute(attribute="cometd.client", description = "ClientID of the cometd client"),
        @WritesAttribute(attribute="cometd.channelId", description = "ID of the channel from which the Flowfile data was emitted"),
})
public class ConnectSalesforceStreaming extends AbstractBayeuxListenerProcessor {
    private static final String AUTHORIZATION = "Authorization";
    private static final String SALESFORCE_API_VERSION = "39.0";
    public static final String COMETD_SUFFIX = "/cometd/";
    public static long REPLAY_FROM_EARLIEST = -2L;
    public static long REPLAY_FROM_TIP = -1L;

    public static final PropertyDescriptor SALESFORCE_CONNECTOR_SERVICE = new PropertyDescriptor
            .Builder().name("salesforce-connector-service")
            .displayName("Salesforce Connector Service")
            .description("Connector service used to access Salesforce.")
            .identifiesControllerService(SalesforceConnectorService.class)
            .required(true)
            .build();

    private final ConcurrentMap<String, Long> replayStatus = new ConcurrentHashMap<>();

    private class SalesforceNifiSubscription extends NifiSubscription {
        public SalesforceNifiSubscription(String channel, Relationship destinationRelationship, AtomicReference<ProcessSessionFactory> sessionFactoryReference) {
            super(channel, destinationRelationship, sessionFactoryReference);
        }

        @Override
        protected String processMessage(Message message) {
            final Object eventMetadata = message.getDataAsMap().get("event");
            if (eventMetadata instanceof Map) {
                final Long replayId = (Long) ((Map) eventMetadata).get("replayId");

                if (replayId.compareTo(replayStatus.get(message.getChannel())) > 0) {
                    replayStatus.put(message.getChannel(), replayId);
                }
            }

            return super.processMessage(message);
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SALESFORCE_CONNECTOR_SERVICE)
                .build();
    }

    @Override
    public void onScheduled(ProcessContext context) throws Exception {
        final Map<String, String> stateMap = context.getStateManager().getState(Scope.CLUSTER).toMap();

        for (Map.Entry<String, String> stateEntry : stateMap.entrySet()) {
            replayStatus.put(stateEntry.getKey(), Long.valueOf(stateEntry.getValue()));
        }

        super.onScheduled(context);
    }

    private void syncState(ProcessContext context) {
        synchronized (this) {
            final Map<String, String> stateMap = this.replayStatus.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, v -> String.valueOf(v.getValue())));
            try {
                context.getStateManager().setState(stateMap, Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Could not connect to state manager and sync internal state. Duplication may result.");
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);

        try {
            if (!this.client.isHandshook()) {
                getLogger().info("Client was not handshook or handshake expired; attempting to establish handshake now.");
                this.client.handshake(metaHandshakeListener);
                boolean handshaken = this.client.waitFor(HANDSHAKE_TIMEOUT, BayeuxClient.State.CONNECTED);
                if (!handshaken) {
                    throw new TimeoutException("Timeout of " + HANDSHAKE_TIMEOUT + " ms was reached by the client.");
                }
            }
        } catch (TimeoutException e) {
            getLogger().error("Could not perform Bayeux handshake", e);
        }

        syncState(context);
        context.yield();
    }

    @Override
    protected BayeuxClient createBayeuxClient(ProcessContext context, HttpClient client) {
        final PartnerConnection connection = context.getProperty(SALESFORCE_CONNECTOR_SERVICE)
                .asControllerService(SalesforceConnectorService.class)
                .getConnection();

        URL cometDUrl;

        try {
            final URL serviceUrl = new URL(connection.getConfig().getServiceEndpoint());
            cometDUrl = new URL(serviceUrl.getProtocol(), serviceUrl.getHost(), serviceUrl.getPort(),
                    COMETD_SUFFIX + SALESFORCE_API_VERSION);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Could not parse URL from the connection's config. This may indicate a problem with the SDK.", e);
        }

        LongPollingTransport httpTransport = new LongPollingTransport(new HashMap<>(), httpClient) {
            @Override
            protected void customize(Request request) {
                request.header(AUTHORIZATION, connection.getConfig().getSessionId());
            }
    };

        final BayeuxClient bayeuxClient = new BayeuxClient(cometDUrl.toExternalForm(), httpTransport);
        getLogger().info("Created client at endpoint " + bayeuxClient.getURL());
        bayeuxClient.addExtension(new ReplayExtension(replayStatus));
        return bayeuxClient;
    }

    @Override
    protected Subscription buildSubscription(String channel, Relationship destinationRelationship, AtomicReference<ProcessSessionFactory> sessionFactoryReference) {
        return new SalesforceNifiSubscription(channel, destinationRelationship, sessionFactoryReference);
    }
}
