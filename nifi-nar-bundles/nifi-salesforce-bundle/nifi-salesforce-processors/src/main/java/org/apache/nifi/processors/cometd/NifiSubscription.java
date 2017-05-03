package org.apache.nifi.processors.cometd;

import com.google.common.base.Objects;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.StopWatch;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by gene on 4/18/17.
 */
public class NifiSubscription implements Subscription, ClientSessionChannel.MessageListener
{
    private static final Logger LOG = LoggerFactory.getLogger(NifiSubscription.class);

    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference;
    //private final SubscriptionOld subscription;

    private final String channel;
    private final Relationship destinationRelationship;

    public NifiSubscription(String channel, Relationship destinationRelationship, AtomicReference<ProcessSessionFactory> sessionFactoryReference) {
        this.channel = channel;
        this.destinationRelationship = destinationRelationship;
        this.sessionFactoryReference = sessionFactoryReference;
    }

    public void subscribe(ClientSession client) {
        LOG.info("Subscribing to channel " + this.channel + " for relationship " + this.destinationRelationship);
        client.getChannel(this.channel).subscribe(this, new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                if(message.isSuccessful()) {
                    LOG.info("Subscribed to channel " + channel + " with message " + message);
                }
            }
        });
    }

    public void unsubscribe(ClientSession client) {
        client.getChannel(this.channel).unsubscribe(this);
    }

    public String getChannel() {
        return channel;
    }

    public Relationship getRelationship() {
        return destinationRelationship;
    }

    protected String processMessage(Message message) {
        return message.getJSON();
    }

    @Override
    public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
        ProcessSessionFactory sessionFactory;
        do {
            sessionFactory = this.sessionFactoryReference.get();
            if (sessionFactory == null) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {

                }
            }
        } while (sessionFactory == null);

        final ProcessSession processSession = sessionFactory.createSession();
        final StopWatch watch = new StopWatch();
        watch.start();
        try {
            final Map<String, String> attributeMap = new HashMap<>();
            FlowFile flowFile = processSession.create();
            flowFile = processSession.write(flowFile, (out) -> {
                final Writer writer = new BufferedWriter(new OutputStreamWriter(out));
                writer.write(processMessage(message));
                writer.close();
            });

            attributeMap.put("cometd.channel", message.getChannel());
            attributeMap.put("cometd.client", message.getClientId());
            attributeMap.put("cometd.channelId", message.getChannelId().getId());

            flowFile = processSession.putAllAttributes(flowFile, attributeMap);
            watch.stop();
            processSession.transfer(flowFile, this.destinationRelationship);
            processSession.commit();
        } finally {

        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NifiSubscription
                && Objects.equal(this.channel, ((NifiSubscription) obj).channel)
                && Objects.equal(this.destinationRelationship, ((NifiSubscription) obj).destinationRelationship);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(),
                this.channel, this.destinationRelationship);
    }

    @Override
    public String toString() {
        return super.toString() + "{channel=" + this.channel + ",relationship=" + this.destinationRelationship + "}";
    }
}
