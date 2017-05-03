package org.apache.nifi.processors.cometd;

import org.apache.nifi.processor.Relationship;
import org.cometd.bayeux.client.ClientSession;

/**
 * Created by gene on 5/1/17.
 */
public interface Subscription {
    void subscribe(ClientSession session);
    void unsubscribe(ClientSession session);
    String getChannel();
    Relationship getRelationship();
}
