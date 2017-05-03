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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"cometd", "bayeux"})
@CapabilityDescription("Listens to one or more channels on a " +
        "Bayeux Protocol enabled server.")
@DynamicProperty(name = "Channel", value = "Destination Relationship", supportsExpressionLanguage = true, description = "Routes FlowFiles received from a particular channel subscription to the specified relationship.")
@DynamicRelationship(name = "Value from Dynamic Property", description = "FlowFiles that are created from a particular Bayeux subscription")
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConnectBayeux extends AbstractBayeuxListenerProcessor {

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("bayeux-endpoint")
            .displayName("Endpoint")
            .description("Bayeux-enabled endpoint to connect to. For example: https://localhost:8080/cometd")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(ENDPOINT)
                .build();
    }

    @Override
    protected BayeuxClient createBayeuxClient(ProcessContext context, HttpClient client) {
        final String url = context.getProperty(ENDPOINT).getValue();
        final ClientTransport transport = new LongPollingTransport(new HashMap<>(), client);
        return new BayeuxClient(url, transport);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
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

        context.yield();
    }

    @Override
    protected Subscription buildSubscription(String channel, Relationship destinationRelationship, AtomicReference<ProcessSessionFactory> sessionFactoryReference) {
        return new NifiSubscription(channel, destinationRelationship, sessionFactoryReference);
    }
}
