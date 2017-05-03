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
package org.apache.nifi.services.salesforce;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.MessageHandler;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "example"})
@CapabilityDescription("Example ControllerService implementation of SalesforceConnectorService.")
public class StandardSalesforceConnectorService extends AbstractControllerService implements SalesforceConnectorService {

    public static final PropertyDescriptor USER = new PropertyDescriptor
            .Builder().name("sf.user")
            .displayName("User")
            .description("User account with which to login to Salesforce.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("sf.password")
            .displayName("Password")
            .description("Password for authentication.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURITY_TOKEN = new PropertyDescriptor
            .Builder().name("sf.security-token")
            .displayName("Security token")
            .description("Security token for authorizaiton. ")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_ENDPOINT = new PropertyDescriptor
            .Builder().name("sf.auth-endpoint")
            .displayName("Authentication endpoint")
            .description("Endpoint for authentication with the SOAP API.")
            .required(true)
            .defaultValue("https://login.salesforce.com/services/Soap/u/39.0")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(USER);
        props.add(PASSWORD);
        props.add(SECURITY_TOKEN);
        props.add(AUTH_ENDPOINT);
        properties = Collections.unmodifiableList(props);
    }

    private PartnerConnection connection;
    private BulkConnection bulkConnection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String username = context.getProperty(USER).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        final String token = context.getProperty(SECURITY_TOKEN).getValue();
        final String endpoint = context.getProperty(AUTH_ENDPOINT).getValue();

        try {
            this.connection = buildConnection(username, password, token, endpoint);
            this.bulkConnection = buildBulkConnection(this.connection);
        } catch (ConnectionException | AsyncApiException e) {
            throw new InitializationException("Cannot create connection to Salesforce API", e);
        }
    }

    protected static PartnerConnection buildConnection(String username, String password, String securityToken, String authEndpoint) throws ConnectionException {
        final ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(username);
        partnerConfig.setPassword(password + securityToken);
        partnerConfig.setAuthEndpoint(authEndpoint);
        /*
        partnerConfig.addMessageHandler(new MessageHandler() {
            @Override
            public void handleRequest(URL url, byte[] bytes) {
                System.out.println("Request Made to " + url.toString());
                System.out.println(new String(bytes));
            }

            @Override
            public void handleResponse(URL url, byte[] bytes) {
                System.out.println("Response received from " + url.toString());
                System.out.println(new String(bytes));
            }
        });
        */
        partnerConfig.setTraceMessage(true);
        return new PartnerConnection(partnerConfig);
    }

    protected static BulkConnection buildBulkConnection(PartnerConnection partnerConnection) throws AsyncApiException {
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConnection.getConfig().getSessionId());
        String soapEndpoint = partnerConnection.getConfig().getServiceEndpoint();
        String apiVersion = "39.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);
        config.setTraceMessage(true);
        return new BulkConnection(config);
    }

    @Override
    public PartnerConnection getConnection() {
        return this.connection;
    }

    @Override
    public BulkConnection getBulkConnection() {
        return this.bulkConnection;
    }

    @OnDisabled
    public void shutdown() {
        try {
            this.connection.logout();
        } catch (ConnectionException e) {
            getLogger().error("Was unable to disconnect cleanly. Unexpected behavior may result.", e);
        } finally {
            this.connection = null;
        }
    }
}
