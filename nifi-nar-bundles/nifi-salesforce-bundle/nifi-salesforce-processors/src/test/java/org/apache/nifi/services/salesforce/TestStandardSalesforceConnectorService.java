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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XMLizable;
import com.sforce.ws.bind.XmlObject;
import org.apache.nifi.processors.salesforce.Util;
import org.apache.nifi.processors.salesforce.WrappedSObject;
import org.apache.nifi.processors.salesforce.XMLObjectWriter;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;

public class TestStandardSalesforceConnectorService {
    private static final String USERNAME = "gene@telligent-data.com";
    private static final String PASSWORD = "Queenmum.1761";
    private static final String SECURITY_TOKEN = "GJCKMZCOtHLGBobIbNundm50J";
    private static final String AUTH_ENDPOINT = "https://login.salesforce.com/services/Soap/u/39.0";

    @Before
    public void init() {

    }

    private abstract class XMLObjectMixin {
        @JsonIgnore
        abstract Iterator<XMLizable> getTypedChildren();

        @JsonIgnore
        abstract String getId();

        @JsonIgnore
        abstract String getType();

        @JsonIgnore
        abstract String[] getFieldsToNull();
    }

    private void writeQueryResultArray(XmlObject xmlObject) {

    }


    @Test
    public void testService() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardSalesforceConnectorService service = new StandardSalesforceConnectorService();
        runner.addControllerService("test-service", service);

        runner.setProperty(service, StandardSalesforceConnectorService.USER, USERNAME);
        runner.setProperty(service, StandardSalesforceConnectorService.PASSWORD, PASSWORD);
        runner.setProperty(service, StandardSalesforceConnectorService.SECURITY_TOKEN, SECURITY_TOKEN);
        runner.setProperty(service, StandardSalesforceConnectorService.AUTH_ENDPOINT, AUTH_ENDPOINT);
        runner.assertValid(service);

        runner.enableControllerService(service);

        assertNotNull(service.getConnection());

        // test code begins here
        final PartnerConnection connection = service.getConnection();


        //final String query = "SELECT ID, AccountNumber, BillingStreet, Phone, Website, AnnualRevenue, Industry, (SELECT FirstName, LastName FROM Account.Contacts) FROM Account";

        final String query = "SELECT ID, AccountNumber, BillingStreet, Phone, Website, AnnualRevenue, Industry, (SELECT FirstName, LastName FROM Account.Contacts) FROM Account LIMIT 2";

        final QueryResult queryResult = connection.query(query);

        //final SObject[] sObjects = connection.retrieve("ID, AccountNumber, BillingStreet, Phone, Website, AnnualRevenue", "Account", new String[] {"0014600000VFCBfAAP"});

        final ObjectMapper mapper = new ObjectMapper();



        for (SObject sObject: queryResult.getRecords()) {
        //for (SObject sObject: sObjects) {
            System.out.println("----------------");

            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            final JsonFactory jsonFactory = new JsonFactory();
            final JsonGenerator jsonGenerator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);


            Util.writeXmlObject(sObject, jsonGenerator);
            jsonGenerator.close();

            System.out.println(new String(outputStream.toByteArray()));

            /*
            System.out.println("name: " + sObject.getName());
            System.out.println("type: " + sObject.getType());
            System.out.println("contacts is : " + sObject.getSObjectField("Contacts"));
            System.out.println("children: ");
            */

            /*
            final Iterator<XmlObject> xmlObjectIterator = sObject.getChildren();
            while (xmlObjectIterator.hasNext()) {
                final XmlObject xmlObject = xmlObjectIterator.next();

                if ("type".equals(xmlObject.getName().getLocalPart())) {
                    continue;
                }



                if (xmlObject instanceof SObject) {
                    System.out.println("instance of SObject!");
                }

                System.out.println("name: " + xmlObject.getName());
                System.out.println("type field: " + xmlObject.getField("type"));

            }

            */

            /*
            final WrappedSObject wrapper = new WrappedSObject(sObject);

            //final String mapped = mapper.writeValueAsString(wrapper);

            mapper.addMixIn(XmlObject.class, XMLObjectMixin.class);
            final String mapped = mapper.writeValueAsString(sObject);

            System.out.println(mapped);

            final SObject unmappedSObject = mapper.readValue(mapped, SObject.class);
            System.out.println(unmappedSObject);

            assertEquals(sObject.getId(), unmappedSObject.getId());
            assertEquals(sObject.getXmlType(), unmappedSObject.getXmlType());
            assertArrayEquals(sObject.getFieldsToNull(), unmappedSObject.getFieldsToNull());

            /*
            System.out.println("Name: " + sObject.getName());
            System.out.println("Type: " + sObject.getType());
            System.out.println("Type (from field): " + sObject.getField("type"));

            if (sObject.hasChildren()) {
                System.out.println("Children: ");
                final Iterator<XmlObject> childrenIterator = sObject.getChildren();

                while(childrenIterator.hasNext()) {
                    final XmlObject child = childrenIterator.next();
                    System.out.println("Name: " + child.getName());
                    System.out.println("Has Children: " + child.hasChildren());
                }
            }

            */


        }




        // test code ends here
        runner.disableControllerService(service);

        assertNull(service.getConnection());

        /*
        final PartnerConnection connection = service.getConnection();

        GregorianCalendar endTime = (GregorianCalendar) connection.getServerTimestamp().getTimestamp();
        GregorianCalendar startTime = (GregorianCalendar) endTime.clone();
        startTime.add(GregorianCalendar.DATE, -5);

        System.out.println("Checking updates as of: " + startTime.getTime().toString());


        GetUpdatedResult result = connection.getUpdated("Account",
                startTime, endTime);

        System.out.println("GetUpdateResult: " + result.getIds().length);


        //final SObject[] sObjects = connection.retrieve("ID, AccountNumber, BillingStreet, Phone, Website, AnnualRevenue, Industry", "Account", result.getIds());

        final String idList = Arrays.stream(result.getIds())
                .map((c) -> "'" + c + "'")
                .collect(Collectors.joining(","));


        //final String query = "SELECT ID, AccountNumber, BillingStreet, Phone, Website, AnnualRevenue, Industry, (SELECT FirstName, LastName FROM Account.Contacts) FROM Account WHERE Account.ID in (" + idList + ")";

        final String objectType = "Contact";

        final String joinedFields = Joiner
                .on(",")
                .join(Arrays.stream(getFields(connection, "Contact"))
                        .map(Field::getName)
                        .collect(Collectors.toList()));
        System.out.println(joinedFields);

        String query = "SELECT ID, FirstName, LastName, Account.Name FROM Contact WHERE Account.ID in (" + idList + ")";

        System.out.println(query);

        final QueryResult queryResult = connection.query(query);

        for (SObject sObject: queryResult.getRecords()) {

        //for (SObject sObject: sObjects) {
            System.out.println("Sobject: " + sObject.toString());
            System.out.println("Account.Name: " + sObject.getField("Account.Name"));
            System.out.println("Name:" + sObject.getField("Name"));
            System.out.println("Account: " + sObject.getField("Account"));
            System.out.println(sObject.getSObjectField("Account"));
            System.out.println(sObject.getSObjectField("Accounts"));
            System.out.println("Children:");
            System.out.println(Joiner.on("\n").join(sObject.getChildren()));

            System.out.println();
            System.out.println("Name/NamespaceURI: " + sObject.getName().getNamespaceURI());
            System.out.println("Name/LocalPart: " + sObject.getName().getLocalPart());
            System.out.println("Name/Prefix: " + sObject.getName().getPrefix());

            System.out.println();

            System.out.println("Value:");
            final TypeMapper mapper = new TypeMapper();
            final OutputStream os = new ByteArrayOutputStream();

            final XmlOutputStream xmlOutputStream = new XmlOutputStream(os, true);
            sObject.write(sObject.getName(), xmlOutputStream, mapper);

            xmlOutputStream.flush();
            os.flush();

            System.out.println(os.toString());
            System.out.println(xmlOutputStream.toString());
        }

        */


        /*
        DescribeGlobalResult results = connection.describeGlobal();
        System.out.println("GLOBAL SOBJECTS");
        for (DescribeGlobalSObjectResult result: results.getSobjects()) {
            System.out.println();
            System.out.println("Name: " + result.getName());
            System.out.println("Label: " + result.getLabel());
            System.out.println("Deletable: " + result.isDeletable());
            System.out.println("Uptadable: " + result.isUpdateable());
        }


        for (DescribeSObjectResult result : connection.describeSObjects(new String[]{"account", "user"})) {
            System.out.println("Name: " + result.getName());
            System.out.println("Label: " + result.getLabel());
            System.out.println("Fields:");
            for (Field field : result.getFields()) {
                System.out.println("Field Name: " + field.getName());
                System.out.println("Field Type: " + field.getType());
            }
        }

        */
    }

}
