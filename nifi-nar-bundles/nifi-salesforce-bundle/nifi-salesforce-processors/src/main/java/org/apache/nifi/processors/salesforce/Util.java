package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XMLizable;
import com.sforce.ws.bind.XmlObject;

import javax.xml.crypto.dsig.XMLObject;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gene on 4/27/17.
 */
public class Util {
    public static Long parseLongOrNull(String longString) {
        try {
            return Long.valueOf(longString);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public class Field {
        private final String name;
        private final String type;
        private String value;

        public Field(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }


    public static void writeXmlObject(XmlObject xmlObject, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartObject();

        final Iterator<XmlObject> xmlObjectIterator = xmlObject.getChildren();
        boolean parsedId = false;
        while (xmlObjectIterator.hasNext()) {
            final XmlObject child = xmlObjectIterator.next();


            // Only parse Id once to maintain JSON spec
            if (child.getName() != null && "Id".equals(child.getName().getLocalPart())) {
                if (parsedId) {
                    continue;
                } else {
                    parsedId = true;
                }
            }

            if (child instanceof SObject) {
                jsonGenerator.writeFieldName(child.getName().getLocalPart());
                writeXmlObject(child, jsonGenerator);
                continue;
            }

            if (child.getXmlType() != null && "QueryResult".equals(child.getXmlType().getLocalPart())) {
                jsonGenerator.writeArrayFieldStart(child.getName().getLocalPart());

                final Iterator<XmlObject> queryResultIterator = child.getChildren();
                while (queryResultIterator.hasNext()) {
                    final XmlObject queryResultChild = queryResultIterator.next();

                    if (queryResultChild.getXmlType() != null && "sObject".equals(queryResultChild.getXmlType().getLocalPart())) {
                        writeXmlObject(queryResultChild, jsonGenerator);
                    }
                }

                jsonGenerator.writeEndArray();
                continue;

            }
            final Object value = child.getValue();
            final String localName = child.getName().getLocalPart();
            if (value == null) {
                jsonGenerator.writeNullField(localName);
            } else {
                    /*
                    jsonGenerator.writeFieldName(localName);
                    parseString((String) value, jsonGenerator);
                    */
                jsonGenerator.writeStringField(localName, value.toString());
            }
        }

        jsonGenerator.writeEndObject();
    }

    public static void getFieldsFromXmlObject(XmlObject xmlObject, String prefix) {
    }

    public static void getFieldsFromSObject(SObject sObject, String prefix) {

        final String fieldName = prefix + sObject.getName().getLocalPart();
        final String fieldType = sObject.getType();


    }

    public static void getFieldsFromSObject(SObject sObject) {
        getFieldsFromSObject(sObject, "");
    }

    public abstract class JacksonXMLObjectMixin {

        @JsonIgnore
        abstract Iterator<XMLizable> getTypedChildren();

        @JsonIgnore
        abstract String getId();

        @JsonIgnore
        abstract String getType();

        @JsonIgnore
        abstract String[] getFieldsToNull();
    }
}
