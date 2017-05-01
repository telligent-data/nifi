package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.xml.namespace.QName;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Wrapper around salesforce Sobject for easy serialization/deserialization
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WrappedSObject {

    private class CloneableSObject extends SObject {
        @Override
        protected void cloneFrom(XmlObject source) {
            super.cloneFrom(source);
        }
    }

    private class WrappedQName {
        private final QName qName;

        protected WrappedQName(QName qName) {
            this.qName = qName;
        }

        @JsonCreator
        private WrappedQName(
                @JsonProperty("namespaceURI") String namespaceURI,
                @JsonProperty("localPart") String localPart,
                @JsonProperty("prefix") String prefix) {
            this.qName = new QName(namespaceURI, localPart, prefix);
        }

        @JsonProperty("namespaceURI")
        public String getNamespaceURI() {
            return this.qName.getNamespaceURI();
        }

        @JsonProperty("prefix")
        public String getPrefix() {
            return this.qName.getPrefix();
        }

        @JsonProperty("localPart")
        public String getLocalPart() {
            return this.qName.getLocalPart();
        }
    }

    //private static final String SF_PARTNER_NAMESPACE = "urn:partner.soap.sforce.com";
    //private static final String SF_PARTNER_XMLTYPE_NAMESPACE = "urn:partner.soap.sforce.com";
    private static final Field childrenField;
    private static final Field xmlTypeField;
    static {
        try {
            childrenField = XmlObject.class.getDeclaredField("children");
            childrenField.setAccessible(true);
            xmlTypeField = XmlObject.class.getDeclaredField("xmlType");
            xmlTypeField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }


    private final SObject sObject;

    public WrappedSObject(final SObject sObject) {
        this.sObject = sObject;
    }

    public WrappedSObject(XmlObject xmlObject) {
        final CloneableSObject object = new CloneableSObject();
        object.cloneFrom(xmlObject);
        this.sObject = object;
    }

    @JsonCreator
    public WrappedSObject(
            @JsonProperty("qName") WrappedQName wrappedName,
            @JsonProperty("qXmlType") WrappedQName wrappedXmlType,
            @JsonProperty("value") String value,
            @JsonProperty("children") List<WrappedSObject> children,
            @JsonProperty("id") String id,
            @JsonProperty("type") String type,
            @JsonProperty("fieldsToNull") String[] fieldsToNull) {

        final SObject sObject = new SObject(type);
        sObject.setName(wrappedName.qName);
        sObject.setId(id);
        sObject.setFieldsToNull(fieldsToNull);
        sObject.setValue(value);
        final ArrayList<XmlObject> parsedChildren = new ArrayList<>(
                children.stream().map(v -> v.sObject)
                .collect(Collectors.toList())
        );
        try {
            childrenField.set(sObject, parsedChildren);
            xmlTypeField.set(sObject, wrappedXmlType.qName);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        this.sObject = sObject;
    }


    @JsonProperty("qName")
    public WrappedQName getQName() {
        return (this.sObject.getName() == null) ? null : new WrappedQName(this.sObject.getName());
    }

    @JsonProperty("name")
    public String getName() {
        return (this.sObject.getName() == null) ? null : this.sObject.getName().getLocalPart();
    }

    /*
    @JsonProperty("nameNamespace")
    public String getNameNamespace() {
        return (this.sObject.getName() == null) ? null : this.sObject.getName().getNamespaceURI();
    }
    */

    @JsonProperty("qXmlType")
    public WrappedQName getQXmlType() {
        return (this.sObject.getXmlType() == null) ? null : new WrappedQName(this.sObject.getXmlType());
    }

    @JsonProperty("xmlType")
    public String getXmlType() {
        return (this.sObject.getXmlType() == null) ? null : this.sObject.getXmlType().getLocalPart();
    }

    /*
    @JsonProperty("xmlTypeNamespace")
    public String getXmlTypeNamespace() {
        return (this.sObject.getXmlType() == null) ? null : this.sObject.getXmlType().getNamespaceURI();
    }
    */

    @JsonProperty("value")
    public Object getValue() {
        return this.sObject.getValue();
    }

    @JsonProperty("children")
    public List<WrappedSObject> getChildren() {
        if (!this.sObject.hasChildren()) {
            return null;
        }

        final Iterable<XmlObject> sourceIterable = this.sObject::getChildren;
        return StreamSupport.stream(sourceIterable.spliterator(), false)
                .map(WrappedSObject::new)
                .collect(Collectors.toList());
    }

    @JsonProperty("id")
    public String getId() {
        return this.sObject.getId();
    }

    @JsonProperty("type")
    public String getType() {
        return this.sObject.getType();
    }

    @JsonProperty("fieldsToNull")
    public String[] getFieldsToNull() {
        return this.sObject.getFieldsToNull();
    }
}
