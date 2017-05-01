package org.apache.nifi.processors.salesforce;

import java.io.OutputStream;

/**
 * Created by gene on 4/27/17.
 */
public interface SalesforceParser {
    SalesforceParser writeField(String name, Object value);
    SalesforceParser setOutputStream(OutputStream outputStream);
}
