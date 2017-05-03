package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.core.JsonGenerator;
import com.sforce.ws.bind.XmlObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

/**
 * Created by gene on 5/2/17.
 */
public class XMLObjectWriter {

    private static void parseString(String s, JsonGenerator jsonGenerator) throws IOException {
        try {
            final Long parsed = Long.valueOf(s);
            jsonGenerator.writeNumber(parsed);
            return;
        } catch (NumberFormatException e) {

        }

        try {
            final BigDecimal parsed = new BigDecimal(s);
            jsonGenerator.writeNumber(parsed);
        } catch (Exception e) {

        }


        try {
            final Integer parsed = Integer.valueOf(s);
            jsonGenerator.writeNumber(parsed);
            return;
        } catch (NumberFormatException e) {

        }


        try {
            final Boolean parsed = Boolean.valueOf(s);
            jsonGenerator.writeBoolean(parsed);
            return;
        } catch (NumberFormatException e) {

        }

        jsonGenerator.writeString(s);
    }

}
