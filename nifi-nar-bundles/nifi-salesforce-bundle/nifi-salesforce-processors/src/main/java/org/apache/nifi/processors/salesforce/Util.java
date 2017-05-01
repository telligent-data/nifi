package org.apache.nifi.processors.salesforce;

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
}
