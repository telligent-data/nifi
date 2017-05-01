package org.apache.nifi.processors.cometd;

import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Created by gene on 4/21/17.
 */
public class Util {
    public static SslContextFactory contextFactoryFromContextService(SSLContextService sslContextService) {
        final boolean needClientAuth = sslContextService != null && sslContextService.getTrustStoreFile() != null;

        final SslContextFactory contextFactory = new SslContextFactory();
        contextFactory.setNeedClientAuth(needClientAuth);

        if (needClientAuth) {
            contextFactory.setTrustStorePath(sslContextService.getTrustStoreFile());
            contextFactory.setTrustStoreType(sslContextService.getTrustStoreType());
            contextFactory.setTrustStorePassword(sslContextService.getTrustStorePassword());
        }

        final String keystorePath = sslContextService == null ? null : sslContextService.getKeyStoreFile();
        if (keystorePath != null) {
            final String keystorePassword = sslContextService.getKeyStorePassword();
            final String keyStoreType = sslContextService.getKeyStoreType();

            contextFactory.setKeyStorePath(keystorePath);
            contextFactory.setKeyManagerPassword(keystorePassword);
            contextFactory.setKeyStorePassword(keystorePassword);
            contextFactory.setKeyStoreType(keyStoreType);
        }

        return contextFactory;
    }

    public enum Attributes {
        ATTR_CHANNEL_NAME("Test attribute", "Test description");

        private final String attr;
        private final String desc;

        Attributes(String attr, String desc) {
            this.attr = attr;
            this.desc = desc;
        }

        public String getAttr() {
            return attr;
        }

        public String getDesc() {
            return desc;
        }
    }
}
