package org.apache.nifi.processors.cometd;

import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gene on 4/21/17.
 */
public class NifiBayeuxParameters {
    private final SSLContextService sslContextService;
    private final String hostname;
    private final String uri;
    private final int port;
    private final Map<String, Object> transportOptions;
    private final boolean isTls;

    private NifiBayeuxParameters(
            String hostname,
            int port,
            String uri,
            SSLContextService sslContextService,
            Map<String, Object> transportOptions) {
        this.sslContextService = sslContextService;
        this.hostname = hostname;
        this.port = port;
        this.uri = uri;
        this.isTls = (sslContextService != null);
        this.transportOptions = transportOptions;
    }

    public SSLContextService getSslContextService() {
        return this.sslContextService;
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }

    public boolean isTls() {
        return this.isTls;
    }

    public String getUri() {
        return this.uri;
    }

    public String getMethod() {
        return (this.isTls) ? "https" : "http";
    }

    public String getEndpoint() {
        return this.getMethod() + "://" + this.hostname + ":" + this.port + this.uri;
    }

    public Map<String, Object> getTransportOptions() {
        return this.transportOptions;
    }

    public static class Builder {
        private SSLContextService sslContextService = null;
        private String hostname = "localhost";
        private String uri = "/";
        private int port = 8080;
        private Map<String, Object> transportOptions;

        public Builder() {

        }

        public Builder setSslContextService(SSLContextService sslContextService) {
            this.sslContextService = sslContextService;
            return this;
        }

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTransportOptions(Map<String, Object> transportOptions) {
            this.transportOptions = transportOptions;
            return this;
        }

        public NifiBayeuxParameters build() {
            transportOptions = (transportOptions == null) ? new HashMap<>() : transportOptions;

            return new NifiBayeuxParameters(
                    this.hostname,
                    this.port,
                    this.uri,
                    this.sslContextService,
                    this.transportOptions
            );
        }
    }
}
