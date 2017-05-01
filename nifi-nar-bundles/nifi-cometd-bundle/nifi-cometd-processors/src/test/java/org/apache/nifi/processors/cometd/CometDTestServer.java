package org.apache.nifi.processors.cometd;

import org.cometd.bayeux.MarkedReference;
import org.cometd.bayeux.server.BayeuxServer;
import org.cometd.bayeux.server.ConfigurableServerChannel;
import org.cometd.bayeux.server.ServerChannel;
import org.cometd.bayeux.server.ServerMessage;
import org.cometd.bayeux.server.ServerSession;
import org.cometd.server.AbstractServerTransport;
import org.cometd.server.AbstractService;
import org.cometd.server.BayeuxServerImpl;
import org.cometd.server.CometDServlet;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Test server for controlling CometD communications.
 */
public class CometDTestServer {
    private static final Logger LOG = LoggerFactory.getLogger(CometDTestServer.class);
    public static final String CONTEXT_PATH = "/cometd";
    public static final long META_CONNECT_TIMEOUT = 20000;
    public static final long MAX_NETWORK_DELAY = 5000;

    private final int selectors = Runtime.getRuntime().availableProcessors();
    private final BayeuxServerImpl bayeuxServer = new BayeuxServerImpl();
    private final QueuedThreadPool threadPool = new QueuedThreadPool();
    private final Server server = new Server(threadPool);
    private final boolean tls;
    private final int port;


    private final BayeuxServer.ChannelListener channelListener = new BayeuxServer.ChannelListener() {
        @Override
        public void channelAdded(ServerChannel serverChannel) {
            LOG.info("Channel " + serverChannel + " added.");
        }

        @Override
        public void channelRemoved(String s) {
            LOG.info("Channel " + s + " removed.");
        }

        @Override
        public void configureChannel(ConfigurableServerChannel configurableServerChannel) {
            LOG.info("Channel " + configurableServerChannel + " configured.");
        }
    };

    private final BayeuxServer.SessionListener sessionListener = new BayeuxServer.SessionListener() {
        @Override
        public void sessionAdded(ServerSession serverSession, ServerMessage serverMessage) {
            LOG.info("Session " + serverSession + " added with handshake message " + serverMessage);
        }

        @Override
        public void sessionRemoved(ServerSession serverSession, boolean b) {
            LOG.info("Session " + serverSession + " removed; due to timeout: " + b);
        }
    };

    private final BayeuxServer.SubscriptionListener subscriptionListener = new BayeuxServer.SubscriptionListener() {
        @Override
        public void subscribed(ServerSession serverSession, ServerChannel serverChannel, ServerMessage serverMessage) {
            LOG.info("Session " + serverChannel + " subscribed to channel " + serverChannel + " with message " + serverMessage);
        }

        @Override
        public void unsubscribed(ServerSession serverSession, ServerChannel serverChannel, ServerMessage serverMessage) {
            LOG.info("Session " + serverChannel + " unsubscribed to channel " + serverChannel + " with message " + serverMessage);
        }
    };

    public CometDTestServer(int port, boolean tls) {
        this.port = port;
        this.tls = tls;
    }



    private ConnectionFactory[] getConnectionFactories() {
        SslContextFactory sslContextFactory = null;
        try {
            if (tls) {
                Path keyStoreFile = Paths.get("src/test/resources/keystore.jks");
                if (!Files.exists(keyStoreFile)) {
                    throw new FileNotFoundException(keyStoreFile.toString());
                }
                sslContextFactory = new SslContextFactory();
                sslContextFactory.setKeyStorePath(keyStoreFile.toString());
                sslContextFactory.setKeyStorePassword("localtest");
                sslContextFactory.setKeyManagerPassword("localtest");
            }
        } catch (FileNotFoundException e) {
            sslContextFactory = null;
        }

        HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.setDelayDispatchUntilContent(true);
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfiguration);
        return AbstractConnectionFactory.getFactories(sslContextFactory, httpConnectionFactory);
    }

    public void run() throws Exception {
        ServerConnector connector = new ServerConnector(server, 1, selectors, getConnectionFactories());
        connector.setIdleTimeout(50 * MAX_NETWORK_DELAY);
        connector.setPort(port);
        this.server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(server, CONTEXT_PATH, ServletContextHandler.SESSIONS);
        context.setAttribute(BayeuxServer.ATTRIBUTE, bayeuxServer);
        context.setInitParameter(ServletContextHandler.MANAGED_ATTRIBUTES, BayeuxServer.ATTRIBUTE);
        //context.addServlet(DefaultServlet.class, "/");

        CometDServlet servlet = new CometDServlet();
        ServletHolder servletHolder = new ServletHolder("cometd", servlet);
        servletHolder.setInitOrder(1);
        context.addServlet(servletHolder, "/*");

        bayeuxServer.setOption(AbstractServerTransport.TIMEOUT_OPTION, String.valueOf(META_CONNECT_TIMEOUT));
        bayeuxServer.setOption(ServletContext.class.getName(), context.getServletContext());
        bayeuxServer.addListener(this.channelListener);
        bayeuxServer.addListener(this.sessionListener);
        bayeuxServer.addListener(this.subscriptionListener);

        this.server.start();
    }

    public boolean isRunning() {
        return this.server.isRunning();
    }

    public boolean isTls() {
        return this.tls;
    }

    public void stop() throws Exception {
        this.server.stop();
    }

    public ServerChannel createPersistentChannel(String channelName) {
        final MarkedReference<ServerChannel> ref = this.bayeuxServer.createChannelIfAbsent(channelName, new ServerChannel.Initializer() {
            public void configureChannel(ConfigurableServerChannel channel) {
                channel.setPersistent(true);
            }
        });

        return ref.getReference();
    }

    public void removePersistentChannel(String channelName) {
        ServerChannel channel = bayeuxServer.getChannel(channelName);
        if (channel == null) {
            return;
        }

        channel.setPersistent(false);
    }

}
