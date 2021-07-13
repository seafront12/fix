import org.quickfixj.jmx.JmxExporter;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.JdbcStoreFactory;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.RejectLogon;
import quickfix.SLF4JLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;
import quickfix.UnsupportedMessageType;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Throwables.propagate;
import static com.hsbc.efx.fog.io.fix.FixUtils.loadSessionSettings;

public class SingleSessionFixAcceptorApplication implements FixAcceptorApplication {
    private final SocketAcceptor acceptor;
    private final FixMsgHandler handler;
    private final ApplicationEvents applicationEvents;
    private ObjectName acceptorObjectName;
    private final JmxExporter exporter;
    private final FixConnectionListener fixConnectionListener;
    private SessionID sessionId;
    private CountDownLatch connectionEstablished;
    private boolean latchUsed = true;

    @Inject
    public SingleSessionFixAcceptorApplication(
            Config config, FixMsgHandler handler,
            ApplicationEvents applicationEvents, FixConnectionListener fixConnectionListener) {
        this.handler = handler;
        this.applicationEvents = applicationEvents;
        this.fixConnectionListener = fixConnectionListener;
        SessionSettings settings = loadSessionSettings(config.fixSettingsFile());
//        MessageStoreFactory storeFactory = new MemoryStoreFactory();
        MessageStoreFactory storeFactory = new JdbcStoreFactory(settings);
        MessageFactory messageFactory = new DefaultMessageFactory();
        LogFactory logFactory = new SLF4JLogFactory(settings);
        connectionEstablished = new CountDownLatch(1);
        try {
            this.exporter = new JmxExporter();
            acceptor = new SocketAcceptor(
                    this,
                    storeFactory,
                    settings,
                    logFactory,
                    messageFactory);
        } catch (JMException | ConfigError e) {
            throw propagate(e);
        }
    }

    public void start() {
        applicationEvents.startingFixServer();
        try {
            acceptor.start();
//            acceptorObjectName = exporter.register(acceptor);
            applicationEvents.fixServerStarted();
            if (latchUsed) {
                connectionEstablished.await();
            }
        } catch (ConfigError | InterruptedException exception) {
            throw propagate(exception);
        }

    }

    public void stop() {
        applicationEvents.stoppingFixServer();
        acceptor.stop();

        // Unregister jmx
        try {
            if (acceptorObjectName != null) {
                exporter.getMBeanServer().unregisterMBean(acceptorObjectName);
            }
            applicationEvents.fixServerStopped();
        } catch (Exception e) {
            applicationEvents.cannotUnregisterMBean("FIX server " + acceptorObjectName, e);
        }
    }

    public void send(Message message) {
        try {
            Session.sendToTarget(message, sessionId);
        } catch (SessionNotFound sessionNotFound) {
            propagate(sessionNotFound);
        }
    }

    public void onCreate(SessionID sessionID) {
    }

    public void onLogon(SessionID sessionID) {
        applicationEvents.fixClientLoggedIn(sessionID.toString());
        this.sessionId = sessionID;
        if (latchUsed) {
            connectionEstablished.countDown();
        }
        fixConnectionListener.onConnect();
    }

    public void onLogout(SessionID sessionID) {
        applicationEvents.fixClientDisconnected(sessionID.toString());
        this.sessionId = null;
        fixConnectionListener.onDisconnect();
    }

    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        handler.handleFixMessage(message);
    }

    public void toAdmin(Message message, SessionID sessionID) {
    }

    public void fromAdmin(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
    }

    public boolean isUp() {
        return sessionId != null;
    }

    public void enableLatch(boolean enabled) {
        latchUsed = enabled;
    }
}
