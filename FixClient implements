import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldConvertError;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.JdbcStoreFactory;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.MessageStoreFactory;
import quickfix.RejectLogon;
import quickfix.SLF4JLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgType;
import quickfix.field.Password;
import quickfix.field.Username;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Throwables.propagate;

public class FixClient implements Application {

    private final SocketInitiator socketInitiator;
    private SessionID sessionID;
    private CountDownLatch latch;
    public final List<Message> receivedMsgs = new ArrayList<>();

    public FixClient(String fixServerConfigFile) throws ConfigError, FieldConvertError {
        this(fixServerConfigFile, false);
    }

    public FixClient(String fixServerConfigFile, boolean jdbcStorage) throws ConfigError, FieldConvertError {
        SessionSettings sessionSettings = FixUtils.loadSessionSettings(fixServerConfigFile);
        MessageStoreFactory messageStoreFactory =jdbcStorage ?new JdbcStoreFactory(sessionSettings): new MemoryStoreFactory() ;
        this.socketInitiator = new SocketInitiator(
                this,
                messageStoreFactory,
                sessionSettings,
                new SLF4JLogFactory(sessionSettings),
                new DefaultMessageFactory()
        );
        latch = new CountDownLatch(1);
    }

    public void start() throws ConfigError {
        socketInitiator.start();
    }

    public void waitUntilInit() throws InterruptedException {
        latch.await();
    }

    public void stop() {
        socketInitiator.stop(true);
    }

    public void sendMessage(Message message) throws InterruptedException {
        latch.await();

        try {
            Session.sendToTarget(message, sessionID);
        } catch (SessionNotFound sessionNotFound) {
            propagate(sessionNotFound);
        }
    }

    public void onCreate(SessionID sessionID) {

    }

    public void onLogon(SessionID sessionID) {
        this.sessionID = sessionID;
        latch.countDown();
    }

    public void onLogout(SessionID sessionID) {
        this.sessionID = null;
    }

    public void toAdmin(Message message, SessionID sessionID) {
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            if (MsgType.LOGON.equals(msgType)) {
                message.setString(Username.FIELD, "FOG");
                message.setString(Password.FIELD, "password");
            }
        } catch (FieldNotFound e) {
            propagate(e);
        }
    }

    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {

    }

    public void toApp(Message message, SessionID sessionID) throws DoNotSend {

    }

    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        receivedMsgs.add(message);
        System.out.println("Received: " + message.getClass().getSimpleName());
    }

    public List<Message> receivedMsgs() {
        return receivedMsgs;
    }
}
