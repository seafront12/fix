import org.junit.Before;
import org.junit.Test;
import quickfix.ConfigError;
import quickfix.FieldConvertError;
import quickfix.StringField;
import quickfix.field.Account;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.Currency;
import quickfix.field.ExecID;
import quickfix.field.ExecInst;
import quickfix.field.ExecType;
import quickfix.field.HandlInst;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.PartyID;
import quickfix.field.Product;
import quickfix.field.SecurityType;
import quickfix.field.SettlType;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TransactTime;
import quickfix.fix50.ExecutionReport;
import quickfix.fix50.NewOrderSingle;
import quickfix.fix50.OrderCancelRequest;
import quickfix.fix50.component.Parties;
import scaffolding.com.hsbc.efx.fog.io.FixClient;
import scaffolding.com.hsbc.efx.fog.poller.Poller;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AcceptorFailOverTest {

    private final Date someDateTime = Date.from(LocalDateTime.of(2016, 1, 2, 5, 34, 54).toInstant(ZoneOffset.UTC));
    private SingleSessionFixAcceptorApplication acceptor1;
    private SingleSessionFixAcceptorApplication acceptor2;
    private Poller poller = new Poller(10000, 100);
    private Poller poller2 = new Poller(10000, 100);
    private FixClient fixClient;

    @Before
    public void setUp() throws ConfigError, InterruptedException, FieldConvertError {
        Config config1 = mock(Config.class, "1");
        Config config2 = mock(Config.class, "2");
        when(config1.fixSettingsFile()).thenReturn("acceptor1.ini");
        when(config2.fixSettingsFile()).thenReturn("acceptor2.ini");
        acceptor1 = new SingleSessionFixAcceptorApplication(config1, message -> {
            if(message instanceof NewOrderSingle) {
                ExecutionReport execRpt = new ExecutionReport(new OrderID("1"), new ExecID("1-1"), new ExecType(ExecType.NEW), new OrdStatus(OrdStatus.NEW), new Side(Side.BUY), new LeavesQty(0d), new CumQty(0d));
                execRpt.setField(new ClOrdID("some_id"));
                acceptor1.send(execRpt);
                System.out.println("+++++++++++++sending Exec Rpt+++++++++++++++++");
            }
        }, mock(ApplicationEvents.class, "1"), new FixConnectionListener() {
            public void onConnect() {

            }

            public void onDisconnect() {

            }
        });

        acceptor2 = new SingleSessionFixAcceptorApplication(config2, message -> {
            if(message instanceof OrderCancelRequest){
                ExecutionReport execRpt = new ExecutionReport(new OrderID("1"), new ExecID("1-2"), new ExecType(ExecType.CANCELED), new OrdStatus(OrdStatus.CANCELED), new Side(Side.BUY), new LeavesQty(0d), new CumQty(0d));
                acceptor2.send(execRpt);
            }
        }, mock(ApplicationEvents.class, "2"), new FixConnectionListener() {
            public void onConnect() {

            }

            public void onDisconnect() {

            }
        });

        acceptor1.enableLatch(false);
        acceptor2.enableLatch(false);
        acceptor1.start();


        //Have to start the client first, as the fixIO service is blocked until a client connects
        fixClient = new FixClient("sender-failover.ini", true);
        fixClient.start();

    }

    @Test
    public void name() throws InterruptedException {
        NewOrderSingle newOrderSingle = new NewOrderSingle();
        newOrderSingle.set(new ClOrdID("some_id"));

        Parties.NoPartyIDs noPartyIDs = new Parties.NoPartyIDs();
        noPartyIDs.set(new PartyID("SomeClient"));
        newOrderSingle.addGroup(noPartyIDs);
        newOrderSingle.set(new Account("SomeAccount"));
        newOrderSingle.set(new SettlType("SPT"));
        newOrderSingle.set(new ExecInst("G"));
        newOrderSingle.set(new HandlInst('1'));
        newOrderSingle.set(new Symbol("EUR/USD"));
        newOrderSingle.set(new Product(4));
        newOrderSingle.set(new SecurityType("FXSPOT"));
        newOrderSingle.set(new Side('1'));
        newOrderSingle.set(new TransactTime(someDateTime));
        newOrderSingle.set(new OrderQty(2_000_000));
        newOrderSingle.set(new OrdType('Z'));
        newOrderSingle.set(new Currency("EUR"));
        newOrderSingle.setField(new StringField(9103, "WM"));
        newOrderSingle.setField(new StringField(9105, "16:00"));
        newOrderSingle.setField(new StringField(9106, "20160512"));
        newOrderSingle.set(new Text("comment"));

        fixClient.sendMessage(newOrderSingle);

        poller.assertEventually(() -> {
            assertThat(fixClient.receivedMsgs(), hasSize(1));
            assertTrue(fixClient.receivedMsgs().get(0) instanceof ExecutionReport);


            acceptor1.stop();
            System.out.println("Acceptor1 stopped");
            acceptor2.start();
            System.out.println("Acceptor2 started");

            Thread.sleep(6000);
            System.out.println("Sending cancel request");
            OrderCancelRequest cancelRequest = new OrderCancelRequest(new OrigClOrdID("some_id"), new ClOrdID("some_id-cxl"), new Side(Side.BUY), new TransactTime(new Date()));
            cancelRequest.set(new OrderQty(2_000_000));
            fixClient.sendMessage(cancelRequest);

            poller2.assertEventually(()->{
                assertThat(fixClient.receivedMsgs, hasSize(2));
                assertTrue(fixClient.receivedMsgs().get(1) instanceof ExecutionReport);
            });

        });
    }
}
