package io.mats3.matsbrokermonitor.htmlgui;

import static io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.ACCESS_CONTROL_ALLOW_ALL;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.plugin.StatisticsBrokerPlugin;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.component.LifeCycle.Listener;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebXmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.CoreConstants;
import io.mats3.MatsFactory;
import io.mats3.MatsFactory.FactoryConfig;
import io.mats3.MatsInitiator.KeepTrace;
import io.mats3.api.intercept.MatsInterceptableMatsFactory;
import io.mats3.impl.jms.JmsMatsFactory;
import io.mats3.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import io.mats3.impl.jms.JmsMatsJmsSessionHandler_Pooling.PoolingKeyInitiator;
import io.mats3.impl.jms.JmsMatsJmsSessionHandler_Pooling.PoolingKeyStageProcessor;
import io.mats3.localinspect.LocalHtmlInspectForMatsFactory;
import io.mats3.localinspect.LocalStatsMatsInterceptor;
import io.mats3.matsbrokermonitor.activemq.ActiveMqMatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.broadcaster.MatsBrokerMonitorBroadcastAndControl;
import io.mats3.matsbrokermonitor.broadcastreceiver.MatsBrokerMonitorBroadcastReceiver;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.BrowseQueueTableAddition;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.ExamineMessageAddition;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.MonitorAddition;
import io.mats3.matsbrokermonitor.htmlgui.SetupTestMatsEndpoints.DataTO;
import io.mats3.matsbrokermonitor.htmlgui.SetupTestMatsEndpoints.StateTO;
import io.mats3.matsbrokermonitor.htmlgui.impl.MatsBrokerMonitorHtmlGuiImpl;
import io.mats3.matsbrokermonitor.jms.JmsMatsBrokerBrowseAndActions;
import io.mats3.serial.MatsSerializer;
import io.mats3.serial.json.MatsSerializerJson;
import io.mats3.test.MatsTestHelp;
import io.mats3.test.broker.MatsTestBroker;
import io.mats3.util.MatsFuturizer;
import io.mats3.util.MatsFuturizer.Reply;

/**
 * @author Endre Stølsvik 2021-12-31 01:50 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerMonitor_TestJettyServer {

    private static final String CONTEXT_ATTRIBUTE_PORTNUMBER = "ServerPortNumber";

    private static final Logger log = LoggerFactory.getLogger(MatsBrokerMonitor_TestJettyServer.class);

    public static final String MATS_DESTINATION_PREFIX = "mats.";

    private static String SERVICE = "MatsTestBrokerMonitor";
    private static String SERVICE_1 = SERVICE + ".FirstSubService";
    private static String SERVICE_2 = SERVICE + ".SecondSubService";
    private static String SERVICE_3 = "Another Group With Spaces.SubService";

    @WebListener
    public static class SCL_Endre implements ServletContextListener {

        private MatsInterceptableMatsFactory _matsFactory;

        @Override
        public void contextInitialized(ServletContextEvent sce) {
            log.info("ServletContextListener.contextInitialized(...): " + sce);
            ServletContext sc = sce.getServletContext();
            log.info("  \\- ServletContext: " + sc);

            // ## Create MatsFactory
            // Get JMS ConnectionFactory from ServletContext
            ConnectionFactory connFactory = (ConnectionFactory) sc.getAttribute(ConnectionFactory.class.getName());
            // MatsSerializer
            MatsSerializer<String> matsSerializer = MatsSerializerJson.create();
            // Create the MatsFactory
            _matsFactory = JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(
                    MatsBrokerMonitor_TestJettyServer.class.getSimpleName(), "*testing*",
                    JmsMatsJmsSessionHandler_Pooling.create(connFactory, PoolingKeyInitiator.INITIATOR,
                            PoolingKeyStageProcessor.STAGE),
                    matsSerializer);
            // Configure the MatsFactory for testing (remember, we're running two instances in same JVM)
            // .. Concurrency of only 2
            FactoryConfig factoryConfig = _matsFactory.getFactoryConfig();
            _matsFactory.holdEndpointsUntilFactoryIsStarted();
            factoryConfig.setConcurrency(SetupTestMatsEndpoints.BASE_CONCURRENCY);
            // .. Use port number of current server as postfix for name of MatsFactory, and of nodename
            Integer portNumber = (Integer) sc.getAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER);
            factoryConfig.setName(getClass().getSimpleName() + "_" + portNumber);
            factoryConfig.setNodename(factoryConfig.getNodename() + "_" + portNumber);
            factoryConfig.setMatsDestinationPrefix(MATS_DESTINATION_PREFIX);
            // Put it in ServletContext, for servlet to get
            sc.setAttribute(JmsMatsFactory.class.getName(), _matsFactory);

            // Install the local stats keeper interceptor
            LocalStatsMatsInterceptor.install(_matsFactory);

            // Make Futurizer:
            MatsFuturizer matsFuturizer = MatsFuturizer.createMatsFuturizer(_matsFactory, SERVICE);
            sc.setAttribute(MatsFuturizer.class.getName(), matsFuturizer);

            // Setup test endpoints
            SetupTestMatsEndpoints.setupMatsTestEndpoints(SERVICE_1, _matsFactory);
            SetupTestMatsEndpoints.setupMatsTestEndpoints(SERVICE_2, _matsFactory);
            SetupTestMatsEndpoints.setupMatsTestEndpoints(SERVICE_3, _matsFactory);

            MatsBrokerMonitorBroadcastReceiver receiver = MatsBrokerMonitorBroadcastReceiver.install(_matsFactory);
            receiver.registerListener(updateEvent -> log.info("Received update via Mats fabric: " + updateEvent));
            sc.setAttribute(MatsBrokerMonitorBroadcastReceiver.class.getName(), receiver);

            _matsFactory.start();

            // :: Create the "local inspect"
            LocalHtmlInspectForMatsFactory inspect = LocalHtmlInspectForMatsFactory.create(_matsFactory);
            sc.setAttribute(LocalHtmlInspectForMatsFactory.class.getName(), inspect);

            // :: Create the ActiveMqMatsBrokerMonitor #1
            MatsBrokerMonitor matsBrokerMonitor1 = ActiveMqMatsBrokerMonitor
                    .createWithDestinationPrefix(connFactory, MATS_DESTINATION_PREFIX, 15_000);
            // Register a dummy listener
            matsBrokerMonitor1.registerListener(destinationUpdateEvent -> {
                log.info("Listener for MBM #1 at TestJettyServer: Got update! " + destinationUpdateEvent);
                destinationUpdateEvent.getEventDestinations().forEach((fqName, matsBrokerDestination) -> log
                        .info(".. event destinations (non-zero): [" + fqName + "] = [" + matsBrokerDestination + "]"));
            });
            MatsBrokerMonitorBroadcastAndControl.install(matsBrokerMonitor1, _matsFactory);
            matsBrokerMonitor1.start();

            // :: Create the ActiveMqMatsBrokerMonitor #2
            MatsBrokerMonitor matsBrokerMonitor2 = ActiveMqMatsBrokerMonitor
                    .createWithDestinationPrefix(connFactory, MATS_DESTINATION_PREFIX, 15_000);
            // Register a dummy listener
            matsBrokerMonitor2.registerListener(destinationUpdateEvent -> {
                log.info("Listener for MBM #2 at TestJettyServer: Got update! " + destinationUpdateEvent);
            });
            MatsBrokerMonitorBroadcastAndControl.install(matsBrokerMonitor2, _matsFactory);
            matsBrokerMonitor2.start();

            // Put it in ServletContext, for shutdown
            sc.setAttribute("matsBrokerMonitor1", matsBrokerMonitor1);

            List<? super MonitorAddition> monitorAdditions = new ArrayList<>();
            monitorAdditions.add(new BrowseQueueTableAddition() {
                @Override
                public String getColumnHeadingHtml(String queueId) {
                    // return null;
                    return "<th>Added table heading: " + queueId.substring(0, 4) + "</th>";
                }

                @Override
                public String convertMessageToHtml(MatsBrokerMessageRepresentation message) {
                    return "<td><div>Added table cell [" + (message.getMatsMessageId() != null
                            ? message.getMatsMessageId().hashCode()
                            : "null") + "]</div></td>";
                }
            });
            monitorAdditions.add(new BrowseQueueTableAddition() {
                @Override
                public String getColumnHeadingHtml(String queueId) {
                    return "<th style='background:blue'>Added: Head</th>";
                }

                @Override
                public String convertMessageToHtml(MatsBrokerMessageRepresentation message) {
                    // return null;
                    return "<td style='background: green'><div>Added: Cell</div></td>";
                }
            });
            monitorAdditions.add(new ExamineMessageAddition() {
                @Override
                public String convertMessageToHtml(MatsBrokerMessageRepresentation message) {
                    return "<h2>Added h2-text!!</h2>";
                }
            });

            monitorAdditions.add(new ExamineMessageAddition() {
                @Override
                public String convertMessageToHtml(MatsBrokerMessageRepresentation message) {
                    return "<input type='button' value='Added button!' class='matsbm_button'></input>";
                }
            });

            // :: Create the JmsMatsBrokerBrowseAndActions #1 (don't need two of this)
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions1 = JmsMatsBrokerBrowseAndActions
                    .createWithDestinationPrefix(connFactory, MATS_DESTINATION_PREFIX);

            // :: Create the MatsBrokerMonitorHtmlGui #1 (don't need two of this)
            MatsBrokerMonitorHtmlGuiImpl matsBrokerMonitorHtmlGui1 = MatsBrokerMonitorHtmlGui
                    .create(matsBrokerMonitor1, matsBrokerBrowseAndActions1, monitorAdditions, matsSerializer);

            // For multiple ActiveMQs:
            // Either: an identifier of sorts, so that the MatsBrokerMonitor knows if it is talked to.
            // Or: .. just use "URL routing" to target the different, i.e. put them on different URL paths.
            // Worth remembering: This is different from a MatsFactory, in that it monitors the _underlying_ broker, not
            // the "local" MatsFactory.

            // Put it in ServletContext, for servlet to get
            sc.setAttribute("matsBrokerMonitorHtmlGui1", matsBrokerMonitorHtmlGui1);
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            log.info("ServletContextListener.contextDestroyed(..): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());
            _matsFactory.stop(5000);
            MatsBrokerMonitor matsBrokerMonitor = (MatsBrokerMonitor) sce.getServletContext()
                    .getAttribute("matsBrokerMonitor1");
            matsBrokerMonitor.close();
        }
    }

    /**
     * Menu.
     */
    @WebServlet("/")
    public static class RootServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
            res.setContentType("text/html; charset=utf-8");
            PrintWriter out = res.getWriter();
            out.println("<h1>Menu</h1>");
            out.println("<a href=\"./matsbrokermonitor\">MatsBrokerMonitor HTML GUI</a><br>");
            out.println("<a href=\"./sendRequest?count=1\">Send Mats requests, count=1</a><br>");
            out.println("<a href=\"./forceUpdate\">Force Update (full) via " + MatsBrokerMonitorBroadcastReceiver.class
                    .getSimpleName() + "</a><br>");
            out.println("<a href=\"./shutdown\">Shutdown</a><br>");
        }
    }

    /**
     * Servlet to shut down this JVM (<code>System.exit(0)</code>).
     */
    @WebServlet("/shutdown")
    public static class ShutdownServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
            res.getWriter().println("Shutting down");

            // Shut down the process
            ForkJoinPool.commonPool().submit(() -> System.exit(0));
        }
    }

    /**
     * Force update via MatsBrokerMonitorBroadcastReceiver
     */
    @WebServlet("/forceUpdate")
    public static class ForceUpdateServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
            String correlationId = "CorrelationId:" + Long.toString(Math.abs(ThreadLocalRandom.current().nextLong()),
                    36);
            res.getWriter().println("Sending ForceUpdate via '" + MatsBrokerMonitorBroadcastReceiver.class
                    .getSimpleName() + "', using CorrelationId [" + correlationId + "]");

            MatsBrokerMonitorBroadcastReceiver mbmbr = (MatsBrokerMonitorBroadcastReceiver) req.getServletContext()
                    .getAttribute(MatsBrokerMonitorBroadcastReceiver.class.getName());

            mbmbr.forceUpdate(correlationId, true);
        }
    }

    /**
     * Send Mats request - notice the "count" URL parameter.
     */
    @WebServlet("/sendRequest")
    public static class SendRequestServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
            res.setContentType("text/plain; charset=utf-8");
            ServletContext servletContext = req.getServletContext();
            MatsFuturizer matsFuturizer = (MatsFuturizer) servletContext.getAttribute(MatsFuturizer.class.getName());
            MatsFactory matsFactory = (MatsFactory) servletContext.getAttribute(JmsMatsFactory.class.getName());

            PrintWriter out = res.getWriter();

            sendAFuturize(out, matsFuturizer, false);
            sendAFuturize(out, matsFuturizer, true);

            takeNap(500);

            out.println("===========\n");

            sendAFuturize(out, matsFuturizer, false);
            sendAFuturize(out, matsFuturizer, true);

            int count = 1;
            String countS = req.getParameter("count");
            if (countS != null) {
                count = Integer.parseInt(countS);
            }
            int countF = count;

            out.println("Sending [" + count + "] requests ..");

            long nanosStart_sendMessages = System.nanoTime();
            StateTO sto = new StateTO(420, 420.024);
            DataTO dto = new DataTO(42, "TheAnswer");
            matsFactory.getDefaultInitiator().initiateUnchecked(
                    (msg) -> {
                        for (int i = 0; i < countF; i++) {
                            msg.traceId(MatsTestHelp.traceId() + "_#" + i)
                                    .keepTrace(KeepTrace.COMPACT)
                            //.nonPersistent()
                                    .from("/sendRequestInitiated")
                                    .to(SERVICE_1 + SetupTestMatsEndpoints.SERVICE_MAIN)
                                    .replyTo(SERVICE_1 + SetupTestMatsEndpoints.TERMINATOR, sto)
                                    .request(dto, new StateTO(1, 2));
                        }
                    });
            double msTaken_sendMessages = (System.nanoTime() - nanosStart_sendMessages) / 1_000_000d;
            out.println(".. [" + count + "] requests sent, took [" + msTaken_sendMessages + "] ms.");
            out.println();

            // :: Send a message to a non-existent endpoint, to have an endpoint with a non-consumed message
            matsFactory.getDefaultInitiator().initiateUnchecked(
                    (msg) -> {
                        msg.traceId(MatsTestHelp.traceId() + "_nonExistentEndpoint")
                                .keepTrace(KeepTrace.FULL)
                                .nonPersistent(60_000)
                                .noAudit()
                                .interactive()
                                .from("/sendRequestInitiated")
                                .to(SERVICE + ".NonExistentService.nonExistentMethod")
                                .send(dto, new StateTO(1, 2));
                    });

            matsFactory.getDefaultInitiator().initiateUnchecked(
                    (msg) -> {
                        msg.traceId(MatsTestHelp.traceId() + "_nonExistentEndpoint")
                                .keepTrace(KeepTrace.FULL)
                                .from("/sendRequestInitiated")
                                .to(SERVICE + ".NonExistentService.nonExistentMethod")
                                .send(dto, new StateTO(1, 2));
                    });

            // :: Send a message to the ActiveMQ Global DLQ
            // Get JMS ConnectionFactory from ServletContext
            ConnectionFactory connFactory = (ConnectionFactory) req.getServletContext()
                    .getAttribute(ConnectionFactory.class.getName());
            try {
                Connection connection = connFactory.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue dlq = session.createQueue("ActiveMQ.DLQ");
                MessageProducer producer = session.createProducer(dlq);
                Message message = session.createMessage();
                producer.send(message);
                connection.close();
            }
            catch (JMSException e) {
                throw new IOException("Couldn't send a message to DLQ.");
            }

            // :: Chill till this has really gotten going.
            takeNap(200);

            Future futureNonInteractive = sendAFuturize(out, matsFuturizer, false);
            Future futureInteractive = sendAFuturize(out, matsFuturizer, true);

            out.flush();

            try {
                futureInteractive.get();
                futureNonInteractive.get();
            }
            catch (Exception e) {
                throw new IOException("Future threw", e);
            }

            takeNap(100);
            out.println("done");
        }

        private static void takeNap(int numMillis) throws IOException{
            try {
                Thread.sleep(numMillis);
            }
            catch (InterruptedException e) {
                throw new IOException("Huh?", e);
            }

        }

        private Future sendAFuturize(PrintWriter out, MatsFuturizer matsFuturizer, boolean interactive) {
            // :: Do an interactive run, which should "jump the line" throughout the system:
            out.println("Doing a MatsFuturizer." + (interactive ? "interactive" : "NON-interactive") + ".");
            long nanosStart_futurize = System.nanoTime();
            CompletableFuture<Reply<DataTO>> futurized = matsFuturizer.futurize("TraceId_" + Math.random(),
                    "/sendRequest_futurized", SERVICE_1 + SetupTestMatsEndpoints.SERVICE_MAIN, 2, TimeUnit.MINUTES,
                    DataTO.class, new DataTO(5, "fem"), msg -> {
                        msg.setTraceProperty(SetupTestMatsEndpoints.DONT_THROW, Boolean.TRUE);
                        msg.keepTrace(KeepTrace.MINIMAL);
                        msg.nonPersistent(); // This works really strange in ActiveMQ Classic. Worrying.
                        if (interactive) {
                            msg.interactive();
                        }
                    });
            double msTaken_futurize = (System.nanoTime() - nanosStart_futurize) / 1_000_000d;
            out.println(".. SEND matsFuturizer." + (interactive ? "interactive" : "NON-interactive")
                    + " took [" + msTaken_futurize + "] ms.");
            out.println();

            futurized.thenAccept(dataTOReply -> {
                double msTaken_futurizedReturn = (System.nanoTime() - nanosStart_futurize) / 1_000_000d;
                out.println(".. #DONE# futurized " + (interactive ? "interactive" : "NON-interactive")
                        + " took [" + msTaken_futurizedReturn + "] ms.");
                out.println();
                out.flush();
            });
            return futurized;
        }
    }

    /**
     * MatsBrokerMonitorServlet
     */
    @WebServlet("/matsbrokermonitor/*")
    public static class MatsBrokerMonitorServlet extends HttpServlet {

        @Override
        protected void doPut(HttpServletRequest req, HttpServletResponse res) throws IOException {
            doJson(req, res);
        }

        @Override
        protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws IOException {
            doJson(req, res);
        }

        protected void doJson(HttpServletRequest req, HttpServletResponse res) throws IOException {
            // :: MatsBrokerMonitorHtmlGUI instance
            MatsBrokerMonitorHtmlGui brokerMonitorHtmlGui = (MatsBrokerMonitorHtmlGui) req.getServletContext()
                    .getAttribute("matsBrokerMonitorHtmlGui1");

            String body = req.getReader().lines().collect(Collectors.joining("\n"));
            res.setContentType("application/json; charset=utf-8");
            PrintWriter out = res.getWriter();
            brokerMonitorHtmlGui.json(out, req.getParameterMap(), body, ACCESS_CONTROL_ALLOW_ALL);
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
            // :: MatsBrokerMonitorHtmlGUI instance
            MatsBrokerMonitorHtmlGui brokerMonitorHtmlGui = (MatsBrokerMonitorHtmlGui) req.getServletContext()
                    .getAttribute("matsBrokerMonitorHtmlGui1");

            res.setContentType("text/html; charset=utf-8");

            PrintWriter out = res.getWriter();

            // :: LocalHtmlInspectForMatsFactory instance
            LocalHtmlInspectForMatsFactory localInspect = (LocalHtmlInspectForMatsFactory) req.getServletContext()
                    .getAttribute(LocalHtmlInspectForMatsFactory.class.getName());

            boolean includeBootstrap3 = req.getParameter("includeBootstrap3") != null;
            boolean includeBootstrap4 = req.getParameter("includeBootstrap4") != null;
            boolean includeBootstrap5 = req.getParameter("includeBootstrap5") != null;

            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("  <head>");
            if (includeBootstrap3) {
                out.println("    <script src=\"https://code.jquery.com/jquery-1.10.1.min.js\"></script>\n");
                out.println("    <link rel=\"stylesheet\" href=\"https://netdna.bootstrapcdn.com/"
                        + "bootstrap/3.0.2/css/bootstrap.min.css\" />\n");
                out.println("    <script src=\"https://netdna.bootstrapcdn.com/"
                        + "bootstrap/3.0.2/js/bootstrap.min.js\"></script>\n");
            }
            if (includeBootstrap4) {
                out.println("    <link rel=\"stylesheet\" href=\"https://cdn.jsdelivr.net/"
                        + "npm/bootstrap@4.6.0/dist/css/bootstrap.min.css\""
                        + " integrity=\"sha384-B0vP5xmATw1+K9KRQjQERJvTumQW0nPEzvF6L/Z6nronJ3oUOFUFpCjEUQouq2+l\""
                        + " crossorigin=\"anonymous\">\n");
                out.println("<script src=\"https://code.jquery.com/jquery-3.5.1.slim.min.js\""
                        + " integrity=\"sha384-DfXdz2htPH0lsSSs5nCTpuj/zy4C+OGpamoFVy38MVBnE+IbbVYUew+OrCXaRkfj\""
                        + " crossorigin=\"anonymous\"></script>\n");
                out.println("<script src=\"https://cdn.jsdelivr.net/"
                        + "npm/bootstrap@4.6.0/dist/js/bootstrap.bundle.min.js\""
                        + " integrity=\"sha384-Piv4xVNRyMGpqkS2by6br4gNJ7DXjqk09RmUpJ8jgGtD7zP9yug3goQfGII0yAns\""
                        + " crossorigin=\"anonymous\"></script>\n");
            }
            if (includeBootstrap5) {
                out.println("<link rel=\"stylesheet\" href=\"https://cdn.jsdelivr.net/"
                        + "npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css\""
                        + " integrity=\"sha384-eOJMYsd53ii+scO/bJGFsiCZc+5NDVN2yr8+0RDqr0Ql0h+rP48ckxlpbzKgwra6\""
                        + " crossorigin=\"anonymous\">\n");
                out.println("<script src=\"https://cdn.jsdelivr.net/"
                        + "npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js\""
                        + " integrity=\"sha384-JEW9xMcG8R+pH31jmWH6WWP0WintQrMb4s7ZOdauHnUtxwoG2vI5DkLtS3qm9Ekf\""
                        + " crossorigin=\"anonymous\"></script>\n");
            }
            out.println("  </head>");
            // NOTE: Setting "margin: 0" just to be able to compare against the Bootstrap-versions without too
            // much "accidental difference" due to the Bootstrap's setting of margin=0.
            out.println("  <body style=\"margin: 0;\">");
            out.println("    <style>");
            brokerMonitorHtmlGui.outputStyleSheet(out); // Include just once, use the first.
            localInspect.getStyleSheet(out); // Include just once, use the first.
            out.println("    </style>");
            out.println("    <script>");
            brokerMonitorHtmlGui.outputJavaScript(out); // Include just once, use the first.
            localInspect.getJavaScript(out);
            out.println("    </script>");
            out.println(" <a href=\"sendRequest?count=1\">Send request, count=1</a> - to initialize Initiator"
                    + " and get some traffic.<br>");
            out.println(" <a href=\".\">Go to menu</a><br><br>");

            // :: Bootstrap3 sets the body's font size to 14px.
            // We scale all the affected rem-using elements back up to check consistency.
            if (includeBootstrap3) {
                out.write("<div style=\"font-size: 114.29%\">\n");
            }
            out.println("<h1>MatsBrokerMonitor HTML embedded GUI</h1>");
            Map<String, String[]> parameterMap = req.getParameterMap();
            brokerMonitorHtmlGui.html(out, parameterMap, ACCESS_CONTROL_ALLOW_ALL);
            if (includeBootstrap3) {
                out.write("</div>\n");
            }

            // Localinspect
            out.write("<h1>LocalHtmlInspectForMatsFactory</h1>\n");
            localInspect.createFactoryReport(out, true, true, true);

            out.println("  </body>");
            out.println("</html>");
        }
    }

    public static Server createServer(ConnectionFactory jmsConnectionFactory, int port) {
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setBaseResource(Resource.newClassPathResource("webapp"));
        // If any problems starting context, then let exception through so that we can exit.
        webAppContext.setThrowUnavailableOnStartupException(true);
        // Store the port number this server shall run under in the ServletContext.
        webAppContext.getServletContext().setAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER, port);
        // Store the JMS ConnectionFactory in the ServletContext
        webAppContext.getServletContext().setAttribute(ConnectionFactory.class.getName(), jmsConnectionFactory);

        // Override the default configurations, stripping down and adding AnnotationConfiguration.
        // https://www.eclipse.org/jetty/documentation/9.4.x/configuring-webapps.html
        // Note: The default resides in WebAppContext.DEFAULT_CONFIGURATION_CLASSES
        webAppContext.setConfigurations(new Configuration[] {
                // new WebInfConfiguration(),
                new WebXmlConfiguration(), // Evidently adds the DefaultServlet, as otherwise no read of "/webapp/"
                // new MetaInfConfiguration(),
                // new FragmentConfiguration(),
                new AnnotationConfiguration() // Adds Servlet annotation processing.
        });

        // :: Get Jetty to Scan project classes too: https://stackoverflow.com/a/26220672/39334
        // Find location for current classes
        URL classesLocation = MatsBrokerMonitor_TestJettyServer.class.getProtectionDomain().getCodeSource()
                .getLocation();
        // Set this location to be scanned.
        webAppContext.getMetaData().setWebInfClassesDirs(Collections.singletonList(Resource.newResource(
                classesLocation)));

        webAppContext.setThrowUnavailableOnStartupException(true);

        // Create the actual Jetty Server
        Server server = new Server(port);

        // Add StatisticsHandler (to enable graceful shutdown), put in the WebApp Context
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(webAppContext);
        server.setHandler(stats);

        // Add a Jetty Lifecycle Listener
        server.addLifeCycleListener(new Listener() {
            @Override
            public void lifeCycleFailure(LifeCycle event, Throwable cause) {
                log.error("====# FAILURE! ===========================================", cause);
            }

            @Override
            public void lifeCycleStarting(LifeCycle event) {
                log.info("====# STARTING! ===========================================");
            }

            @Override
            public void lifeCycleStarted(LifeCycle event) {
                log.info("====# STARTED! ===========================================");
            }

            @Override
            public void lifeCycleStopping(LifeCycle event) {
                log.info("====# STOPPING! ===========================================");
            }

            @Override
            public void lifeCycleStopped(LifeCycle event) {
                log.info("====# STOPPED! ===========================================");
            }
        });

        // :: Graceful shutdown
        server.setStopTimeout(1000);
        server.setStopAtShutdown(true);
        return server;
    }

    public static void main(String... args) throws Exception {
        // Turn off LogBack's absurd SCI
        System.setProperty(CoreConstants.DISABLE_SERVLET_CONTAINER_INITIALIZER_KEY, "true");

        // :: Get ConnectionFactory
        ConnectionFactory jmsConnectionFactory;

        String mode = "Direct_ActiveMQ";

        if ("MatsTestBroker_Artemis".equals(mode)) {
            System.setProperty("mats.test.broker", "artemis");
            MatsTestBroker matsTestBroker = MatsTestBroker.create();
            jmsConnectionFactory = matsTestBroker.getConnectionFactory();
        }
        else if ("MatsTestBroker_ActiveMQ".equals(mode)) {
            System.setProperty("mats.test.broker", "activemq");
            MatsTestBroker matsTestBroker = MatsTestBroker.create();
            jmsConnectionFactory = matsTestBroker.getConnectionFactory();
        }
        else if ("Standard_ActiveMQ".equals(mode)) {
            // Using defaults
            ActiveMQConnectionFactory amqConnectionFactory = new ActiveMQConnectionFactory();

            // Set redelivery policies for testing.
            RedeliveryPolicy redeliveryPolicy = amqConnectionFactory.getRedeliveryPolicy();
            redeliveryPolicy.setMaximumRedeliveries(1);
            redeliveryPolicy.setInitialRedeliveryDelay(100);

            // Chill the prefetch.
            ActiveMQPrefetchPolicy prefetchPolicy = amqConnectionFactory.getPrefetchPolicy();
            prefetchPolicy.setQueuePrefetch(25);

            jmsConnectionFactory = amqConnectionFactory;
        }
        else if ("Direct_ActiveMQ".equals(mode)) {
            BrokerService inVmActiveMqBroker = createInVmActiveMqBroker();
//            jmsConnectionFactory = createActiveMQConnectionFactory("vm://" + inVmActiveMqBroker.getBrokerName()
//                    + "?create=false");
            jmsConnectionFactory = createActiveMQConnectionFactory("tcp://localhost:61616");
        }
        else if ("Direct_Artemis".equals(mode)) {
            String brokerUrl = "vm://" + Math.abs(ThreadLocalRandom.current().nextInt());
            EmbeddedActiveMQ artemisBroker = createArtemisBroker(brokerUrl);
            org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory activeMQConnectionFactory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(brokerUrl);
            //activeMQConnectionFactory.setConsumerWindowSize(128 * 1024 * 1024);
            jmsConnectionFactory = activeMQConnectionFactory;

        }
        else {
            throw new IllegalStateException("No mode");
        }

        Server server = createServer(jmsConnectionFactory, 8080);
        try {
            server.start();
        }
        catch (Throwable t) {
            log.error("server.start() got thrown out!", t);
            System.exit(1);
        }
        log.info("MAIN EXITING!!");
    }

    /// ---

    protected static BrokerService createInVmActiveMqBroker() {
        String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder brokername = new StringBuilder(10);
        brokername.append("MatsTestActiveMQ_");
        for (int i = 0; i < 10; i++)
            brokername.append(ALPHABET.charAt(ThreadLocalRandom.current().nextInt(ALPHABET.length())));

       System.setProperty("org.apache.activemq.kahaDB.files.skipMetadataUpdate", "true");

        log.info("Setting up in-vm ActiveMQ BrokerService '" + brokername + "'.");
        BrokerService broker = new BrokerService();
        try {
            TransportConnector connector = new TransportConnector();
            connector.setUri(new URI("nio://localhost:61616"));
            broker.addConnector(connector);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
        broker.setBrokerName(brokername.toString());
        // :: Disable a bit of stuff for testing:
        // No need for JMX registry; We won't control nor monitor it over JMX in tests
        broker.setUseJmx(false);



        // === NOTE: THE PERSISTENCE IS DIFFERENT FROM MatsTestBroker setup.

        broker.setPersistent(true);
        try {
            // NOTE: Good KahaDB docs from RedHat:
            // https://access.redhat.com/documentation/en-us/red_hat_amq/6.3/html/configuring_broker_persistence/kahadbconfiguration
            KahaDBPersistenceAdapter kahaDBPersistenceAdapter = new KahaDBPersistenceAdapter();
            kahaDBPersistenceAdapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.PERIODIC.name());
            // NOTE: This following value is default 1000, i.e. sync interval of 1 sec.
            // Interestingly, setting it to a much lower value, e.g. 10 or 25, seemingly doesn't severely impact
            // performance of the PERIODIC strategy. Thus, instead of potentially losing a full second's worth of
            // messages if someone literally pulled the power cord of the ActiveMQ instance, you'd lose much less.
            kahaDBPersistenceAdapter.setJournalDiskSyncInterval(25);
            broker.setPersistenceAdapter(kahaDBPersistenceAdapter);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // No need for Advisory Messages; We won't be needing those events in tests.
        broker.setAdvisorySupport(false);
        // No need for shutdown hook; We'll shut it down ourselves in the tests.
        broker.setUseShutdownHook(false);
        // Need scheduler support for redelivery plugin
        broker.setSchedulerSupport(true);

        // ::: Add features that we would want in prod.

        // NOTICE: When using KahaDB, then you most probably want to have "skipMetadataUpdate=true":
        // org.apache.activemq.kahaDB.files.skipMetadataUpdate=true
        // https://access.redhat.com/documentation/en-us/red_hat_amq/6.3/html/tuning_guide/perstuning-kahadb

        // NOTICE: All programmatic config here can be set via standalone broker config file 'conf/activemq.xml'.

        // :: Purge inactive destinations
        // Some usages of Mats ends up using "host-specific topics", e.g. MatsFuturizer from 'util' does this.
        // When using e.g. Kubernetes, hostnames will change when deploying new versions of your services.
        // Therefore, you'll end up with many dead topics. Thus, we want to scavenge these after some inactivity.
        // Both the Broker needs to be configured, as well as DestinationPolicies.
        broker.setSchedulePeriodForDestinationPurge(60_123); // Every 1 minute

        // :: Plugins

        // .. Statistics, since that is what we want people to do in production, and so that an
        // ActiveMQ instance created with this tool can be used by 'matsbrokermonitor'.
        StatisticsBrokerPlugin statisticsBrokerPlugin = new StatisticsBrokerPlugin();
        // .. add the plugins to the BrokerService
        broker.setPlugins(new BrokerPlugin[] { statisticsBrokerPlugin });

        // :: Set Individual DLQ - which you most definitely should do in production.
        // Hear, hear: https://users.activemq.apache.narkive.com/H7400Mn1/policymap-api-is-really-bad
        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("DLQ.");
        individualDeadLetterStrategy.setTopicPrefix("DLQ.");
        // .. Send expired messages to DLQ (Note: true is default)
        individualDeadLetterStrategy.setProcessExpired(true);
        // .. Also DLQ non-persistent messages
        individualDeadLetterStrategy.setProcessNonPersistent(true);
        individualDeadLetterStrategy.setUseQueueForTopicMessages(true); // true is also default
        individualDeadLetterStrategy.setUseQueueForQueueMessages(true); // true is also default.

        // :: Create destination policy entry for QUEUES:
        PolicyEntry allQueuesPolicy = new PolicyEntry();
        allQueuesPolicy.setDestination(new ActiveMQQueue(">")); // all queues
        // .. add the IndividualDeadLetterStrategy
        allQueuesPolicy.setDeadLetterStrategy(individualDeadLetterStrategy);
        // .. we do use prioritization, and this should ensure that priority information is handled in queue, and
        // persisted to store. Store JavaDoc: "A hint to the store to try recover messages according to priority"
        allQueuesPolicy.setPrioritizedMessages(true);
        // Purge inactive Queues. The set of Queues should really be pretty stable. We only want to eventually
        // get rid of queues for Endpoints which are taken out of the codebase.
        allQueuesPolicy.setGcInactiveDestinations(true);
        allQueuesPolicy.setInactiveTimeoutBeforeGC(2 * 24 * 60 * 60 * 1000); // Two full days.

        // :: Create policy entry for TOPICS:
        PolicyEntry allTopicsPolicy = new PolicyEntry();
        allTopicsPolicy.setDestination(new ActiveMQTopic(">")); // all topics
        // .. add the IndividualDeadLetterStrategy, not sure if that is ever relevant for plain Topics.
        allTopicsPolicy.setDeadLetterStrategy(individualDeadLetterStrategy);
        // .. and prioritization, not sure if that is ever relevant for Topics.
        allTopicsPolicy.setPrioritizedMessages(true);
        // Purge inactive Topics. The names of Topics will often end up being host-specific. The utility
        // MatsFuturizer uses such logic. When using Kubernetes, the pods will change name upon redeploy of
        // services. Get rid of the old pretty fast. But we want to see them in destination browsers like
        // MatsBrokerMonitor, so not too fast.
        allTopicsPolicy.setGcInactiveDestinations(true);
        allTopicsPolicy.setInactiveTimeoutBeforeGC(2 * 60 * 60 * 1000); // 2 hours.
        // .. note: Not leveraging the SubscriptionRecoveryPolicy features, as we do not have evidence of this being
        // a problem, and using it does incur a cost wrt. memory and time.
        // Would probably have used a FixedSizedSubscriptionRecoveryPolicy, with setUseSharedBuffer(true).
        // To actually get subscribers to use this, one would have to also set the client side (consumer) to be
        // 'Retroactive Consumer' i.e. new ActiveMQTopic("TEST.Topic?consumer.retroactive=true"); This is not
        // done by the JMS impl of Mats.
        // https://activemq.apache.org/retroactive-consumer

        /*
         * .. chill the prefetch a bit, from Queue:1000 and Topic:Short.MAX_VALUE (!), which is very much when used
         * with Mats and its transactional logic of "consume a message, produce a message, commit", as well as
         * multiple StageProcessors per Stage, on multiple instances/replicas of the services. Lowering this
         * considerably to instead focus on lower memory usage, good distribution, and if one consumer by any chance
         * gets hung, it won't allocate so many of the messages into a "void". This can be set on client side, but
         * if not set there, it gets the defaults from server, AFAIU.
         */
        allQueuesPolicy.setQueuePrefetch(250);
        allTopicsPolicy.setTopicPrefetch(250);

        // .. create the PolicyMap containing the two destination policies
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(allQueuesPolicy.getDestination(), allQueuesPolicy);
        policyMap.put(allTopicsPolicy.getDestination(), allTopicsPolicy);
        // .. set this PolicyMap containing our PolicyEntry on the broker.
        broker.setDestinationPolicy(policyMap);

        // :: Memory: Override available system memory. (The default is 1GB, which feels small for prod.)
        // For production, you'd probably want to tune this, possibly using some calculation based on
        // 'Runtime.getRuntime().maxMemory()', e.g. 'all - some fixed amount for the JVM and ActiveMQ'
        // There's also a 'setPercentOfJvmHeap(..)' instead of 'setLimit(..)'
        broker.getSystemUsage()
                .getMemoryUsage()
                .setLimit(1024L * 1024 * 1024); // 1 GB, which is the default. This is a test-broker!!
        /*
         * .. by default, ActiveMQ shares the memory between producers and consumers. We've encountered issues where
         * the AMQ enters a state where the producers are unable to produce messages while consumers are also unable
         * to consume due to the fact that there is no more memory to be had, effectively deadlocking the system
         * (since a Mats Stage typically reads one message and produces one message). Default behaviour for ActiveMq
         * is that both producers and consumers share the main memory pool of the application. By dividing this into
         * two sections, and providing the producers with a large piece of the pie, it should ensure that the
         * producers will always be able to produce messages while the consumers will always have some memory
         * available to consume messages. Consumers do not use a lot of memory, in fact they use almost nothing.
         * Therefore we've elected to override the default values of AMQ (60/40 P/C) and enforce a 95/5 P/C split
         * instead.
         *
         * Source: ActiveMQ: Understanding Memory Usage
         * http://blog.christianposta.com/activemq/activemq-understanding-memory-usage/
         */
        broker.setSplitSystemUsageForProducersConsumers(true);
        broker.setProducerSystemUsagePortion(95);
        broker.setConsumerSystemUsagePortion(5);

        // :: Start the broker.
        try {
            broker.start();
        }
        catch (Exception e) {
            throw new AssertionError("Could not start ActiveMQ BrokerService '" + brokername + "'.", e);
        }
        return broker;
    }

    protected static ConnectionFactory createActiveMQConnectionFactory(String brokerUrl) {
        log.info("Setting up ActiveMQ ConnectionFactory to brokerUrl: [" + brokerUrl + "].");
        org.apache.activemq.ActiveMQConnectionFactory conFactory = new org.apache.activemq.ActiveMQConnectionFactory(
                brokerUrl);

        // We don't need in-order, so just deliver other messages while waiting for redelivery.
        // NOTE: This is NOT possible to use until https://issues.apache.org/jira/browse/AMQ-8617 is fixed!!
        // conFactory.setNonBlockingRedelivery(true);

        // :: RedeliveryPolicy
        RedeliveryPolicy redeliveryPolicy = conFactory.getRedeliveryPolicy();
        redeliveryPolicy.setInitialRedeliveryDelay(500);
        redeliveryPolicy.setRedeliveryDelay(2000); // This is not in use when using exp. backoff and initial != 0
        redeliveryPolicy.setUseExponentialBackOff(true);
        redeliveryPolicy.setBackOffMultiplier(2);
        redeliveryPolicy.setUseCollisionAvoidance(true);
        redeliveryPolicy.setCollisionAvoidancePercent((short) 15);
        // Only need 1 redelivery for testing, totally ignoring the above. Use 6-10 for production.
        redeliveryPolicy.setMaximumRedeliveries(1);

        // Tune prefetch from client side
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(500); // Overrides whatever set from broker
        prefetchPolicy.setTopicPrefetch(500); // Overrides whatever set from broker
        conFactory.setPrefetchPolicy(prefetchPolicy);

        return conFactory;
    }




    public static EmbeddedActiveMQ createArtemisBroker(String brokerUrl) {
        log.info("Setting up in-vm Artemis embedded broker on URL '" + brokerUrl + "'.");
        org.apache.activemq.artemis.core.config.Configuration config = new ConfigurationImpl();
        try {
            config.setSecurityEnabled(false);
            //config.setPersistenceEnabled(false);
            config.addAcceptorConfiguration("in-vm", brokerUrl);

            // :: Configuring for common DLQs (since we can't get individual DLQs to work!!)
            config.addAddressesSetting("#",
                    new AddressSettings()
                            .setDeadLetterAddress(SimpleString.toSimpleString("DLQ"))
                            .setExpiryAddress(SimpleString.toSimpleString("ExpiryQueue"))
                            .setMaxDeliveryAttempts(3));
            // :: This is just trying to emulate the default config from default broker.xml - inspired by
            // Spring Boot which also got problems with default config in embedded mode being a tad lacking.
            // https://github.com/spring-projects/spring-boot/pull/12680/commits/a252bb52b5106f3fec0d3b2b157507023aa04b2b
            // This effectively makes these address::queue combos "sticky" (i.e. not auto-deleting), AFAIK.
            config.addAddressConfiguration(
                    new CoreAddressConfiguration()
                            .setName("DLQ")
                            .addRoutingType(RoutingType.ANYCAST)
                            .addQueueConfiguration(new QueueConfiguration("DLQ")
                                    .setRoutingType(RoutingType.ANYCAST)));
            config.addAddressConfiguration(
                    new CoreAddressConfiguration()
                            .setName("ExpiryQueue")
                            .addRoutingType(RoutingType.ANYCAST)
                            .addQueueConfiguration(new QueueConfiguration("ExpiryQueue")
                                    .setRoutingType(RoutingType.ANYCAST)));
        }
        catch (Exception e) {
            throw new AssertionError("Can't config the Artemis Configuration.", e);
        }

        EmbeddedActiveMQ server = new EmbeddedActiveMQ();
        server.setConfiguration(config);
        try {
            server.start();
        }
        catch (Exception e) {
            throw new AssertionError("Can't start the Artemis Broker.", e);
        }
        return server;
    }


}
