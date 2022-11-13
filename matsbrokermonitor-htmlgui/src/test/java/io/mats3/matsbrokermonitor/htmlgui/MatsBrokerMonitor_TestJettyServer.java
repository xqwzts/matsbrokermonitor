package io.mats3.matsbrokermonitor.htmlgui;

import static io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.ACCESS_CONTROL_ALLOW_ALL;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
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
                            PoolingKeyStageProcessor.FACTORY),
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
                                    .keepTrace(KeepTrace.FULL)
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
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                throw new IOException("Huh?", e);
            }

            sendAFuturize(out, matsFuturizer, true);
            sendAFuturize(out, matsFuturizer, false);
        }

        private void sendAFuturize(PrintWriter out, MatsFuturizer matsFuturizer, boolean interactive)
                throws IOException {
            // :: Do an interactive run, which should "jump the line" throughout the system:
            out.println("Doing a MatsFuturizer." + (interactive ? "interactive" : "NON-interactive") + ".");
            long nanosStart_futurize = System.nanoTime();
            CompletableFuture<Reply<DataTO>> futurized;
            futurized = matsFuturizer.futurize("TraceId_" + Math.random(),
                    "/sendRequest_futurized", SERVICE_1 + SetupTestMatsEndpoints.SERVICE_MAIN, 2, TimeUnit.MINUTES,
                    DataTO.class, new DataTO(5, "fem"), msg -> {
                        msg.setTraceProperty(SetupTestMatsEndpoints.DONT_THROW, Boolean.TRUE);
                        if (interactive) {
                            msg.interactive();
                        }
                    });
            double msTaken_futurize = (System.nanoTime() - nanosStart_futurize) / 1_000_000d;
            out.println(".. matsFuturizer." + (interactive ? "interactive" : "NON-interactive")
                    + " took [" + msTaken_futurize + "] ms.");
            try {
                Reply<DataTO> dataTOReply = futurized.get(50, TimeUnit.SECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new IOException("WTF?", e);
            }
            double msTaken_futurizedReturn = (System.nanoTime() - nanosStart_futurize) / 1_000_000d;
            out.println(".. futurized.get() took [" + msTaken_futurizedReturn + "] ms.");
            out.println();
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
        if (true) {
            // BrokerService inVmActiveMqBroker = createInVmActiveMqBroker();
            // jmsConnectionFactory = new ActiveMQConnectionFactory("vm://" + inVmActiveMqBroker.getBrokerName()
            // + "?create=false");
            MatsTestBroker matsTestBroker = MatsTestBroker.create();
            jmsConnectionFactory = matsTestBroker.getConnectionFactory();
        }
        else {
            // Using defaults
            ActiveMQConnectionFactory amqConnectionFactory = new ActiveMQConnectionFactory();

            /*
             * Set redelivery policies for testing.
             */
            RedeliveryPolicy redeliveryPolicy = amqConnectionFactory.getRedeliveryPolicy();
            redeliveryPolicy.setMaximumRedeliveries(1);
            redeliveryPolicy.setInitialRedeliveryDelay(100);

            // Chill the prefetch.
            ActiveMQPrefetchPolicy prefetchPolicy = amqConnectionFactory.getPrefetchPolicy();
            prefetchPolicy.setQueuePrefetch(25);

            jmsConnectionFactory = amqConnectionFactory;
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
}
