package io.mats3.matsbrokermonitor.htmlgui;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mats3.matsbrokermonitor.api.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.BrokerIOException;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageIterable;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfo;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointGroupBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsStageBrokerRepresentation;
import io.mats3.serial.MatsSerializer;
import io.mats3.serial.MatsSerializer.DeserializedMatsTrace;
import io.mats3.serial.MatsTrace;
import io.mats3.serial.MatsTrace.Call;
import io.mats3.serial.MatsTrace.StackState;

/**
 * Instantiate a <b>singleton</b> of this class, supplying it a {@link MatsBrokerMonitor} instance, <b>to which this
 * instance will register as listener</b>. Again: You are NOT supposed to instantiate an instance of this class per
 * rendering, as this instance is "active" and will register as listener and may instantiate threads.
 *
 * @author Endre Stølsvik 2021-12-17 10:22 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerMonitorHtmlGuiImpl implements MatsBrokerMonitorHtmlGui {
    private final MatsBrokerMonitor _matsBrokerMonitor;
    private final MatsBrokerBrowseAndActions _matsBrokerBrowseAndActions;
    private final MatsSerializer<?> _matsSerializer;

    public static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions,
            MatsSerializer<?> matsSerializer) {
        return new MatsBrokerMonitorHtmlGuiImpl(matsBrokerMonitor, matsBrokerBrowseAndActions, matsSerializer);
    }

    public static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions) {
        return create(matsBrokerMonitor, matsBrokerBrowseAndActions, null);
    }

    MatsBrokerMonitorHtmlGuiImpl(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions,
            MatsSerializer<?> matsSerializer) {
        _matsBrokerMonitor = matsBrokerMonitor;
        _matsBrokerBrowseAndActions = matsBrokerBrowseAndActions;
        _matsSerializer = matsSerializer;
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getStyleSheet(Appendable out) throws IOException {
        // Regular fonts: Using the "Native font stack" of Bootstrap 5
        String font_regular = ""
                // Cross-platform generic font family (default user interface font)
                // + "system-ui,"
                // Safari for macOS and iOS (San Francisco)
                + " -apple-system,"
                // Chrome < 56 for macOS (San Francisco)
                + " BlinkMacSystemFont,"
                // Windows
                + " \"Segoe UI\","
                // Android
                + " Roboto,"
                // Basic web fallback
                + " \"Helvetica Neue\", Arial,"
                // Linux
                + " \"Noto Sans\","
                + " \"Liberation Sans\","
                // Sans serif fallback
                + " sans-serif,"
                // Emoji fonts
                + " \"Apple Color Emoji\", \"Segoe UI Emoji\", \"Segoe UI Symbol\", \"Noto Color Emoji\"";

        // Monospaced fonts: Using the "Native font stack" of Bootstrap 5
        String font_mono = "SFMono-Regular,Menlo,Monaco,Consolas,\"Liberation Mono\",\"Courier New\",monospace;";

        out.append(".mats_report {\n"
                + "  font-family: " + font_regular + ";\n"
                + "  font-weight: 400;\n"
                + "  font-size: 80%;\n"
                + "  line-height: 1.35;\n"
                + "  color: #212529;\n"
                + "}\n");

        // :: Fonts and headings
        out.append(".mats_report h1, .mats_report h2, .mats_report h3, .mats_report h4 {\n"
                // Have to re-set font here, otherwise Bootstrap 3 takes over.
                + "  font-family: " + font_regular + ";\n"
                + "  display: inline-block;\n"
                + "  line-height: 1.2;\n"
                + "  margin: 0.15em 0 0.3em 0;\n"
                + "}\n");
        out.append(".mats_report h1 {\n"
                + "  font-size: 1.8em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_report h2 {\n"
                + "  font-size: 1.5em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_report h3 {\n"
                + "  font-size: 1.4em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_report h4 {\n"
                + "  font-size: 1.3em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_heading {\n"
                + "  display: block;\n"
                + "  margin: 0em 0em 0.5em 0em;\n"
                + "}\n");

        // .. integers in timings (i.e. ms >= 500)
        // NOTE! The point of this class is NOT denote "high timings", but to denote that there are no
        // decimals, to visually make it easier to compare a number '1 235' with '1.235'.
        out.append(".mats_integer {\n"
                + "  color: #b02a37;\n"
                + "}\n");

        // :: TEXT: EndpointId
        out.append(".mats_endpoint_id {\n"
                + "}\n");

        // :: TEXT: Age
        out.append(".mats_age {\n"
                + "  display: inline;\n"
                + "  font-size: 75%;\n"
                + "  font-style: italic;\n"
                + "  margin: 0 0 0 0.2em;\n"
                + "}\n");

        // TEXT: Ids
        out.append(".mats_epid, .mats_queue, .mats_topic {\n"
                + "  display: inline-block;\n"
                + "  font-family: " + font_mono + ";\n"
                + "  font-size: 1em;\n"
                + "  margin: 0 0 0 0.5em;\n"
                + "  padding: 2px 4px 1px 4px;\n"
                + "  border-radius: 3px;\n"
                + "}\n");

        // TEXT: EndpointIds
        out.append(".mats_epid {\n"
                + "  margin: 0 0 0 1.5em;\n"
                + "  font-size: 1.1em;\n"
                + "  background-color: rgba(0, 0, 255, 0.04);\n"
                + "}\n");

        // TEXT: Queue
        out.append(".mats_queue {\n"
                + "  font-size: 0.9em;\n"
                + "  font-style: italic;\n"
                + "  background-color: rgba(0, 255, 0, 0.15);\n"
                + "}\n");
        // TEXT: Topic
        out.append(".mats_topic {\n"
                + "  font-size: 0.9em;\n"
                + "  font-style: italic;\n"
                + "  background-color: rgba(0, 255, 255, 0.2);\n"
                + "}\n");

        // :: Destinations-button-links: Incoming and DLQ
        out.append(".mats_report .incoming_zero,.incoming,.dlq {\n"
                + "  text-decoration: none;\n"
                + "  background-color: initial;\n"
                + "  background-image: linear-gradient(#00D775, #00BD68);\n"
                + "  box-shadow: rgba(0, 0, 0, .3) 0 5px 15px;\n"
                + "  border-radius: 6px;\n"
                + "  box-shadow: rgba(0, 0, 0, 0.1) 0 2px 4px;\n"
                + "  color: #FFFFFF;\n"
                + "  display: inline-block;\n"
                + "  margin: 0 0 0 0.4em;\n" // spacing between "S1" and this box
                + "  padding: 0px 10px;\n" // Extra width to sides of queue count
                + "  text-align: center;\n"
                + "  border: 0;\n"
                + "  transition: box-shadow 0.4s;\n"
                + "}\n"
                + "\n"
                + ".mats_report .incoming_zero:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(13, 112, 234, 0.9) 0 3px 8px;\n"
                + "}");

        // :: ... button Incoming_zero
        out.append(".mats_report .incoming_zero {\n"
                + "  background-image: linear-gradient(#00D775, #00BD68);\n"
                + "}\n"
                + "\n"
                + ".mats_report .incoming_zero:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(13, 112, 234, 0.9) 0 3px 8px;\n"
                + "}");

        // :: ... button Incoming
        out.append(".mats_report .incoming {\n"
                + "  background-image: linear-gradient(#0dccea, #0d70ea);\n"
                + "}\n"
                + "\n"
                + ".mats_report .incoming:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(13, 112, 234, 0.9) 0 3px 8px;\n"
                + "}");

        // :: ... button DLQ
        out.append(".mats_report .dlq {\n"
                + "  background-image: linear-gradient(#FF7E31, #E62C03);\n"
                + "}\n"
                + "\n"
                + ".mats_report .dlq:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(253, 76, 0, 0.9) 0 3px 8px;\n"
                + "}");

        // :: The different parts of the report
        out.append(".mats_info {\n"
                + "  margin: 0em 0em 0em 0.5em;\n"
                + "}\n");

        // Boxes:
        out.append(".mats_broker, .mats_endpoint_group {\n"
                + "  border-radius: 3px;\n"
                + "  box-shadow: 2px 2px 2px 0px rgba(0,0,0,0.37);\n"
                + "  border: thin solid #a0a0a0;\n"
                + "  margin: 0.5em 0.5em 0.7em 0.5em;\n"
                + "  padding: 0.1em 0.5em 0.5em 0.5em;\n"
                + "}\n");
        out.append(".mats_broker {\n"
                + "  background: #f0f0f0;\n"
                + "}\n");
        out.append(".mats_endpoint_group {\n"
                + "  background: #e8f0e8;\n"
                + "  margin: 0.5em 0.5em 1em 0.5em;\n"
                + "}\n");

        // :: Stage small-box
        out.append(".mats_stage {\n"
                + "  background: #f0f0f0;\n"
                + "  display: inline-block;\n"
                + "  border-radius: 3px;\n"
                + "  box-shadow: 2px 2px 2px 0px rgba(0,0,0,0.37);\n"
                + "  border: thin solid #a0a0a0;\n"
                + "  margin: 0.2em 0 0.2em 0.5em;\n"
                + "  padding: 0.2em 0.2em 0.2em 0.2em;\n"
                + "}\n");

        // :: TABLE: Queue browse messages
        out.append(".mats_browse_messages {\n"
                + "    width: 100%;\n"
                + "    border-collapse: collapse;\n"
                + "    font-size: 0.9em;\n"
                + "    box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);\n"
                + "}");
        out.append(".mats_browse_messages thead th {\n"
                + "    position: sticky;\n"
                + "    top: 0;\n"
                + "    background-color: #009879;\n"
                + "    color: #ffffff;\n"
                + "}\n");
        out.append(".mats_browse_messages th, .mats_browse_messages td {\n"
                + "    padding: 0.5em 0.5em 0.5em 0.5em;\n"
                + "}\n");
        out.append(".mats_browse_messages .bool {\n"
                + "    text-align: center;\n"
                + "}\n");
        out.append(".mats_browse_messages tbody tr {\n"
                + "    border-bottom: thin solid #dddddd;\n"
                + "}\n"
                + ".mats_browse_messages tbody tr:nth-of-type(even) {\n"
                + "    background-color: #f3f3f3;\n"
                + "}\n"
                + ".mats_browse_messages tbody tr:hover {\n"
                + "    background-color: #ffff99;\n"
                + "}\n"
                + ".mats_browse_messages tbody tr:last-of-type {\n"
                + "    border-bottom: thick solid #009879;\n"
                + "}\n");

        // :: TABLE: Message: Properties

        out.append(".mats_flow_and_message {\n"
                + "    border-collapse: collapse;\n"
                + "    font-size: 0.9em;\n"
                + "    box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);\n"
                + "}");
        out.append(".mats_flow_and_message td {\n"
                + "    vertical-align: top;"
                + "    padding: 1em;\n"
                + "}\n");
        out.append(".mats_flow_and_message tbody tr {\n"
                + "    border-bottom: thin solid #dddddd;\n"
                + "}\n");

        out.append(".mats_message_props {\n"
                // + " width: 100%;\n"
                + "    border-collapse: collapse;\n"
                + "    font-size: 0.9em;\n"
                + "    box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);\n"
                + "}");
        out.append(".mats_message_props thead th {\n"
                + "    position: sticky;\n"
                + "    top: 0;\n"
                + "    background-color: #009879;\n"
                + "    color: #ffffff;\n"
                + "}\n");
        out.append(".mats_message_props th, .mats_message_props td {\n"
                + "    padding: 0.5em 0.5em 0.5em 0.5em;\n"
                + "}\n");
        out.append(".mats_message_props .bool {\n"
                + "    text-align: center;\n"
                + "}\n");
        out.append(".mats_message_props tbody tr {\n"
                + "    border-bottom: thin solid #dddddd;\n"
                + "}\n"
                + ".mats_message_props tbody tr:nth-of-type(even) {\n"
                + "    background-color: #f3f3f3;\n"
                + "}\n"
                + ".mats_message_props tbody tr:hover {\n"
                + "    background-color: #ffff99;\n"
                + "}\n"
                + ".mats_message_props tbody tr:last-of-type {\n"
                + "    border-bottom: thick solid #009879;\n"
                + "}\n");

    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getJavaScript(Appendable out) throws IOException {
        out.append("");
    }

    public void main(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        if (requestParameters.containsKey("browse")) {
            String destinationId = getDestinationId(requestParameters, ac);
            // ----- Passed Access Control for browse of specific destination, render it.
            browse(out, destinationId, ac);
            return;
        }

        else if (requestParameters.containsKey("examineMessage")) {
            String destinationId = getDestinationId(requestParameters, ac);

            String[] messageSystemIds = requestParameters.get("messageSystemId");
            if (messageSystemIds == null) {
                throw new IllegalArgumentException("Missing messageSystemIds");
            }
            if (messageSystemIds.length > 1) {
                throw new IllegalArgumentException(">1 messageSystemId args");
            }
            String messageSystemId = messageSystemIds[0];
            examineMessage(out, destinationId, messageSystemId);
            return;
        }

        // E-> No special argument, assume overview
        boolean overview = ac.overview();
        if (!overview) {
            throw new AccessDeniedException("Not allowed to see broker overview!");
        }
        // ----- Passed Access Control for overview, render it.
        overview(out, requestParameters, ac);
    }

    private String getDestinationId(Map<String, String[]> requestParameters, AccessControl ac) {
        String[] destinationIds = requestParameters.get("destinationId");
        if (destinationIds == null) {
            throw new IllegalArgumentException("Missing destinationId");
        }
        if (destinationIds.length > 1) {
            throw new IllegalArgumentException(">1 browse args");
        }
        String destinationId = destinationIds[0];
        if (!(destinationId.startsWith("queue:") || destinationId.startsWith("topic:"))) {
            throw new IllegalArgumentException("the browse arg should start with queue: or topic:");
        }
        boolean browseAllowed = ac.browse(destinationId);
        if (!browseAllowed) {
            throw new AccessDeniedException("Not allowed to browse destination!");
        }
        return destinationId;
    }

    @Override
    public void json(Appendable out, Map<String, String[]> requestParameters,
            AccessControl ac) throws IOException, AccessDeniedException {

    }

    @Override
    public void html(Appendable out, Map<String, String[]> requestParameters,
            AccessControl ac) throws IOException, AccessDeniedException {

    }

    protected void browse(Appendable out, String destinationId, AccessControl ac)
            throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.append("<div class=\"mats_report mats_broker\">\n");
        out.append("<a href=\"?\">Back to Broker overview</a><br />\n");

        String queueId = destinationId.substring("queue:".length());

        Collection<MatsBrokerDestination> values = _matsBrokerMonitor.getMatsDestinations().values();
        MatsBrokerDestination matsBrokerDestination = null;
        for (MatsBrokerDestination dest : values) {
            if (dest.getDestinationType() == DestinationType.QUEUE
                    && queueId.equals(dest.getDestinationName())) {
                matsBrokerDestination = dest;
            }
        }
        if (matsBrokerDestination == null) {
            throw new IllegalArgumentException("Unknown destination!");
        }

        out.append("Broker Queue '").append(queueId).append("'");
        // ?: Is this the Global DLQ?
        if (matsBrokerDestination.isGlobalDlq()) {
            // -> Yes, global DLQ
            out.append(" is the Global DLQ, fully qualified name: [")
                    .append(matsBrokerDestination.getFqDestinationName())
                    .append("]<br />\n");
        }
        else {
            // -> No, not the Global DLQ
            // ?: Is this a MatsStage Queue or DLQ?
            if (matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> Mats stage queue.
                out.append(" is the ");
                out.append(matsBrokerDestination.isDlq() ? "DLQ" : "incoming Queue");
                out.append(" for Mats Stage [")
                        .append(matsBrokerDestination.getMatsStageId().get());
            }
            else {
                // -> Non-Mats Queue. Not really supported, but just to handle it.
                out.append(" is a ");
                out.append(matsBrokerDestination.isDlq() ? "DLQ" : "Queue");
            }
            out.append("<br />\n");
        }
        out.append("It has ").append(Long.toString(matsBrokerDestination.getNumberOfQueuedMessages())).append(
                " messages");
        if (matsBrokerDestination.getNumberOfInflightMessages().isPresent()) {
            out.append(" of which ")
                    .append(Long.toString(matsBrokerDestination.getNumberOfInflightMessages().getAsLong()))
                    .append(" are in-flight.");
        }
        out.append("<br />\n");
        out.append("<br />\n");

        out.append("<div class=\"table-container\">");
        try (MatsBrokerMessageIterable iterable = _matsBrokerBrowseAndActions.browseQueue(queueId)) {
            out.append("<table class=\"mats_browse_messages\">");
            out.append("<thead>");
            out.append("<th>Sent</th>");
            out.append("<th>TraceId</th>");
            out.append("<th>Init App</th>");
            out.append("<th>InitatorId</th>");
            out.append("<th>Type</th>");
            out.append("<th>From Id</th>");
            out.append("<th>Persistent</th>");
            out.append("<th>Interactive</th>");
            out.append("</thead>");
            out.append("<tbody>");
            for (MatsBrokerMessageRepresentation matsMsg : iterable) {
                out.append("<tr>");

                out.append("<td>");
                out.append("<a href=\"?examineMessage&destinationId=").append(destinationId)
                        .append("&messageSystemId=").append(matsMsg.getMessageSystemId()).append("\">");
                Instant instant = Instant.ofEpochMilli(matsMsg.getTimestamp());
                out.append(formatTimestamp(instant));
                out.append("</a>");
                out.append("</td>");

                // Found MessageSystemId to be pretty irrelevant in this overview.

                out.append("<td>");
                out.append(matsMsg.getTraceId());
                out.append("</td>");

                out.append("<td>");
                out.append(matsMsg.getInitializingApp() != null ? matsMsg.getInitializingApp() : "{missing init app}");
                out.append("</td>");

                out.append("<td>");
                out.append(matsMsg.getInitiatorId() != null ? matsMsg.getInitiatorId() : "{missing init id}");
                out.append("</td>");

                out.append("<td>");
                out.append(matsMsg.getMessageType());
                out.append(" from");
                out.append("</td>");

                out.append("<td>");
                out.append(matsMsg.getFromStageId());
                out.append("</td>");

                out.append("<td class=\"bool\">");
                out.append(Boolean.toString(matsMsg.isPersistent()));
                out.append("</td>");

                out.append("<td class=\"bool\">");
                out.append(Boolean.toString(matsMsg.isInteractive()));
                out.append("</td>");

                out.append("</tr>\n");
            }
            out.append("</div>");
            out.append("</tbody>");
            out.append("</table>");
        }
        catch (BrokerIOException e) {
            throw new IOException("Can't talk with broker.", e);
        }
        out.append("</div>");
    }

    protected void examineMessage(Appendable out, String destinationId, String messageSystemId) throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.append("<div class=\"mats_report mats_broker\">\n");
        out.append("<a href=\"?browse&destinationId=").append(destinationId)
                .append("\">Back to Queue</a> - ");
        out.append("<a href=\"?\">Back to broker overview</a><br />\n");

        String queueId = destinationId.substring("queue:".length());
        Optional<MatsBrokerMessageRepresentation> matsBrokerMessageRepresentationO = _matsBrokerBrowseAndActions
                .examineMessage(queueId, messageSystemId);
        if (!matsBrokerMessageRepresentationO.isPresent()) {
            out.append("No such message! Queue:[" + queueId + "], MessageSystemId:[" + messageSystemId + "]<br />\n");
            out.append("</div>");
            return;
        }
        MatsBrokerMessageRepresentation matsMsg = matsBrokerMessageRepresentationO.get();

        MatsTrace<?> matsTrace = null;
        int matsTraceDecompressedLength = 0;
        if ((_matsSerializer != null)
                && matsMsg.getMatsTraceBytes().isPresent() && matsMsg.getMatsTraceMeta().isPresent()) {
            byte[] matsTraceBytes = matsMsg.getMatsTraceBytes().get();
            String matsTraceMeta = matsMsg.getMatsTraceMeta().get();
            DeserializedMatsTrace<?> deserializedMatsTrace = _matsSerializer.deserializeMatsTrace(matsTraceBytes,
                    matsTraceMeta);
            matsTraceDecompressedLength = deserializedMatsTrace.getSizeDecompressed();
            matsTrace = deserializedMatsTrace.getMatsTrace();
        }

        out.append("Queue:[" + queueId + "], MessageSystemId:[" + messageSystemId + "]<br />\n");

        out.append("<table class=\"mats_flow_and_message\"><tr>"); // start Flow/Message table
        out.append("<td>\n"); // start Flow information cell
        out.append("<h2>Flow information</h2>\n");

        // :: FLOW PROPERTIES
        out.append("<table class=\"mats_message_props\">");
        out.append("<thead>");
        out.append("<tr>");
        out.append("<th>Property</th>");
        out.append("<th>Value</th>");
        out.append("</tr>\n");
        out.append("</thead>");
        out.append("<tbody>");

        out.append("<tr>");
        out.append("<td>TraceId</td>");
        out.append("<td>").append(matsMsg.getTraceId()).append("</td>");
        out.append("</tr>\n");

        String initializingApp = "{no info present}";
        String initiatorId = "{no info present}";
        if (matsTrace != null) {
            initializingApp = matsTrace.getInitializingAppName() + "; v." + matsTrace.getInitializingAppVersion();
            initiatorId = matsTrace.getInitiatorId();
        }
        // ?: Do we have InitializingApp from MsgSys?
        // TODO: Remove this "if" in 2023.
        else if (matsMsg.getInitializingApp() != null) {
            initializingApp = matsMsg.getInitializingApp();
            initiatorId = matsMsg.getInitiatorId();
        }

        out.append("<tr>");
        out.append("<td>Initializing App @ Host</td>");
        out.append("<td>").append(initializingApp);
        if (matsTrace != null) {
            out.append(" @ ").append(matsTrace.getInitializingHost());
        }
        out.append("</td></tr>\n");

        out.append("<tr>");
        out.append("<td>Initiator Id</td>");
        out.append("<td>").append(initiatorId).append("</td>");
        out.append("</tr>\n");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Mats Flow Initialized Timestamp</td>");
            out.append("<td>").append(formatTimestamp(matsTrace.getInitializedTimestamp())).append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Mats Flow Id</td>");
            out.append("<td>").append(matsTrace.getFlowId()).append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Parent Mats Message Id</td>");
            out.append("<td>").append(matsTrace.getParentMatsMessageId() != null
                    ? matsTrace.getParentMatsMessageId()
                    : "<i>-no parent-</i>").append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Init debug info</td>");
            String debugInfo = matsTrace.getDebugInfo();
            if ((debugInfo == null) || (debugInfo.isEmpty())) {
                debugInfo = "{none present}";
            }
            out.append("<td>").append(debugInfo.replace(";", "<br>\n")).append("</td>");
            out.append("</tr>\n");
        }

        // .. MatsTrace props
        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>&nbsp;&nbsp;KeepMatsTrace</td>");
            out.append("<td>").append(matsTrace.getKeepTrace().toString()).append(" MatsTrace").append("</td>");
            out.append("</tr>\n");
            out.append("<tr>");
        }

        out.append("<tr>");
        out.append("<td>&nbsp;&nbsp;Persistent</td>");
        out.append("<td>").append(matsMsg.isPersistent() ? "Persistent" : "Non-Persistent").append("</td>");
        out.append("</tr>\n");

        out.append("<tr>");
        out.append("<td>&nbsp;&nbsp;Interactive</td>");
        out.append("<td>").append(matsMsg.isInteractive() ? "Interactive" : "Non-Interactive").append("</td>");
        out.append("</tr>\n");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>&nbsp;&nbsp;TimeToLive</td>");
            out.append("<td>").append(matsTrace.getTimeToLive() == 0 ? "Live Forever"
                    : matsTrace.getTimeToLive() + " ms").append("</td>");
            out.append("</tr>\n");
            out.append("<tr>");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>&nbsp;&nbsp;Audit</td>");
            out.append("<td>").append(matsTrace.isNoAudit() ? "No audit" : "Audit").append("</td>");
            out.append("</tr>\n");
        }

        out.append("</tbody>");
        out.append("</table>");

        out.append("</td>\n"); // end Flow information cell

        // :: MESSAGE PROPERTIES
        out.append("<td>\n"); // start Message information cell
        out.append("<h2>Message information (\"Current call\")</h2>");
        out.append("<table class=\"mats_message_props\">");
        out.append("<thead>");
        out.append("<tr>");
        out.append("<th>Property</th>");
        out.append("<th>Value</th>");
        out.append("</tr>\n");
        out.append("</thead>");
        out.append("<tbody>");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>From App @ Host</td>");
            out.append("<td>").append(matsTrace.getCurrentCall().getCallingAppName()
                    + "; v." + matsTrace.getCurrentCall().getCallingAppVersion())
                    .append(" @ ").append(matsTrace.getCurrentCall().getCallingHost())
                    .append("</td>");
            out.append("</tr>\n");
        }

        out.append("<tr>");
        out.append("<td>From</td>");
        out.append("<td>").append(matsMsg.getFromStageId()).append("</td>");
        out.append("</tr>\n");

        out.append("<tr>");
        out.append("<td>Type</td>");
        out.append("<td>").append(matsMsg.getMessageType()).append("</td>");
        out.append("</tr>\n");

        out.append("<tr>");
        out.append("<td>To (this)</td>");
        out.append("<td>").append(matsMsg.getToStageId()).append("</td>");
        out.append("</tr>\n");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Mats Message Timestamp</td>");
            out.append("<td>").append(formatTimestamp(matsTrace.getCurrentCall().getCalledTimestamp())).append("</td>");
            out.append("</tr>\n");
        }

        out.append("<tr>");
        out.append("<td>MsgSys Message Timestamp</td>");
        out.append("<td>").append(formatTimestamp(matsMsg.getTimestamp())).append("</td>");
        out.append("</tr>\n");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Call debug info</td>");
            String debugInfo = matsTrace.getCurrentCall().getDebugInfo();
            if ((debugInfo == null) || (debugInfo.trim().isEmpty())) {
                debugInfo = "{none present}";
            }
            debugInfo = debugInfo.replace(";", "<br>\n").replace('<', '{').replace('>', '}');
            out.append("<td>").append(debugInfo.replace(";", "<br>\n")).append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Mats Message Id</td>");
            out.append("<td>").append(matsTrace.getCurrentCall().getMatsMessageId()).append("</td>");
            out.append("</tr>\n");
        }

        out.append("<tr>");
        out.append("<td>MsgSys Message Id</td>");
        out.append("<td>").append(matsMsg.getMessageSystemId()).append("</td>");
        out.append("</tr>\n");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Call number</td>");
            out.append("<td>#").append(Integer.toString(matsTrace.getCallNumber())).append(" in this flow, #")
                    .append(Integer.toString(matsTrace.getTotalCallNumber())).append(" counting parent flows")
                    .append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>MatsTrace Size</td>");
            String size = matsMsg.getMatsTraceBytes().get().length == matsTraceDecompressedLength
                    ? matsTraceDecompressedLength + " bytes uncompressed"
                    : matsMsg.getMatsTraceBytes().get().length + " bytes compressed, "
                            + matsTraceDecompressedLength + " bytes decompressed";
            out.append("<td>").append(size).append("</td>");
            out.append("</tr>\n");
        }

        out.append("</tbody>");
        out.append("</table>");

        out.append("</td>\n"); // end Message information cell
        out.append("</tr></table>"); // end Flow/Message table

        if (matsMsg.getMatsTraceBytes().isPresent() && (matsTrace == null)) {
            out.append("<br/><h2>NOTICE! There is a serialized MatsTrace byte array in the message, but I am"
                    + " constructed without a MatsSerializer, so I can't decipher it!</h2><br />\n");
        }

        if (matsTrace != null) {

            currentCallMatsTrace(out, matsTrace);

            out.append("<pre>");
            out.append(matsTrace.toString().replace('<', '{').replace('>', '}'));
            out.append("</pre>");
        }

        out.append("</div>");
    }

    protected void currentCallMatsTrace(Appendable out, MatsTrace<?> matsTrace) throws IOException {
        Type[] interfaces = matsTrace.getClass().getGenericInterfaces();
        ParameterizedType matsTraceType = null;
        for (Type anInterface : interfaces) {
            if (anInterface instanceof ParameterizedType) {
                ParameterizedType ap = (ParameterizedType) anInterface;
                if (ap.getRawType().getTypeName().equals(MatsTrace.class.getName())) {
                    matsTraceType = ap;
                }
            }
        }
        if (null == matsTraceType) {
            out.append("<h2>Warning! - didn't find the MatsTrace interface of the MatsTrace implementation!</h2>");
            out.append("<pre>");
            out.append(matsTrace.toString().replace('<', '{').replace('>', '}'));
            out.append("</pre>");
            return;
        }

        Type[] actualTypeArguments = matsTraceType.getActualTypeArguments();

        boolean innerSerializeIsString = actualTypeArguments[0].getTypeName().equals(String.class.getName());

        if (!innerSerializeIsString) {
            out.append("<h2>Warning! - the inner type serialization (STOs and DTOs) isn't String! Cannot introspect.</h2>");
            out.append("<pre>");
            out.append(matsTrace.toString().replace('<', '{').replace('>', '}'));
            out.append("</pre>");
            return;
        }

        // ----- The inner type serialization (STOs and DTOs) is String. Assuming JSON.

        out.append("The \"inner type serialization\" (for STOs and DTOs) of MatsTrace is String, assuming JSON.<br />");

        @SuppressWarnings("unchecked")
        MatsTrace<String> stringMatsTrace = (MatsTrace<String>) matsTrace;

        Optional<StackState<String>> currentStateO = stringMatsTrace.getCurrentState();
        if (!currentStateO.isPresent()) {
            out.append("No current state.<br />");
            return;
        }
        String state = currentStateO.get().getState();
        String jsonState = new ObjectMapper().readTree(state).toPrettyString();
        out.append("Current state:<br />");
        out.append("<pre>").append(jsonState).append("</pre>");

        Call<String> currentCall = stringMatsTrace.getCurrentCall();
        String data = currentCall.getData();
        String jsonData = new ObjectMapper().readTree(state).toPrettyString();
        out.append("Incoming message:<br />");
        out.append("<pre>").append(jsonData).append("</pre>");
    }

    protected void overview(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        out.append("<div class=\"mats_report mats_broker\">\n");
        out.append("  <div class=\"mats_heading\">");
        Optional<BrokerInfo> brokerInfoO = _matsBrokerMonitor.getBrokerInfo();
        if (brokerInfoO.isPresent()) {
            BrokerInfo brokerInfo = brokerInfoO.get();
            out.append("Broker <h1>'").append(brokerInfo.getBrokerName()).append("'</h1>");
            out.append("   of type ").append(brokerInfo.getBrokerType());
        }
        else {
            out.append("<h2>Unknown broker</h2>");
        }
        out.append("  </div>\n");

        Map<String, MatsBrokerDestination> matsDestinations = _matsBrokerMonitor.getMatsDestinations();
        MatsFabricBrokerRepresentation stack = MatsFabricBrokerRepresentation.stack(matsDestinations.values());

        // :: ToC
        out.append("<b>EndpointGroups ToC</b><br />\n");
        for (MatsEndpointGroupBrokerRepresentation service : stack.getMatsEndpointGroupBrokerRepresentations()
                .values()) {
            String endpointGroupId = service.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : service.getEndpointGroup();
            out.append("&nbsp;&nbsp;<b><a href=\"#").append(endpointGroupId).append("\">")
                    .append(endpointGroupId)
                    .append("</a></b><br />\n");
        }
        out.append("<br />\n");

        // :: Global DLQ
        if (stack.getGlobalDlq().isPresent()) {
            out.append("<div class=\"mats_endpoint_group\">\n");
            out.append("<h2>Global DLQ</h2><br />");
            MatsBrokerDestination globalDlq = stack.getGlobalDlq().get();
            out.append("<div class=\"mats_epid\">")
                    .append(globalDlq.getDestinationName())
                    .append("</div>");
            out.append("<div class=\"mats_stage\">")
                    .append(globalDlq.getFqDestinationName());

            queueCount(out, globalDlq);

            out.append("</div>");
            out.append("</div>");
        }

        // :: Foreach EndpointGroup
        for (MatsEndpointGroupBrokerRepresentation service : stack.getMatsEndpointGroupBrokerRepresentations()
                .values()) {
            // :: EndpointGroup
            String endpointGroupId = service.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : service.getEndpointGroup();
            out.append("<div class=\"mats_endpoint_group\" id=\"").append(endpointGroupId).append("\">\n");
            out.append("<a href=\"#").append(endpointGroupId).append("\">");
            out.append("<h2>").append(endpointGroupId).append("</h2></a><br />\n");

            // :: Foreach Endpoint
            for (MatsEndpointBrokerRepresentation endpoint : service.getMatsEndpointBrokerRepresentations().values()) {
                String endpointId = endpoint.getEndpointId();
                Map<Integer, MatsStageBrokerRepresentation> stages = endpoint.getStages();

                // There will always be at least one stage, otherwise the endpoint wouldn't be defined.
                MatsStageBrokerRepresentation first = stages.values().iterator().next();
                // There will either be an incoming, or a DLQ, otherwise the endpoint wouldn't be defined.
                MatsBrokerDestination firstIncomingOrDlq = first.getIncomingDestination()
                        .orElseGet(() -> first.getDlqDestination()
                                .orElseThrow(() -> new AssertionError("Missing both Incoming and DLQ destinations!")));
                String endpointType = firstIncomingOrDlq.getDestinationType() == DestinationType.QUEUE
                        ? "<div class=\"mats_queue\">Queue</div>"
                        : "<div class=\"mats_topic\">Topic</div>";

                out.append("<div class=\"mats_epid\">").append(endpointId).append("</div>");
                out.append(" ").append(endpointType);

                // :: Foreach Stage
                for (MatsStageBrokerRepresentation stage : stages.values()) {
                    out.append("<div class=\"mats_stage\">");
                    out.append(stage.getStageIndex() == 0 ? "Initial" : "S" + stage.getStageIndex());
                    Optional<MatsBrokerDestination> incomingDest = stage.getIncomingDestination();
                    if (incomingDest.isPresent()) {
                        MatsBrokerDestination incoming = incomingDest.get();
                        queueCount(out, incoming);
                    }

                    Optional<MatsBrokerDestination> dlqDest = stage.getDlqDestination();
                    if (dlqDest.isPresent()) {
                        queueCount(out, dlqDest.get());
                    }
                    out.append("</div>"); // /mats_stage
                }
                out.append("<br />\n");
            }
            out.append("</div>\n");
        }
        out.append("</div>\n");
    }

    private void queueCount(Appendable out, MatsBrokerDestination destination) throws IOException {
        String style = destination.isDlq()
                ? "dlq"
                : destination.getNumberOfQueuedMessages() == 0 ? "incoming_zero" : "incoming";
        out.append("<a class=\"").append(style).append("\" href=\"?browse&destinationId=")
                .append(destination.getDestinationType() == DestinationType.QUEUE ? "queue:" : "topic:")
                .append(destination.getDestinationName())
                .append("\">")
                .append(destination.isDlq() ? "DLQ:" : "")
                .append(Long.toString(destination.getNumberOfQueuedMessages()))
                .append("</a>");
        long age = destination.getHeadMessageAgeMillis().orElse(0);
        if (age > 0) {
            out.append("<div class=\"mats_age\">(").append(millisSpanToHuman(age)).append(")</div>");
        }
    }

    private static String millisSpanToHuman(long millis) {
        if (millis < 60_000) {
            return millis + " ms";
        }
        else {
            Duration d = Duration.ofMillis(millis);
            long days = d.toDays();
            d = d.minusDays(days);
            long hours = d.toHours();
            d = d.minusHours(hours);
            long minutes = d.toMinutes();
            d = d.minusMinutes(minutes);
            long seconds = d.getSeconds();

            StringBuilder buf = new StringBuilder();
            if (days > 0) {
                buf.append(days).append("d");
            }
            if ((hours > 0) || (buf.length() != 0)) {
                if (buf.length() != 0) {
                    buf.append(":");
                }
                buf.append(hours).append("h");
            }
            if ((minutes > 0) || (buf.length() != 0)) {
                if (buf.length() != 0) {
                    buf.append(":");
                }
                buf.append(minutes).append("m");
            }
            if (buf.length() != 0) {
                buf.append(":");
            }
            buf.append(seconds).append("s");
            return buf.toString();
        }
    }

    private static String formatTimestamp(Instant instant) {
        long millisAgo = System.currentTimeMillis() - instant.toEpochMilli();
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault()) + " (" + millisSpanToHuman(millisAgo) + ")";
    }

    private static String formatTimestamp(long timestamp) {
        return formatTimestamp(Instant.ofEpochMilli(timestamp));
    }

    static final DecimalFormatSymbols NF_SYMBOLS;
    static final DecimalFormat NF_INTEGER;
    static final DecimalFormat NF_3_DECIMALS;
    static {
        NF_SYMBOLS = new DecimalFormatSymbols(Locale.US);
        NF_SYMBOLS.setDecimalSeparator('.');
        NF_SYMBOLS.setGroupingSeparator('\u202f');

        NF_INTEGER = new DecimalFormat("#,##0");
        NF_INTEGER.setMaximumFractionDigits(0);
        NF_INTEGER.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_3_DECIMALS = new DecimalFormat("#,##0.000");
        NF_3_DECIMALS.setMaximumFractionDigits(3);
        NF_3_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);
    }
}
