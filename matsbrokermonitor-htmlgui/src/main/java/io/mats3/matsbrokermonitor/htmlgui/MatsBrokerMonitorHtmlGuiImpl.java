package io.mats3.matsbrokermonitor.htmlgui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.mats3.serial.MatsTrace.Call.CallType;
import io.mats3.serial.MatsTrace.KeepMatsTrace;
import io.mats3.serial.MatsTrace.StackState;

/**
 * Instantiate a <b>singleton</b> of this class, supplying it a {@link MatsBrokerMonitor} instance, <b>to which this
 * instance will register as listener</b>. Again: You are NOT supposed to instantiate an instance of this class per
 * rendering, as this instance is "active" and will register as listener and may instantiate threads.
 *
 * @author Endre Stølsvik 2021-12-17 10:22 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerMonitorHtmlGuiImpl implements MatsBrokerMonitorHtmlGui {
    private final Logger log = LoggerFactory.getLogger(MatsBrokerMonitorHtmlGuiImpl.class);

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
        includeFile(out, "matsbrokermonitor.css");
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getJavaScript(Appendable out) throws IOException {
        includeFile(out, "matsbrokermonitor.js");
    }

    private static void includeFile(Appendable out, String file) throws IOException {
        String filename = MatsBrokerMonitorHtmlGuiImpl.class.getPackage().getName().replace('.', '/') + '/' + file;
        InputStream is = MatsBrokerMonitorHtmlGuiImpl.class.getClassLoader().getResourceAsStream(filename);
        if (is == null) {
            throw new IllegalStateException("Missing '" + file + "' from ClassLoader.");
        }
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        while (true) {
            String line = br.readLine();
            if (line == null) {
                break;
            }
            out.append(line).append('\n');
        }
    }

    public void main(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        if (requestParameters.containsKey("browse")) {
            String destinationId = getDestinationId(requestParameters, ac);
            // ----- Passed Access Control for browse of specific destination, render it.
            browseQueue(out, destinationId, ac);
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

    protected void browseQueue(Appendable out, String destinationId, AccessControl ac)
            throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.append("<div class=\"matsbm_report matsbm_broker\">\n");
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

        try (MatsBrokerMessageIterable iterable = _matsBrokerBrowseAndActions.browseQueue(queueId)) {
            out.append("<table class=\"matsbm_table_browse_queue\">");
            out.append("<thead>");
            out.append("<th>Sent</th>");
            out.append("<th>TraceId</th>");
            out.append("<th>Init App</th>");
            out.append("<th>InitatorId</th>");
            out.append("<th>Type</th>");
            out.append("<th>From Id</th>");
            out.append("<th>Persistent</th>");
            out.append("<th>Interactive</th>");
            out.append("<th>Expires</th>");
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

                out.append("<td>");
                out.append(matsMsg.isPersistent() ? "Persistent" : "Non-Persistent");
                out.append("</td>");

                out.append("<td>");
                out.append(matsMsg.isInteractive() ? "Interactive" : "Non-Interactive");
                out.append("</td>");

                out.append("<td>");
                out.append(matsMsg.getExpirationTimestamp() == 0
                        ? "Never expires"
                        : formatTimestamp(matsMsg.getExpirationTimestamp()));
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
    }

    protected void examineMessage(Appendable out, String destinationId, String messageSystemId) throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.append("<div class=\"matsbm_report matsbm_broker\">\n");
        out.append("<a href=\"?browse&destinationId=").append(destinationId)
                .append("\">Back to Queue</a> - ");
        out.append("<a href=\"?\">Back to broker overview</a><br />\n");

        String queueId = destinationId.substring("queue:".length());
        Optional<MatsBrokerMessageRepresentation> matsBrokerMessageRepresentationO = _matsBrokerBrowseAndActions
                .examineMessage(queueId, messageSystemId);
        if (!matsBrokerMessageRepresentationO.isPresent()) {
            out.append("<h1>No such message!</h1><br/>\n");
            out.append("MessageSystemId: [" + messageSystemId + "].<br/>\n");
            out.append("Queue:[" + queueId + "]<br/>\n");
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

        // :: FLOW AND MESSAGE PROPERTIES

        examineMessage_FlowAndMessageProperties(out, matsMsg, matsTrace, matsTraceDecompressedLength);

        // :: Incoming message and state

        Optional<MatsTrace<String>> stringMatsTraceO = getStringMatsTrace(matsTrace);

        if (stringMatsTraceO.isPresent()) {
            examimeMessage_currentCallMatsTrace(out, stringMatsTraceO.get());
        }



        // :: MATS TRACE!

        if (matsTrace == null) {
            // -> No MatsTrace, why?
            if (matsMsg.getMatsTraceBytes().isPresent()) {
                // -> Seemingly because we don't have a MatsSerializer, and thus cannot deserialize the present bytes.
                out.append("<br/><h2>NOTICE! There is a serialized MatsTrace byte array in the message, but I am"
                        + " constructed without a MatsSerializer, so I can't decipher it!</h2><br />\n");
            }
            else {
                // -> Evidently because there was no MatsTrace in the message.
                out.append("<br/><h2>NOTICE! Missing MatsTrace information from the message, so cannot show"
                        + " call trace information!</h2><br />\n");
            }
        }
        else {
            // -> We do have a MatsTrace, output what we can
            examineMessage_MatsTrace(out, matsTrace);
        }

        if (!stringMatsTraceO.isPresent()) {
            out.append("<h2>NOTICE: couldn't resolve MatsTrace to MatsTrace&lt;String&gt;!</h2>");
            out.append("Here's matsTrace.toString() of the MatsTrace present:");
            out.append("<pre>");
            out.append(matsTrace.toString().replace('<', '{').replace('>', '}'));
            out.append("</pre>");
        }

        out.append("Here's matsMessage.toString(), which should include the raw info from the broker:<br/>\n");
        out.append(matsMsg.toString());

        out.append("</div>");
    }

    private void examineMessage_FlowAndMessageProperties(Appendable out, MatsBrokerMessageRepresentation matsMsg,
            MatsTrace<?> matsTrace,
            int matsTraceDecompressedLength) throws IOException {
        out.append("<table class=\"matsbm_table_flow_and_message\"><tr>"); // start Flow/Message table
        out.append("<td>\n"); // start Flow information cell
        out.append("<h2>Flow information</h2>\n");

        // :: FLOW PROPERTIES
        out.append("<table class=\"matsbm_table_message_props\">");
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
        out.append("<table class=\"matsbm_table_message_props\">");
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

        out.append("<tr>");
        out.append("<td>Expires</td>");
        out.append("<td>").append(matsMsg.getExpirationTimestamp() == 0
                ? "Never expires"
                : formatTimestamp(matsMsg.getExpirationTimestamp()))
                .append("</td>");
        out.append("</tr>");

        out.append("</tbody>");
        out.append("</table>");

        out.append("</td>\n"); // end Message information cell
        out.append("</tr></table>"); // end Flow/Message table
    }

    private void examineMessage_MatsTrace(Appendable out, MatsTrace<?> matsTrace) throws IOException {
        List<? extends Call<?>> callFlow = matsTrace.getCallFlow();
        List<? extends StackState<?>> stateFlow = matsTrace.getStateFlow();

        // TODO: ONLY DO IF KeepMatsTrace.FULL

        /*
         * Determine if the initial REQUEST or SEND was performed with initial state (dto, initialState) or normal
         * (dto). This is a bit convoluted, as the state flow data structure really has no direct correlation to the
         * data structure of the call flow (!). However, from the call flow, we can deduce how the state flow must have
         * occurred - both with and without initalState. Then we compare it against the actual stateFlow we have, and
         * see which one that match!
         */

        // :: REQUESTS:
        // The initial call, which is a REQUEST, "leaves" a state at level 0 for the terminator.

        // From StateFlow with normal "request(dto)":
        // 0:1:2:2:1:1:2:2:1:1:1:2:2:1:2:2:

        // From StateFlow with "request(dto, initialState)":
        // # Notice the added extra "1" in front after the "0", for the initialState to requested endpoint.
        // 0:1:1:2:2:1:1:2:2:1:1:1:2:2:1:2:2:

        // :: SENDS:
        // The initial call, which is a SEND, does NOT leave a state since there is no replyTo terminator.
        // Notice how the first element is therefore missing, and all the rest are "-1" in height compared to the
        // identical initial-call-is-REQUEST flows.

        // From StateFlow with "send(dto)":
        // 0:1:1:0:0:1:1:0:0:0:1:1:0:1:1:

        // From StateFlow with "send(dto, initialState)"
        // # Notice the added extra "0" in front, for the initialState to the sent-to endpoint.
        // 0:0:1:1:0:0:1:1:0:0:0:1:1:0:1:1:

        StringBuilder actualStateHeightsFromStateFlow = new StringBuilder();
        stateFlow.forEach(stackState -> actualStateHeightsFromStateFlow.append(stackState.getHeight()).append(':'));

        StringBuilder stateHeightsFromCallFlow_normal = new StringBuilder();
        StringBuilder stateHeightsFromCallFlow_initialState = new StringBuilder();
        boolean initiationIsSend = callFlow.get(0).getCallType() == CallType.SEND;
        for (int i = 0; i < callFlow.size(); i++) {
            // ?: Assuming initialState: Are we BEFORE the initial call which is a SEND?
            if ((i == 0) && initiationIsSend) {
                // -> Yes, before initial call which was SEND.
                // A SEND send does NOT add state itself, since there is no replyTo / no terminator to receive it.
                // Since we assume initialState, it added initialState at level 0 for targeted endpoint.
                // This latter we add here, since it isn't represented in the call flow.
                stateHeightsFromCallFlow_initialState.append("0:");
            }
            Call<?> call = callFlow.get(i);
            if (call.getCallType() == CallType.REQUEST) {
                stateHeightsFromCallFlow_normal.append(call.getReplyStackHeight() - 1).append(':');
                stateHeightsFromCallFlow_initialState.append(call.getReplyStackHeight() - 1).append(':');
            }
            if ((call.getCallType() == CallType.NEXT) || (call.getCallType() == CallType.GOTO)) {
                stateHeightsFromCallFlow_normal.append(call.getReplyStackHeight()).append(':');
                stateHeightsFromCallFlow_initialState.append(call.getReplyStackHeight()).append(':');
            }
            // ?: Assuming initialState: Are we AFTER the initial call which is a REQUEST?
            if ((i == 0) && (!initiationIsSend)) {
                // -> Yes, after initial call which was REQUEST.
                // A REQUEST adds a state at level 0 for the message to the replyTo / terminator.
                // Since we assume initialState, it also added initialState at level 1 for targeted endpoint.
                // This latter we add here, since it isn't represented in the call flow.
                stateHeightsFromCallFlow_initialState.append("1:");
            }
        }

        int initialStateStatus; // 1:normal, 2:initialState, -1:neither (can't deduce, can't resolve states)
        // ?: Was this a /normal/ flow, without initialState from initiation?
        if (actualStateHeightsFromStateFlow.toString().equals(stateHeightsFromCallFlow_normal.toString())) {
            // -> Yes, /normal/ flow
            initialStateStatus = 1;
        }
        // ?: Was this an initiation with initialState to the called endpoint?
        else if (actualStateHeightsFromStateFlow.toString().equals(stateHeightsFromCallFlow_initialState
                .toString())) {
            // -> Yes, initiation with initialState.
            initialStateStatus = 2;
        }
        else {
            // We don't comprehend the state flow, so we can't pretend to display it sanely.
            initialStateStatus = -1;
        }

        // out.append("" + actualStateHeightsFromStateFlow).append(" -- actual<br/>\n");
        // out.append("" + stateHeightsFromCallFlow_normal).append(" -- fromCallFlow assuming normal<br/>\n");
        // out.append("" + stateHeightsFromCallFlow_initialState).append(
        // " -- fromCallFlow assuming initialState<br/>\n");
        // out.append("InitialStateStatus: " + initialStateStatus + "<br/>\n");

        // :: Go through the state flow, and re-run the stack, so that we know incoming state, if any, for each call

        // Store the incoming state as an identity map from the call.
        IdentityHashMap<Call<?>, StackState<?>> callToState = new IdentityHashMap<>();
        // ?: Did we comprehend the state flow?
        if (initialStateStatus > 0) {
            // -> Yes, we understood the stateflow, so do the hook-on to the calls having incoming state
            Deque<StackState<?>> stack = new ArrayDeque<>();
            Iterator<? extends StackState<?>> stateFlowIt = stateFlow.iterator();
            // ?: Was the initiation call a REQUEST?
            if (!initiationIsSend) {
                // -> Initiation call is REQUEST, so push a state from flow to stack
                stack.push(stateFlowIt.next());
            }

            // ?: Was this an initiation call with initialState?
            if (initialStateStatus == 2) {
                // -> Yes, it had initialState, so pick off the initialState
                callToState.put(callFlow.get(0), stateFlowIt.next());
            }

            // Begin from the next call (after we've handled the possible initialState above)
            for (int i = 1; i < callFlow.size(); i++) {
                Call<?> call = callFlow.get(i);
                if (call.getCallType() == CallType.REQUEST) {
                    // -> REQUEST, so we push a state from the stateflow.
                    stack.push(stateFlowIt.next());
                }
                else if ((call.getCallType() == CallType.REPLY)) {
                    // -> REPLY, so we pop a state from the stack
                    callToState.put(call, stack.pop());
                }
                else if ((call.getCallType() == CallType.NEXT) || (call.getCallType() == CallType.GOTO)) {
                    // -> NEXT (or GOTO, which semantically is equal), so we need to push to the stack from the
                    // stateflow, and then pop from the stack, which cooks down to pulling directly from the stateflow.
                    callToState.put(call, stateFlowIt.next());
                }
                else {
                    throw new AssertionError("Don't know [" + call.getCallType() + "].");
                }
            }
        }

        // :: CALLS TABLE

        out.append("<h2>MatsTrace</h2><br/>\n");
        out.append("<b>Remember that the MatsTrace, and the rows in this table, refers to the <i>calls, i.e. the"
                + " messages from one stage to the next in a flow</i>, not the processing on the stages"
                + " themselves.</b><br/>\n");
        out.append("Thus, it is the REQUEST, REPLY and NEXT parts in the table that are the real info carriers -"
                + " the \"processed on\" lines in the table are extracted from the previous stage in the flow,"
                + " just to aid your intuition.<br />\n");

        out.append("<table class=\"matsbm_table_call_flow\">");
        out.append("<thead>");
        out.append("<tr>");
        out.append("<th>Call#</th>");
        out.append("<th>time</th>");
        out.append("<th>diff</th>");
        out.append("<th>Call/Processing</th>");
        out.append("<th>Application</th>");
        out.append("<th>Host</th>");
        out.append("<th>DebugInfo</th>");
        out.append("</tr>");
        out.append("</thead>");

        // :: Flow
        out.append("<tbody>");

        // :: MatsTrace's Initiation
        out.append("<tr>");
        out.append("<td>#0</td>");
        out.append("<td>0 ms</td>");
        out.append("<td></td>");
        out.append("<td>");
        out.append("INIT<br />");
        out.append(matsTrace.getInitiatorId());
        out.append("</td><td>");
        out.append(matsTrace.getInitializingAppName())
                .append("; v.").append(matsTrace.getInitializingAppVersion());
        out.append("</td><td>");
        out.append(matsTrace.getInitializingHost());
        out.append("</td><td>");
        out.append(debugInfoToHtml(matsTrace.getDebugInfo()));
        out.append("</td>");
        out.append("</tr>\n");

        // :: IF we're in MINIMAL mode, output empty rows to represent the missing calls.
        if (matsTrace.getKeepTrace() == KeepMatsTrace.MINIMAL) {
            int currentCallNumber = matsTrace.getCallNumber();
            for (int i = 0; i < currentCallNumber - 1; i++) {
                out.append("<tr><td colspan=100></td></tr>");
            }
        }

        // :: MatsTrace's Calls

        long initializedTimestamp = matsTrace.getInitializedTimestamp();
        // NOTE: If we are KeepMatsTrace.MINIMAL, then there is only 1 entry here
        String prevIndent = "";
        long previousCalledTimestamp = matsTrace.getInitializedTimestamp();
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            // If there is only one call, then it is either first, or MINIMAL and last.
            int currentCallNumber = callFlow.size() == 1 ? matsTrace.getCallNumber() : i + 1;
            // Can we get a nextCall?
            Call<?> prevCall = (i == 0)
                    ? null
                    : callFlow.get(i - 1);
            out.append("<tr>");
            out.append("<td>#");
            out.append(Integer.toString(currentCallNumber));
            out.append("</td>");
            long currentCallTimestamp = currentCall.getCalledTimestamp();
            out.append("<td>").append(Long.toString(currentCallTimestamp - initializedTimestamp))
                    .append("&nbsp;ms</td>");
            long diffBetweenLast = currentCallTimestamp - previousCalledTimestamp;
            previousCalledTimestamp = currentCallTimestamp;
            out.append("<td>").append(Long.toString(diffBetweenLast)).append("&nbsp;ms</td>"); // Proc
            int replyStackHeight = currentCall.getReplyStackHeight();
            StringBuilder indentBuf = new StringBuilder("&nbsp;&nbsp;");
            for (int x = 0; x < replyStackHeight; x++) {
                // indentBuf.append("\u00A6&nbsp;&nbsp;");
                indentBuf.append("\u00A6&nbsp;&nbsp;&nbsp;&nbsp;");
                if (x != (replyStackHeight - 1)) {
                    indentBuf.append("&nbsp;&nbsp;");
                }
            }
            String indent = indentBuf.toString();
            out.append("<td onclick='matsbm_call(event)' data-callno="+currentCallNumber+">")
                    .append(prevIndent + "&nbsp;<i>(processed&nbsp;on&nbsp;");
            prevIndent = indent;
            if (prevCall != null) {
                out.append(prevCall.getTo().getId());
            }
            else {
                out.append("Initiation");
            }
            out.append(")</i><br />\n");
            String indentAndCallType;
            switch (currentCall.getCallType()) {
                case REQUEST:
                    indentAndCallType = indent + "\u2198 this is a REQUEST";
                    break;
                case REPLY:
                    indentAndCallType = indent + "&nbsp;&nbsp;\u2199 this is a REPLY";
                    break;
                case NEXT:
                case GOTO:
                    indentAndCallType = indent + "&nbsp;<b>\u2193</b>&nbsp; this is a " + currentCall.getCallType();
                    break;
                default:
                    indentAndCallType = indent + "this is a " + currentCall.getCallType();
            }
            out.append(indentAndCallType).append(" call");
            StackState<?> stackState = callToState.get(currentCall);
            if (stackState != null) {
                out.append(i == 0 ? " w/ initial state" : " w/ state");
            }
            out.append(" - <a href='//show call' onclick='matsbm_noclick()'>show</a>");
            out.append("<br/>");
            out.append(indent);
            if (replyStackHeight > 0) {
                out.append("&nbsp;");
            }

            out.append("<i>to:</i>&nbsp;").append(currentCall.getTo().getId());
            out.append("</td>");

            out.append("<td>");
            out.append(currentCall.getCallingAppName())
                    .append("; v.").append(currentCall.getCallingAppVersion());
            out.append("</td><td>");
            out.append(currentCall.getCallingHost());
            out.append("</td><td>");
            out.append(debugInfoToHtml(currentCall.getDebugInfo()));
            out.append("</td>");

            out.append("</tr>");
        }
        out.append("</table>");

        // :: MODALS: Calls and optionally also state


        out.append("<div id='matsmb_callmodalunderlay' class='matsmb_callmodalunderlay' onclick='matsmb_clearcallmodal()'>");

        String previousTo = "Initiation";
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            // If there is only one call, then it is either first, or MINIMAL and last.
            int currentCallNumber = callFlow.size() == 1 ? matsTrace.getCallNumber() : i + 1;
            out.append("<div class=\"matsbm_box_call_and_state_modal\" id='matsbm_call_"+currentCallNumber+"'>\n");
            out.append("This is a message from <b>").append(previousTo)
                    .append("</b><br/>on application <b>").append(currentCall.getCallingAppName())
                    .append("; v.").append(currentCall.getCallingAppVersion())
                    .append("</b><br/>running on node <b>").append(currentCall.getCallingHost())
                    .append("</b><br/>.. and it is a<br />\n");
            out.append("<h3>").append(currentCall.getCallType().toString())
                    .append(" call to <b>").append(currentCall.getTo().getId())
                    .append("</b></h3><br/>\n");
            previousTo = currentCall.getTo().getId();
            // State:
            out.append("<div class=\"matsbm_box_call_or_state\">\n");
            StackState<?> stackState = callToState.get(currentCall);
            if (stackState != null) {
                out.append("Incoming state: ");
                presentTransferredObject(out, stackState.getState());
            }
            else {
                out.append("<i>-no incoming state-</i>");
            }
            out.append("</div><br/>\n");

            // Message:
            out.append("<div class=\"matsbm_box_call_or_state\">\n");
            out.append("Incoming message: ");
            presentTransferredObject(out, currentCall.getData());
            out.append("</div><br />\n");

            out.append("</div><br/>\n");
        }
        out.append("</div>");

        out.append(""
                + "<button class=\"trigger\">Click here to trigger the modal!</button>\n"
                + "    <div class=\"modal\">\n"
                + "        <div class=\"modal-content\">\n"
                + "            <span class=\"close-button\">&times;</span>\n"
                + "            <h1>Hello, I am a modal!</h1>\n"
                + "        </div>\n"
                + "    </div>");


        // TEMP:
        out.append("<br /><br /><br /><br />");
        out.append("Temporary! MatsTrace.toString()");
        out.append("<pre>");
        out.append(matsTrace.toString());
        out.append("</pre>");
    }

    private void presentTransferredObject(Appendable out, Object data) throws IOException {
        if (data instanceof String) {
            String stringData = (String) data;
            out.append("String[").append(Integer.toString(stringData.length())).append(" chars]<br/>\n");

            try {
                String jsonData = new ObjectMapper().readTree(stringData).toPrettyString();
                out.append("<div class=\"matsbm_box_call_or_state_div\">").append(jsonData).append("</div>");
            }
            catch (JsonProcessingException e) {
                out.append("Couldn't parse incoming String as json (thus no pretty printing), so here it is unparsed.<br/>");
                out.append(stringData);
            }
        }
        if (data instanceof byte[]) {
            byte[] byteData = (byte[]) data;
            out.append("byte[").append(Integer.toString(byteData.length)).append(" bytes]<br/>\n");
        }
    }

    private String debugInfoToHtml(String debugInfo) {
        if ((debugInfo == null) || (debugInfo.trim().isEmpty())) {
            debugInfo = "{none present}";
        }
        debugInfo = debugInfo.replace(";", "<br>\n").replace("<", "&lt;").replace(">", "&gt;");
        return "<code>" + debugInfo + "</code>";
    }

    protected void examimeMessage_currentCallMatsTrace(Appendable out, MatsTrace<String> stringMatsTrace)
            throws IOException {
        if (stringMatsTrace != null) {
            Call<String> currentCall = stringMatsTrace.getCurrentCall();
            String data = currentCall.getData();
            String jsonData = new ObjectMapper().readTree(data).toPrettyString();
            out.append("Incoming message:<br />");
            out.append("<pre>").append(jsonData).append("</pre>");

            Optional<StackState<String>> currentStateO = stringMatsTrace.getCurrentState();
            if (!currentStateO.isPresent()) {
                out.append("No incoming state.<br />");
            }
            else {
                String state = currentStateO.get().getState();
                String jsonState = new ObjectMapper().readTree(state).toPrettyString();
                out.append("Incoming state:<br />");
                out.append("<pre>").append(jsonState).append("</pre>");
            }
        }
    }

    private Optional<MatsTrace<String>> getStringMatsTrace(MatsTrace<?> matsTrace) throws IOException {
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
        // ?: Did we find the MatsTrace type?
        if (null == matsTraceType) {
            // -> No, so no can do.
            return Optional.empty();
        }

        Type[] actualTypeArguments = matsTraceType.getActualTypeArguments();
        boolean innerSerializeIsString = actualTypeArguments[0].getTypeName().equals(String.class.getName());
        // ?: Do we have inner serialization as String?
        if (!innerSerializeIsString) {
            return Optional.empty();
        }
        // E-> Yes, the inner serialization is String - hopefully JSON!
        @SuppressWarnings("unchecked")
        MatsTrace<String> stringMatsTrace = (MatsTrace<String>) matsTrace;
        return Optional.of(stringMatsTrace);
    }

    protected void overview(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        out.append("<div class=\"matsbm_report matsbm_broker\">\n");
        out.append("  <div class=\"matsbm_heading\">");
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
            out.append("<div class=\"matsbm_endpoint_group\">\n");
            out.append("<h2>Global DLQ</h2><br />");
            MatsBrokerDestination globalDlq = stack.getGlobalDlq().get();
            out.append("<div class=\"matsbm_epid\">")
                    .append(globalDlq.getDestinationName())
                    .append("</div>");
            out.append("<div class=\"matsbm_stage\">")
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
            out.append("<div class=\"matsbm_endpoint_group\" id=\"").append(endpointGroupId).append("\">\n");
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
                        ? "<div class=\"matsbm_queue\">Queue</div>"
                        : "<div class=\"matsbm_topic\">Topic</div>";

                out.append("<div class=\"matsbm_epid\">").append(endpointId).append("</div>");
                out.append(" ").append(endpointType);

                // :: Foreach Stage
                for (MatsStageBrokerRepresentation stage : stages.values()) {
                    out.append("<div class=\"matsbm_stage\">");
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
                    out.append("</div>"); // /matsbm_stage
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
            out.append("<div class=\"matsbm_age\">(").append(millisSpanToHuman(age)).append(")</div>");
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
