package io.mats3.matsbrokermonitor.htmlgui.impl;

import static io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType.UNKNOWN;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.BrokerIOException;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerSnapshot;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.AccessControl;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.ExamineMessageAddition;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.MonitorAddition;
import io.mats3.serial.MatsSerializer;
import io.mats3.serial.MatsSerializer.DeserializedMatsTrace;
import io.mats3.serial.MatsTrace;
import io.mats3.serial.MatsTrace.Call;
import io.mats3.serial.MatsTrace.Call.CallType;
import io.mats3.serial.MatsTrace.Call.Channel;
import io.mats3.serial.MatsTrace.KeepMatsTrace;
import io.mats3.serial.MatsTrace.StackState;

/**
 * @author Endre Stølsvik 2022-03-13 23:33 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ExamineMessage {
    private static final Logger log = LoggerFactory.getLogger(ExamineMessage.class);

    static void gui_ExamineMessage(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions, MatsSerializer<?> matsSerializer,
            List<? super MonitorAddition> monitorAdditions,
            Outputter out, String queueId, String messageSystemId, AccessControl ac) throws IOException {
        out.html("<div id='matsbm_page_examine_message' class='matsbm_report'>\n");
        out.html("<div class='matsbm_actionbuttons'>\n");
        out.html("<a id='matsbm_back_broker_overview' href='?'>Back to Broker Overview</a><br>\n");
        out.html("<a id='matsbm_back_browse_queue' href='?browse&destinationId=queue:").DATA(queueId)
                .html("'>Back to Queue [Esc]</a>").html("<br>\n");

        // :: Verify that we have the queue in the stats
        // (Otherwise we'll make the queue by just browsing for the message)
        Collection<MatsBrokerDestination> brokerDestinations = matsBrokerMonitor.getSnapshot()
                .map(BrokerSnapshot::getMatsDestinations)
                .map(SortedMap::values)
                .orElseGet(Collections::emptySet);

        MatsBrokerDestination matsBrokerDestination = null;
        for (MatsBrokerDestination d : brokerDestinations) {
            if ((d.getDestinationType() == DestinationType.QUEUE) && queueId.equals(d.getDestinationName())) {
                matsBrokerDestination = d;
            }
        }
        if (matsBrokerDestination == null) {
            out.html("</div>");
            out.html("<h1>Can't look up message, because no info about the queue!</h1><br>\n");
            out.html("<b>MessageSystemId:</b> ").DATA(messageSystemId).html(".<br>\n");
            out.html("<b>Queue:</b> ").DATA(queueId).html("<br>\n");
            out.html("<br>");
            // Don't output last </div>, as caller does it.
            return;
        }

        Optional<MatsBrokerMessageRepresentation> matsBrokerMessageRepresentationO;
        try {
            matsBrokerMessageRepresentationO = matsBrokerBrowseAndActions.examineMessage(queueId, messageSystemId);
        }
        catch (BrokerIOException e) {
            out.html("<h1>Got BrokerIOException when trying to examine message!</h1><br>\n");
            log.error("Got BrokerIOException when trying to examine message!", e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            out.html("</div><pre>").DATA(sw.toString()).html("</pre>");
            // Don't output last </div>, as caller does it.
            return;
        }
        if (matsBrokerMessageRepresentationO.isEmpty()) {
            out.html("</div>");
            out.html("<h1>No such message!</h1><br>\n");
            out.html("<b>MessageSystemId:</b> ").DATA(messageSystemId).html(".<br>\n");
            out.html("<b>Queue:</b> ").DATA(queueId).html("<br>\n");
            out.html("<br>");
            // Don't output last </div>, as caller does it.
            return;
        }

        out.html("<div class='matsbm_heading'>");
        // ?: Is this the Global DLQ?
        if (matsBrokerDestination.isBrokerDefaultGlobalDlq()) {
            // -> Yes, global DLQ
            out.html("<h1>Examine Message from <i>Global DLQ</i></h1>\n");
        }
        else {
            // -> No, not the Global DLQ
            // ?: Is this a MatsStage Queue or DLQ?
            if (matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> Mats stage queue.
                String typeName = matsBrokerDestination.getStageDestinationType().orElse(UNKNOWN).getTypeName();
                out.html("<h1>Examine Message from <i><b>").DATA(typeName)
                        .html("</b></i> for <div class='matsbm_stageid'>")
                        .DATA(matsBrokerDestination.getMatsStageId().get()).html("</div></h1>");
            }
            else {
                // -> Non-Mats Queue. Not really supported, but just to handle it.
                out.html("<h1>Examine message from ").DATA(matsBrokerDestination.isDlq() ? "DLQ" : "Queue");
                out.html(" named ").DATA(matsBrokerDestination.getDestinationName()).html("</h1>");
            }
        }
        out.html("<br/>Fully Qualified Queue Name: <code><b>").DATA(matsBrokerDestination.getFqDestinationName())
                .html("</b></code><br>\n");
        out.html("</div>\n"); // /matsbm_heading

        MatsBrokerMessageRepresentation msgRepr = matsBrokerMessageRepresentationO.get();

        MatsTrace<?> matsTrace = null;
        int matsTraceDecompressedLength = 0;
        if ((matsSerializer != null)
                && msgRepr.getMatsTraceBytes().isPresent() && msgRepr.getMatsTraceMeta().isPresent()) {
            byte[] matsTraceBytes = msgRepr.getMatsTraceBytes().get();
            String matsTraceMeta = msgRepr.getMatsTraceMeta().get();
            DeserializedMatsTrace<?> deserializedMatsTrace = matsSerializer.deserializeMatsTrace(matsTraceBytes,
                    matsTraceMeta);
            matsTraceDecompressedLength = deserializedMatsTrace.getSizeDecompressed();
            matsTrace = deserializedMatsTrace.getMatsTrace();
        }

        // :: ACTION BUTTONS: REISSUE, MUTE, DELETE

        // ?: Is this a DLQ? (Both Normal and Muted DLQ)
        if (matsBrokerDestination.isDlq()) {
            // -> Yes, DLQ, so output Reissue button
            if (ac.reissueMessage(queueId)) {
                out.html("<button id='matsbm_reissue_single'"
                        + " class='matsbm_button matsbm_button_wider matsbm_button_reissue'"
                        + " onclick='matsbm_reissue_single(event,\"")
                        .DATA(queueId).html("\",\"").DATA(messageSystemId).html("\")'>"
                                + "Reissue <b>this message</b> [r]</button>");
            }

            // ?: Is this a NOT a Muted DLQ?
            if (ac.muteMessage(queueId) && matsBrokerDestination.getStageDestinationType().isPresent() &&
                    (matsBrokerDestination.getStageDestinationType()
                            .get() != StageDestinationType.DEAD_LETTER_QUEUE_MUTED)) {
                // -> No, normal DLQ, so output Mute button
                out.html("<button id='matsbm_mute_single'"
                        + " class='matsbm_button matsbm_button_wider matsbm_button_mute'"
                        + " onclick='matsbm_mute_single(event,\"")
                        .DATA(queueId).html("\",\"").DATA(messageSystemId).html("\")'>"
                                + "Mute <b>this message</b> [m]</button>");
            }
        }

        // Delete
        if (ac.deleteMessage(queueId)) {
            out.html("<button id='matsbm_delete_single'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete'"
                    + " onclick='matsbm_delete_single_propose(event)'>"
                    + "Delete <b>this message</b>... [d]</button>");

            out.html("<button id='matsbm_delete_single_cancel'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete_cancel matsbm_button_hidden'"
                    + " onclick='matsbm_delete_single_cancel(event)'>"
                    + "Cancel <b>Delete This</b> [Esc]</button>");

            out.html("<button id='matsbm_delete_single_confirm'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete matsbm_button_hidden'"
                    + " onclick='matsbm_delete_single_confirm(event,\"")
                    .DATA(queueId).html("\",\"").DATA(messageSystemId).html("\")'>"
                            + "Confirm <b>Delete This</b> [x]</button>");
        }

        List<ExamineMessageAddition> examineAdditions = monitorAdditions.stream()
                .filter(o -> o instanceof ExamineMessageAddition)
                .map(o -> (ExamineMessageAddition) o)
                .collect(Collectors.toList());
        for (ExamineMessageAddition examineAddition : examineAdditions) {
            out.html(examineAddition.convertMessageToHtml(msgRepr));
        }
        out.html("<span id='matsbm_action_message'></span>");
        out.html("</div>");
        out.html("<br>");

        // :: FLOW AND MESSAGE PROPERTIES

        part_FlowAndMessageProperties(out, msgRepr, matsTrace, matsTraceDecompressedLength);

        // :: DLQ Exception, if any

        // .. SVG-sprite: awesome-clone
        // (from font-awesome, via https://leungwensen.github.io/svg-icon/#awesome)
        out.html("<svg display='none'>\n"
                + "  <symbol viewBox='0 0 1792 1792' id='clone'>\n"
                + "    <path d='M1664 1632V544q0-13-9.5-22.5T1632 512H544q-13 0-22.5 9.5T512 544v1088q0 13 9.5 22.5"
                + "             t22.5 9.5h1088q13 0 22.5-9.5t9.5-22.5zm128-1088v1088q0 66-47 113t-113 47H544"
                + "             q-66 0-113-47t-47-113V544q0-66 47-113t113-47h1088q66 0 113 47t47 113zm-384-384v160"
                + "             h-128V160q0-13-9.5-22.5T1248 128H160q-13 0-22.5 9.5T128 160v1088q0 13 9.5 22.5t22.5 9.5"
                + "             h160v128H160q-66 0-113-47T0 1248V160Q0 94 47 47T160 0h1088q66 0 113 47t47 113z'/>"
                + "  </symbol>"
                + "</svg>");

        // .. SVG-sprite: awesome-check
        // (from font-awesome, via https://leungwensen.github.io/svg-icon/#awesome)
        out.html("<svg display='none'>\n"
                + " <symbol viewBox='0 0 1550 1188' id='check'>\n"
                + "   <path d='M1550 232q0 40-28 68l-724 724-136 136q-28 28-68 28t-68-28l-136-136L28 662Q0 634 0 594"
                + "            t28-68l136-136q28-28 68-28t68 28l294 295 656-657q28-28 68-28t68 28l136 136q28 28 28 68z'/>"
                + "  </symbol>"
                + "</svg>");

        part_DlqInformation(out, matsBrokerDestination.getStageDestinationType().orElse(UNKNOWN), msgRepr, matsTrace);

        // :: MATS TRACE! - do we have any?!

        if (matsTrace == null) {
            // -> No MatsTrace, why?
            if (msgRepr.getMatsTraceBytes().isPresent()) {
                // -> Seemingly because we don't have a MatsSerializer, and thus cannot deserialize the present bytes.
                out.html("<div id='matsbm_part_matstrace'>");
                out.html("<h2>NOTICE! There is a serialized MatsTrace byte array in the message, but I am"
                        + " constructed without a MatsSerializer, so I can't decipher it!</h2><br></div>\n");
            }
            else {
                // -> Evidently because there was no MatsTrace in the message.
                out.html("<div id='matsbm_part_matstrace'>");
                out.html("<h2>NOTICE! Missing MatsTrace information from the message, so cannot show"
                        + " call trace information!</h2><br></div>\n");
            }
        }
        else {
            // -> We do have a MatsTrace, output what we can!

            // :: INCOMING STATE AND MESSAGE

            part_StateAndMessage(out, matsTrace);

            // :: REPLY_TO STACK

            part_ReplyToStack(out, matsTrace);

            // :: MATS_TRACE ITSELF (all calls)

            part_MatsTrace(out, matsTrace, matsBrokerDestination.isDlq());

            // ?: Is this not a MatsTrace<String>? (Also checking for null, since currentCall.getData() may be null, but
            // still the MatsTrace is String)
            if (!((matsTrace.getCurrentCall().getData() == null)
                    || (matsTrace.getCurrentCall().getData() instanceof String))) {
                // -> No, not MatsTrace<String>: Okay then, toString() it.
                out.html("<h2>NOTICE: couldn't resolve MatsTrace to MatsTrace&lt;String&gt;!</h2><br>");
                out.html(" Here's matsTrace.toString() of the MatsTrace present:<br>\n");
                out.html("<pre>");
                out.html(matsTrace.toString().replace("<", "&lt;").replace(">", "&gt;"));
                out.html("</pre>");
            }

            // TODO: MISSING: SpanId-stack. (This is not yet publicly accessible).
        }

        // :: MATS_MESSAGE_REPRESENTATION.toString()

        out.html("<div id='matsbm_part_msgrepr_tostring'>");
        out.html("<h2>Raw broker message info</h2><br>\n");
        out.html("Here's matsMessageRepresentation.toString(), which should include the raw info from the broker:"
                + "<br><br>\n");
        out.html("<code>").DATA(msgRepr.toString()).html("</code>");
        out.html("</div>\n");

        // Call into JavaScript to set state for buttons etc.
        out.html("<script>matsbm_examine_message_view_loaded();</script>\n");

        // Don't output last </div>, as caller does it.
    }

    private static void part_FlowAndMessageProperties(Outputter out,
            MatsBrokerMessageRepresentation brokerMsg,
            MatsTrace<?> matsTrace,
            int matsTraceDecompressedLength) throws IOException {
        out.html("<div id='matsbm_part_flow_and_message_props'>");

        out.html("<table class='matsbm_table_flow_and_message'><tr>"); // start Flow/Message table
        out.html("<td>\n"); // start Flow information cell
        out.html("<h2>Flow information</h2>\n");

        // :: FLOW PROPERTIES
        out.html("<table class='matsbm_table_message_props'>");
        out.html("<thead>");
        out.html("<tr>");
        out.html("<th>Property</th>");
        out.html("<th>Value</th>");
        out.html("</tr>\n");
        out.html("</thead>");
        out.html("<tbody>");

        out.html("<tr>");
        out.html("<td>TraceId</td>");
        out.html("<td class='matsbm_table_browse_breakall'>").DATA(brokerMsg.getTraceId()).html("</td>");
        out.html("</tr>\n");

        String initiatingApp;
        String initiatorId;
        if (matsTrace != null) {
            initiatingApp = matsTrace.getInitiatingAppName() + "; v." + matsTrace.getInitiatingAppVersion();
            initiatorId = matsTrace.getInitiatorId();
        }
        else {
            initiatingApp = brokerMsg.getInitiatingApp();
            initiatorId = brokerMsg.getInitiatorId();
        }

        out.html("<tr><td>Initiating App @ Host</td>");
        out.html("<td>").DATA(initiatingApp);
        if (matsTrace != null) {
            out.html(" @ ").DATA(matsTrace.getInitiatingHost());
        }
        out.html("</td></tr>\n");

        out.html("<tr><td>Initiator Id</td>");
        out.html("<td>").DATA(initiatorId);
        out.html("</td></tr>\n");

        if (matsTrace != null) {
            out.html("<tr><td>Init debug info</td>");
            out.html("<td>").html(debugInfoToHtml(matsTrace.getDebugInfo()));
            out.html("</td></tr>\n");
        }

        if (matsTrace != null) {
            out.html("<tr><td>Mats Flow Id</td>");
            out.html("<td>").DATA(matsTrace.getFlowId());
            out.html("</td></tr>\n");
        }

        if (matsTrace != null) {
            out.html("<tr><td>Mats Flow Init Timestamp</td>");
            out.html("<td>").DATA(Statics.formatTimestampSpan(matsTrace.getInitiatingTimestamp()));
            out.html("</td></tr>\n");
        }

        if (matsTrace != null) {
            out.html("<tr><td>Parent Mats Message Id</td>");
            out.html("<td>");
            if (matsTrace.getParentMatsMessageId() != null) {
                out.DATA(matsTrace.getParentMatsMessageId());
            }
            else {
                out.html("<i>-no parent-</i>");
            }
            out.html("</td></tr>\n");
        }

        // .. MatsTrace props
        if (matsTrace != null) {
            out.html("<tr><td>&nbsp;&nbsp;KeepMatsTrace</td>");
            out.html("<td>").DATA(matsTrace.getKeepTrace().toString());
            out.html(" MatsTrace</td></tr>\n");
        }

        out.html("<tr><td>&nbsp;&nbsp;Persistent</td>");
        out.html("<td>").html(brokerMsg.isNonPersistent()
                ? "<b>Non-Persistent</b> <i>(non-default)</i>"
                : "Persistent <i>(default)</i>");
        out.html("</td></tr>\n");

        out.html("<tr><td>&nbsp;&nbsp;Interactive</td>");
        out.html("<td>").html(brokerMsg.isInteractive()
                ? "<b>Interactive</b> <i>(non-default)</i>"
                : "Non-Interactive <i>(default)</i>");
        out.html("</td></tr>\n");

        if (matsTrace != null) {
            out.html("<tr><td>&nbsp;&nbsp;TimeToLive</td>");
            out.html("<td>").html(matsTrace.getTimeToLive() == 0
                    ? "Live Forever <i>(default)</i>"
                    : "<b>" + matsTrace.getTimeToLive() + " ms</b> <i>(non-default)</i>");
            out.html("</td></tr>\n");
        }

        out.html("<tr><td>&nbsp;&nbsp;Audit</td>");
        out.html("<td>").html(brokerMsg.isNoAudit()
                ? "<b>No audit</b> <i>(non-default)</i>"
                : "Audit <i>(default)</i>");
        out.html("</td></tr>\n");

        out.html("</tbody>");
        out.html("</table>");

        out.html("</td>\n"); // end Flow information cell

        // :: MESSAGE PROPERTIES

        out.html("<td>\n"); // start Message information cell
        out.html("<h2>Message information (\"Current call\")</h2>");
        out.html("<table class=\"matsbm_table_message_props\">");
        out.html("<thead>");
        out.html("<tr>");
        out.html("<th>Property</th>");
        out.html("<th>Value</th>");
        out.html("</tr>\n");
        out.html("</thead>");
        out.html("<tbody>");

        out.html("<tr>");
        out.html("<td>Type</td>");
        out.html("<td>").DATA(brokerMsg.getDispatchType()).html(": ").DATA(brokerMsg.getMessageType()).html("</td>");
        out.html("</tr>\n");

        if (matsTrace != null) {
            out.html("<tr>");
            out.html("<td>From App @ Host</td>");
            if (matsTrace.getCallNumber() == 1) {
                out.html("<td><i>{initial call; same as 'Initiating App @ Host'}</i></td>");
            }
            else {
                out.html("<td>").DATA(matsTrace.getCurrentCall().getCallingAppName()
                        + "; v." + matsTrace.getCurrentCall().getCallingAppVersion())
                        .html(" @ ").DATA(matsTrace.getCurrentCall().getCallingHost())
                        .html("</td>");
            }
            out.html("</tr>\n");
        }

        out.html("<tr>");
        out.html("<td>From</td>");
        if ((matsTrace != null) && (matsTrace.getCallNumber() == 1)) {
            out.html("<td><i>{initial call; comes from 'Initiator Id'}</i></td>");
        }
        else {
            out.html("<td><div class='matsbm_stageid'>").DATA(brokerMsg.getFromStageId()).html("</div></td>");
        }
        out.html("</tr>\n");

        if (matsTrace != null) {
            out.html("<tr>");
            out.html("<td>Call debug info</td>");
            String debugInfo = matsTrace.getCurrentCall().getDebugInfo();
            // ?: Is this the initial call, and there is no info on the call (which is expected)
            if (((debugInfo == null) || (debugInfo.trim().isEmpty())) && (matsTrace.getCallNumber() == 1)) {
                // -> Yes, initial, and call.debugInfo missing: Use info from init.
                out.html("<td><i>{initial call; same as 'Init debug info'}</i></td>");
            }
            else {
                out.html("<td>").html(debugInfoToHtml(debugInfo)).html("</td>");
            }
            out.html("</tr>\n");
        }

        if (matsTrace != null) {
            out.html("<tr>");
            out.html("<td>Mats Message Id</td>");
            out.html("<td>").DATA(matsTrace.getCurrentCall().getMatsMessageId()).html("</td>");
            out.html("</tr>\n");
        }

        if (matsTrace != null) {
            out.html("<tr>");
            out.html("<td>Mats Message Timestamp</td>");
            out.html("<td>").DATA(Statics.formatTimestampSpan(matsTrace.getCurrentCall().getCalledTimestamp()))
                    .html("</td>");
            out.html("</tr>\n");
        }

        out.html("<tr>");
        out.html("<td>To (this)</td>");
        out.html("<td><div class='matsbm_stageid'>").DATA(brokerMsg.getToStageId()).html("</div></td>");
        out.html("</tr>\n");

        if (matsTrace != null) {
            out.html("<tr>");
            out.html("<td>Call number</td>");
            out.html("<td>#").DATA(Integer.toString(matsTrace.getCallNumber())).html(" in this flow, #")
                    .DATA(Integer.toString(matsTrace.getTotalCallNumber())).html(" counting parent flows")
                    .html("</td>");
            out.html("</tr>\n");
        }

        if (matsTrace != null) {
            out.html("<tr>");
            out.html("<td>MatsTrace Size</td>");
            String size = brokerMsg.getMatsTraceBytes().get().length == matsTraceDecompressedLength
                    ? matsTraceDecompressedLength + " bytes (not compressed)"
                    : brokerMsg.getMatsTraceBytes().get().length + " bytes compressed, "
                            + matsTraceDecompressedLength + " bytes decompressed";
            out.html("<td>").DATA(size).html("</td>");
            out.html("</tr>\n");
        }

        out.html("<tr>");
        out.html("<td>MsgSys Message Timestamp</td>");
        out.html("<td>").DATA(Statics.formatTimestampSpan(brokerMsg.getTimestamp())).html("</td>");
        out.html("</tr>\n");

        out.html("<tr>");
        out.html("<td>MsgSys Message Id</td>");
        out.html("<td>").DATA(brokerMsg.getMessageSystemId()).html("</td>");
        out.html("</tr>\n");

        out.html("<tr>");
        out.html("<td>MsgSys Expires</td>");
        out.html("<td>").html(brokerMsg.getExpirationTimestamp() == 0
                ? "Never expires"
                : (brokerMsg.getExpirationTimestamp() - System.currentTimeMillis() < 0
                        ? "<b><span style='color:red'>Expired!</span></b> "
                        : "") + (Statics.formatTimestampSpan(brokerMsg.getExpirationTimestamp())));
        out.html("</td>");
        out.html("</tr>");

        out.html("</tbody>");
        out.html("</table>");

        out.html("</td>\n"); // end Message information cell
        out.html("</tr></table>"); // end Flow/Message table

        out.html("</div>");
    }

    private static void part_StateAndMessage(Outputter out, MatsTrace<?> matsTrace)
            throws IOException {
        out.html("<div id='matsbm_part_state_and_message'>\n");
        out.html("<h2>Incoming State and Message</h2><br>\n");
        // State:
        out.html("<div class='matsbm_box_call_or_state'>\n");
        Optional<? extends StackState<?>> currentStateO = matsTrace.getCurrentState();
        if (currentStateO.isPresent()) {
            out.html("Incoming <b>State</b>: ");
            out_displaySerializedRepresentation(out, currentStateO.get().getState());
        }
        else {
            out.html("<i>-no incoming state-</i>");
        }
        out.html("</div><br>\n");

        // Message:
        out.html("<div class='matsbm_box_call_or_state'>\n");
        out.html("Incoming <b>Message</b>: ");
        out_displaySerializedRepresentation(out, matsTrace.getCurrentCall().getData());
        out.html("</div></div>\n");
    }

    private static void part_DlqInformation(Outputter out, StageDestinationType stageDestinationType,
            MatsBrokerMessageRepresentation brokerMsg, MatsTrace<?> matsTrace)
            throws IOException {
        // These three properties are added by Mats3 when performing 'Mats Managed DLQ Divert', and they are cleared
        // on reissue, implying that they shall only be present on the message when it is Mats3 that has just DLQed it.
        // (This does not hold for the DLQ count, which is a counter that is increased each time the message is
        // reissued and then DLQed again - and it is thus NOT cleared upon reissue.)
        boolean hasDlqInformation = brokerMsg.getDlqDeliveryCount().isPresent()
                || brokerMsg.getDlqStageOrigin().isPresent()
                || brokerMsg.getDlqAppVersionAndHost().isPresent();
        // ?: Do we have any DLQ information on the message?
        if (!hasDlqInformation) {
            // -> No DLQ information on message, but is this still a DLQ queue?
            if (stageDestinationType.isDlq()) {
                // -> Yes, this is a DLQ queue - so point out that there's no DLQ information.
                out.html("<div id='matsbm_part_dlq_information'>\n");
                out.html("<h2>DLQ Information</h2><br>\n");
                out.html("<i>-no DLQ information on message, even though this is a Dead Letter Queue-</i><br>\n");
                out.html("<br>This probably means that the message was DLQ'ed by the message broker, and not on the"
                        + " client side employing <i>'Mats Managed DLQ Divert'</i>.<br/>\n");
                if ((matsTrace != null) && (matsTrace.getTimeToLive() > 0)) {
                    out.html("<br><b>There's good reasons to believe that it timed out while on queue</b>, since"
                            + " the Time-To-Live was " + matsTrace.getTimeToLive() + " ms, and this message"
                            + " now resides on a DLQ without any DLQ information.<br>\n");
                }
                out.html("</div>\n");
            }
            return;
        }

        out.html("<div id='matsbm_part_dlq_information'>\n");
        out.html("<h2>DLQ Information</h2><br>\n");

        // ?: Do we have App and Version? (This SHALL be present if we have any DLQ information, but hey ho)
        if (brokerMsg.getDlqAppVersionAndHost().isPresent()) {
            String appVersionAndHostname = brokerMsg.getDlqAppVersionAndHost().get();
            // Split the string into three, app, version and hostname, by first semicolon, then "@"
            String[] parts = appVersionAndHostname.split("[;@]", 3);
            String app = parts[0];
            String version = parts[1];
            String hostname = parts[2];

            out.html("&nbsp;&nbsp;DLQ'ed by Stage: <div class='matsbm_stageid'>")
                    .DATA(brokerMsg.getToStageId());
            out.html("</div><br/>&nbsp;&nbsp;Application: <b>").DATA(app).html("</b> v.<b>")
                    .DATA(version);
            out.html("</b><br/>&nbsp;&nbsp;Running on Host: <b>").DATA(hostname).html("</b><br><br>\n");
        }

        // ?: Is this a refused message? (I.e. the message was refused by the stage, by throwing
        // MatsMessageRefuseException)
        // (Note: This will only be present if stacktrace is also present..)
        if (brokerMsg.isDlqMessageRefused().isPresent()) {
            if (brokerMsg.isDlqMessageRefused().get()) {
                out.html("&nbsp;&nbsp;<b>Message was refused by the Stage!</b>"
                        + " &nbsp;&nbsp;<i>(Raised <code>MatsRefuseMessageException</code>)</i><br><br>\n");
            }
            else if (brokerMsg.getDlqDeliveryCount().isPresent() && (brokerMsg.getDlqDeliveryCount().get() > 1)) {
                out.html("&nbsp;&nbsp;<b>Message failed delivery multiple times, and thus DLQ'ed!</b>"
                        + " &nbsp;&nbsp;<i>(Processing raised some Exception on every attempt)</i><br><br>\n");
            }
            else {
                out.html("&nbsp;&nbsp;<b>Reason for DLQ isn't conclusive!</b>"
                        + " - you should probably check logs.<br><br>\n");
            }
        }

        // ?: Are we missing the reason Exception?
        if (brokerMsg.getDlqExceptionStacktrace().isEmpty()) {
            // -> Yes, missing Exception, which implies that the message was DLQed on the receive side, before
            // processing started.
            out.html("&nbsp;&nbsp;<span style='color: red;'><b>Missing reason for DLQ!</b></span> This implies that the"
                    + " DLQ was performed on the reception side of the processing (before processing started).<br>\n"
                    + "&nbsp;&nbsp;This is a fallback mechanism in case the normal DLQ handling fails: If Stage"
                    + " processing is finished and Mats3 has started to send any outgoing messages, but then<br>\n"
                    + "&nbsp;&nbsp;get into problems with the sending, or with committing of database, it cannot"
                    + " anymore divert the message to DLQ even though the delivery count has reached max.<br>\n"
                    + "&nbsp;&nbsp;Mats3 thus needs to rollback, which results in a new redelivery. Mats3 then"
                    + " catches the exceeded delivery count on the receive side and thus directly divert to DLQ.<br>\n"
                    + "&nbsp;&nbsp;<b>You will need to check logs!</b><br><br>\n");
        }

        // ?: Do we have a delivery count? (This SHALL be present if we have any DLQ information, but hey ho)
        if (brokerMsg.getDlqDeliveryCount().isPresent()) {
            out.html("&nbsp;&nbsp;Attempted deliveries: <b>").DATA(Integer.toString(brokerMsg
                    .getDlqDeliveryCount().get())).html("</b> &nbsp;&nbsp;&nbsp;<i>(How many times this message was"
                            + " attempted delivered to the Stage.)</i><br>\n");
        }

        // ?: Do we have a DLQ count? (This SHALL be present if we have any DLQ information, but hey ho)
        if (brokerMsg.getDlqCount().isPresent()) {
            out.html("&nbsp;&nbsp;DLQ count: <b>").DATA(Integer.toString(brokerMsg.getDlqCount().get()))
                    .html("</b> &nbsp;&nbsp;&nbsp;<i>(How many times this message has ended up on DLQ -"
                            + " will increase after reissue and subsequent new DLQ again!)</i><br>\n");
        }

        // ?: Do we have a Last Reissued Username? (Added by MatsBrokerMonitor on reissue, thus only present if
        // already reissued)
        if (brokerMsg.getDlqLastOperationUsername().isPresent()) {
            boolean muted = stageDestinationType == StageDestinationType.DEAD_LETTER_QUEUE_MUTED;
            out.html("&nbsp;&nbsp;" + (muted ? "Muted" : "Last reissued") + " by: <b>")
                    .DATA(brokerMsg.getDlqLastOperationUsername().get())
                    .html("</b> &nbsp;&nbsp;&nbsp;<i>(The user which used MatsBrokerMonitor to " + (muted
                            ? "move this message from the DLQ to the Muted DLQ"
                            : "reissue this message from the DLQ last time.") + ")</i><br>\n");
        }

        // ?: Do we have a Stage Origin? (This SHALL be present if we have any DLQ information, but hey ho)
        if (brokerMsg.getDlqStageOrigin().isPresent()) {
            out.html("<br/><div class='matsbm_box_call_or_state'>\n"
                    + "<b>Stage Origin</b> (\"Debug Info\"):\n"
                    + "<div class='matsbm_box_call_or_state_div'>");

            String debugInfo = brokerMsg.getDlqStageOrigin().get();
            if (debugInfo.trim().isEmpty()) {
                debugInfo = "{none present}";
            }
            debugInfo = Outputter.ESCAPE(debugInfo).replace(";", "\n");
            if (brokerMsg.getDlqAppVersionAndHost().isPresent()) {
                String appVersionAndHostname = brokerMsg.getDlqAppVersionAndHost().get();
                String[] parts = appVersionAndHostname.split("[;@]", 3);
                String app = parts[0];
                String version = parts[1];
                String hostname = parts[2];
                debugInfo = "<i><b>" + app + "</b> v.<b>" + version + "</b> @ <b>" + hostname + "</b></i>\n"
                        + debugInfo;
            }
            out.html(debugInfo);
            out.html("</div></div><br>\n");
        }

        // ?: Do we have a stacktrace?
        if (brokerMsg.getDlqExceptionStacktrace().isPresent()) {
            // -> Yes, we have a stacktrace
            out.html("<br/><div class='matsbm_box_call_or_state'>\n");
            out.html("Final <b>Exception</b> which caused the DLQ:");
            out.html("<div class='matsbm_box_call_or_state_div'>");
            out.DATA(brokerMsg.getDlqExceptionStacktrace().get());
            out.html("</div></div><br>\n");
        }
        out.html("</div>\n");
    }

    private static void part_ReplyToStack(Outputter out, MatsTrace<?> matsTrace) throws IOException {
        out.html("<div id='matsbm_part_stack'>");
        out.html("<h2>ReplyTo Stack</h2><br>\n");

        out.html("Current ReplyTo stack (frames below us):<br>");
        List<Channel> stack = matsTrace.getCurrentCall().getReplyStack();
        if (stack.isEmpty()) {
            out.html("<h3><i>stack is empty</i></h3> (terminator-level, cannot reply)<br>");
        }
        else {
            List<? extends StackState<?>> stateStack = matsTrace.getStateStack();
            out.html("<table class='matsbm_table_replytostack'>");
            out.html("<thead><tr>");
            out.html("<th>Height</th>");
            out.html("<th>ReplyTo</th>");
            out.html("<th>State</th>");
            out.html("</tr></thead>");

            out.html("<tbody>");
            for (int i = stack.size() - 1; i >= 0; i--) {
                Channel channel = stack.get(i);
                out.html("<tr>");
                out.html("<td>").DATA(Integer.toString(i));
                out.html("<td>").DATA(channel.getMessagingModel().toString()).html(": ").DATA(channel.getId())
                        .html("</td>\n");
                out.html("<td><div class='matsbm_box_call_or_state'>\n");
                out_displaySerializedRepresentation(out, stateStack.get(i).getState());
                out.html("</div></td>\n");
                out.html("</tr>\n");
            }
            out.html("</tbody></table>");
        }
        out.html("</div>");
    }

    private static void part_MatsTrace(Outputter out, MatsTrace<?> matsTrace, boolean isDlq) throws IOException {
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

        List<? extends Call<?>> callFlow = matsTrace.getCallFlow();
        List<? extends StackState<?>> stateFlow = matsTrace.getStateFlow();

        // :: Run through the actual state flow, and build a string of the heights
        StringBuilder actualStateHeightsFromStateFlow = new StringBuilder();
        stateFlow.forEach(stackState -> actualStateHeightsFromStateFlow.append(stackState.getHeight()).append(':'));

        // :: Run through the call flow, and build two strings of the heights: One for the case where we assume
        // initialState, and one for the case where we assume normal (no initialState).
        StringBuilder stateHeightsFromCallFlow_normal = new StringBuilder();
        StringBuilder stateHeightsFromCallFlow_initialState = new StringBuilder();
        boolean initiationIsSend = callFlow.get(0).getCallType() == CallType.SEND;
        for (int i = 0; i < callFlow.size(); i++) {
            // ?: For the case w/ initialState: Are we BEFORE the initial call which is a SEND?
            if ((i == 0) && initiationIsSend) {
                // -> Yes, before initial call which was SEND.
                // A SEND call does NOT add state itself, since there is no replyTo / no terminator to receive it.
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
            // ?: For the case w/ initialState: Are we AFTER the initial call which is a REQUEST?
            if ((i == 0) && (!initiationIsSend)) {
                // -> Yes, after initial call which was REQUEST.
                // A REQUEST adds a state at level 0 for the message to the replyTo / terminator.
                // Since we assume initialState, it also added initialState at level 1 for targeted endpoint.
                // This latter we add here, since it isn't represented in the call flow.
                stateHeightsFromCallFlow_initialState.append("1:");
            }
        }

        // :: Compare the actual state flow with the two state flows we deduced from the call flow
        int initialStateStatus; // 1:normal, 2:initialState, -1:neither (can't deduce, can't resolve states)
        // ?: Was this a /normal/ flow, without initialState from initiation?
        if (actualStateHeightsFromStateFlow.toString().contentEquals(stateHeightsFromCallFlow_normal)) {
            // -> Yes, /normal/ flow
            initialStateStatus = 1;
        }
        // ?: Was this an initiation with initialState to the called endpoint?
        else if (actualStateHeightsFromStateFlow.toString().contentEquals(stateHeightsFromCallFlow_initialState)) {
            // -> Yes, initiation with initialState.
            initialStateStatus = 2;
        }
        else {
            // We don't comprehend the state flow, so we can't pretend to display it sanely.
            initialStateStatus = -1;
        }

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

        out.html("<div id='matsbm_part_matstrace'>");

        out.html("<h2>MatsTrace</h2><br>\n");
        if (matsTrace.getKeepTrace() != KeepMatsTrace.MINIMAL) {
            out.html("<b>Remember that the MatsTrace, and the rows in this table, refers to the <i>calls, i.e. the"
                    + " messages from one stage to the next in a flow</i>, not the processing on the stages"
                    + " themselves.</b><br>\n");
            out.html("Thus, it is the REQUEST, REPLY and NEXT rows (the calls) in the table that are the real info"
                    + " carriers - the <i>\"Processed on\"</i> rows are synthesized with stageId taken from the previous"
                    + " call's \"to\", and the <i>app/host</i> and <i>DebugInfo</i> from current call -"
                    + " just to aid your intuition.<br>\n");
        }
        else {
            out.html("<b>NOTICE!! This Mats Flow was initiated with KeepTrace.MINIMAL, which removes all information"
                    + " about all calls other than the current to save space.</b><br>");
            out.html("<i>(hopefully you've got massive increased performance in exchange for"
                    + " this considerably reduced debuggability!)</i><br>\n");
            out.html("You'll have to use your centralized logging system and search with either the FlowId or TraceId"
                    + " to piece together what lead up to the current call.<br>\n");
        }

        // .. SVG-sprite: Arrow down (slanted and colored using CSS)
        // (from font-awesome, via https://leungwensen.github.io/svg-icon/#awesome)
        out.html("<svg display='none'>\n"
                + "  <symbol viewBox='0 0 1558 1483' id='arrow-down'>\n"
                + "    <path d='M1558 704q0 53-37 90l-651 652q-39 37-91 37-53 0-90-37L38 794Q0 758 0 704q0-53"
                + "             38-91l74-75q39-37 91-37 53 0 90 37l294 294V128q0-52 38-90t90-38h128q52 0 90 38t38"
                + "             90v704l294-294q37-37 90-37 52 0 91 37l75 75q37 39 37 91z'/>"
                + "  </symbol>"
                + "</svg>");

        int highestStackHeight = 0;
        for (Call<?> currentCall : callFlow) {
            highestStackHeight = Math.max(highestStackHeight, currentCall.getReplyStackHeight());
        }

        out.html("<table class='matsbm_table_matstrace' id='matsbm_table_matstrace'>");
        out.html("<thead>");
        out.html("<tr>");
        out.html("<th>Call#</th>");
        out.html("<th>time</th>");
        out.html("<th>diff</th>");
        out.html("<th colspan='").DATA(highestStackHeight + 1).html("'>Call/Processing</th>");
        out.html("<th>Application</th>");
        out.html("<th>Host</th>");
        out.html("<th>DebugInfo</th>");
        out.html("</tr>");
        out.html("</thead>");

        // :: Flow
        out.html("<tbody>");

        // :: MatsTrace's Initiation
        out.html("<tr class='call'>");
        out.html("<td>#0</td>");
        out.html("<td>0 ms</td>");
        out.html("<td></td>");
        if (matsTrace.getKeepTrace() != KeepMatsTrace.MINIMAL) {
            // -> KeepTrace.FULL or KeepTrace.COMPACT
            out.html("<td colspan=100>");
            out.html("INIT<br>from: ").DATA(matsTrace.getInitiatorId());
            out.html("</td>");
        }
        else {
            // -> KeepTrace.MINIMAL
            out.html("<td colspan='").DATA(highestStackHeight + 1).html("'>INIT<br>from: ")
                    .DATA(matsTrace.getInitiatorId()).html("</td\n>");
            out.html("<td>").DATA(matsTrace.getInitiatingAppName())
                    .html("; v.").DATA(matsTrace.getInitiatingAppVersion()).html("</td>\n");
            out.html("<td>").DATA(matsTrace.getInitiatingHost()).html("</td>\n");
            out.html("<td>").html(debugInfoToHtml(matsTrace.getDebugInfo())).html("</td>\n");
        }
        out.html("</tr>\n");

        // :: IF we're in MINIMAL mode, output empty rows to represent the missing calls.
        if (matsTrace.getKeepTrace() == KeepMatsTrace.MINIMAL) {
            int currentCallNumber = matsTrace.getCallNumber();
            for (int i = 0; i < currentCallNumber - 1; i++) {
                out.html("<tr class='processing'><td colspan=100><div style='height: 1pt;'></div></td></tr>");
                out.html("<tr class='call'><td colspan=100><div style='height: 2pt;'></div></td></tr>");
            }
        }

        // :: MATSTRACE Calls table

        long initializedTimestamp = matsTrace.getInitiatingTimestamp();
        // NOTE: If we are KeepMatsTrace.MINIMAL, then there is only 1 entry here
        String prevIndent = "";
        int prevIndentLevel = 0;
        long previousCalledTimestamp = matsTrace.getInitiatingTimestamp();
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            // If there is only one call, then it is either first, or MINIMAL and last.
            // If it is first, then both matsTrace.getCallNumber() and 'i+1' == 1
            int currentCallNumber = callFlow.size() == 1 ? matsTrace.getCallNumber() : i + 1;
            // Can we get a prevCall?
            Call<?> prevCall = (i == 0)
                    ? null
                    : callFlow.get(i - 1);

            StringBuilder indentBuf = new StringBuilder("");
            int indentLevel = currentCall.getReplyStackHeight();
            for (int x = 0; x < indentLevel; x++) {
                indentBuf.append("<td class='indent'><div class='matsbm_line'></div></td>");
            }
            String indent = indentBuf.toString();

            // :: PROCESSING row
            out.html("<tr class='processing' id='matsbm_processrow_").DATA(currentCallNumber).html("'"
                    + " onmouseover='matsbm_hover_call(event)' onmouseout='matsbm_hover_call_out(event)' data-callno='")
                    .DATA(currentCallNumber).html("'>");
            out.html("<td></td>");
            out.html("<td></td>");
            out.html("<td></td>");
            if (matsTrace.getKeepTrace() != KeepMatsTrace.MINIMAL) {
                // -> KeepTrace.FULL or KeepTrace.COMPACT
                out.html(prevIndent).html("<td onclick='matsbm_callmodal(event)' colspan='").DATA(highestStackHeight
                        - prevIndentLevel + 1).html("'>");
                out.html("<i>Processed&nbsp;on&nbsp;</i>");
                prevIndent = indent;
                prevIndentLevel = indentLevel;
                if (prevCall != null) {
                    out.html(prevCall.getTo().getId());
                }
                else {
                    out.html("Initiation");
                }
            }
            else {
                // -> KeepTrace.MINIMAL
                out.html(indent).html("<td onclick='matsbm_callmodal(event)' colspan='").DATA(highestStackHeight
                        - indentLevel + 1).html("'>");
                out.html("<br>Processed on<br>");
            }

            out.html("<br>\n");
            out.html("</td>");
            out.html("<td>@");
            if (currentCallNumber > 1) {
                out.DATA(currentCall.getCallingAppName())
                        .html("; v.").DATA(currentCall.getCallingAppVersion());
            }
            else {
                out.DATA(matsTrace.getInitiatingAppName())
                        .html("; v.").DATA(matsTrace.getInitiatingAppVersion());
            }
            out.html("</td><td>@");
            if (currentCallNumber > 1) {
                out.DATA(currentCall.getCallingHost());
            }
            else {
                out.DATA(matsTrace.getInitiatingHost());
            }
            out.html("</td><td class='matsbm_from_info'>");
            if (currentCallNumber > 1) {
                out.html(debugInfoToHtml(currentCall.getDebugInfo()));
            }
            else {
                out.html(debugInfoToHtml(matsTrace.getDebugInfo()));
            }
            out.html("</td>");
            out.html("</tr>");

            // :: CALL row
            out.html("<tr class='call' id='matsbm_callrow_").DATA(currentCallNumber).html("'"
                    + " onmouseover='matsbm_hover_call(event)' onmouseout='matsbm_hover_call_out(event)' data-callno='")
                    .DATA(currentCallNumber).html("'>");
            out.html("<td>#");
            out.html(Integer.toString(currentCallNumber));
            out.html("</td>");
            long currentCallTimestamp = currentCall.getCalledTimestamp();
            out.html("<td>").DATA(currentCallTimestamp - initializedTimestamp)
                    .html("&nbsp;ms</td>");
            long diffBetweenLast = currentCallTimestamp - previousCalledTimestamp;
            previousCalledTimestamp = currentCallTimestamp;
            out.html("<td>").DATA(diffBetweenLast).html("&nbsp;ms</td>"); // Proc

            out.html(indent).html("<td onclick='matsbm_callmodal(event)' colspan='")
                    .DATA(highestStackHeight - indentLevel + 5).html("'>");
            String callType;
            switch (currentCall.getCallType()) {
                case REQUEST:
                    callType = "<svg class='matsbm_arrow_req'><use xlink:href=\"#arrow-down\" /></svg>";
                    break;
                case REPLY:
                    callType = "<svg class='matsbm_arrow_rep'><use xlink:href=\"#arrow-down\" /></svg>";
                    break;
                case NEXT:
                    callType = "<svg class='matsbm_arrow_next'><use xlink:href=\"#arrow-down\" /></svg>";
                    break;
                case GOTO:
                    callType = "<svg class='matsbm_arrow_goto'><use xlink:href=\"#arrow-down\" /></svg>";
                    break;
                case SEND: // For initiations only, both SEND and PUBLISH.
                    callType = "<svg class='matsbm_arrow_send'><use xlink:href=\"#arrow-down\" /></svg>";
                    break;
                default:
                    callType = "";
            }
            callType += " this is a " + currentCall.getCallType();
            out.html(callType).html(" call");
            StackState<?> stackState = callToState.get(currentCall);
            if (stackState != null) {
                out.html(i == 0 ? " w/ initial state" : " w/ state");
            }
            out.html(" - <a href='//show call' onclick='matsbm_noclick(event)'>show</a>");
            out.html("<br>");

            out.html("<i>to:</i>&nbsp;").DATA(currentCall.getTo().getId());
            out.html("</td>");

            out.html("</tr>");
        }
        // If DLQ, then explanation-row about crashed processing
        if (isDlq) {
            out.html("<tr class='matsbm_table_matstrace_dlqrow'>");
            out.html("<td colspan='").DATA(highestStackHeight + 7)
                    .html("' style='width: 1200px; text-align: center;'>This message resides on a DLQ, which means"
                            + " that the final call failed processing on <div class='matsbm_stageid'>")
                    .DATA(matsTrace.getCurrentCall().getTo().getId())
                    .html("</div><br>Either you have some good info above in the 'DLQ information' section above, or"
                            + " you'll have to use your logging system to understand what happened there.</td>");
            out.html("</tr>");
        }

        out.html("</table>");

        // :: MODALS: Calls and optionally also state

        // The "modal underlay", i.e. "gray out" - starts out 'display: none', visible if modal is showing.
        out.html("<div id='matsbm_callmodalunderlay' class='matsbm_callmodalunderlay'"
                + " onclick='matsbm_clearcallmodal(event)'>");

        String previousTo = "Initiation";
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            // If there is only one call, then it is either first, or MINIMAL and last.
            // If it is first, then both matsTrace.getCallNumber() and 'i+1' == 1
            int currentCallNumber = callFlow.size() == 1 ? matsTrace.getCallNumber() : i + 1;
            out.html("<div class='matsbm_box_call_and_state_modal' id='matsbm_callmodal_")
                    .DATA(currentCallNumber).html("'>\n");
            String from = matsTrace.getKeepTrace() == KeepMatsTrace.MINIMAL
                    ? currentCall.getFrom()
                    : previousTo;
            String appAndVer = currentCallNumber == 1
                    ? matsTrace.getInitiatingAppName() + "; v." + matsTrace.getInitiatingAppVersion()
                    : currentCall.getCallingAppName() + "; v." + currentCall.getCallingAppVersion();
            String host = currentCallNumber == 1
                    ? matsTrace.getInitiatingHost()
                    : currentCall.getCallingHost();
            String debugInfoHtml = currentCallNumber == 1
                    ? debugInfoToHtml(matsTrace.getDebugInfo())
                    : debugInfoToHtml(currentCall.getDebugInfo());
            out.html("<i>(Arrows \u2b06 and \u2b07 to navigate, Esc to exit)</i><br>\n");
            out.html("<div class='matsbm_box_call_and_state_callinfo'>This is a message from <b>").DATA(from)
                    .html("</b><br>on application <b>").DATA(appAndVer)
                    .html("</b><br>running on node <b>").DATA(host)
                    .html("</b></div><br><div class='matsbm_box_call_and_state_debuginfo'>").html(debugInfoHtml);
            out.html("</div><br>.. and it is a<br>\n");
            out.html("<h3>").DATA(currentCall.getCallType())
                    .html(" call to <b>").DATA(currentCall.getTo().getId())
                    .html("</b></h3><br>\n");
            previousTo = currentCall.getTo().getId();
            // State:
            if ((matsTrace.getKeepTrace() == KeepMatsTrace.FULL)
                    || (i == (callFlow.size() - 1))) {
                out.html("<div class='matsbm_box_call_or_state'>\n");

                Object state;
                // ?: Is this the last call?
                if (i == (callFlow.size() - 1)) {
                    // -> Yes, so fetch state from current call
                    Optional<? extends StackState<?>> currentStateO = matsTrace.getCurrentState();
                    state = currentStateO.<Object> map(StackState::getState).orElse(null);
                }
                else {
                    // -> No, so fetch state from the callToState map
                    StackState<?> stackState = callToState.get(currentCall);
                    state = stackState != null ? stackState.getState() : null;
                }
                if (state != null) {
                    out.html("Incoming state: ");
                    out_displaySerializedRepresentation(out, state);
                }
                else {
                    out.html("<i>-no incoming state-</i>");
                }
                out.html("</div><br>\n");

                // Message:
                out.html("<div class='matsbm_box_call_or_state'>\n");
                out.html("Incoming message: ");
                out_displaySerializedRepresentation(out, currentCall.getData());
                out.html("</div><br>\n");
            }
            else {
                out.html("<i><b>(Not KeepTrace.FULL, so state and message is only present on current"
                        + " (last) call)</b></i>");
            }

            out.html("</div><br>\n");
        }
        out.html("</div>");

        out.html("</div>");
    }

    /**
     * If String, try to display as JSON, if not just raw. If byte, just display array size.
     */
    private static void out_displaySerializedRepresentation(Outputter out, Object data) throws IOException {
        if (data == null) {
            out.html("<i>null</i><br>\n");
            out.html("<div class='matsbm_box_call_or_state_div'><i>null</i></div>\n");
        }
        else if (data instanceof String) {
            String stringData = (String) data;
            out.html("String[").DATA(stringData.length()).html(" chars]<br>\n");

            try {
                String jsonData = new ObjectMapper().readTree(stringData).toPrettyString();
                out.html("<div class='matsbm_box_call_or_state_div'>").DATA(jsonData).html("</div>\n");
            }
            catch (JsonProcessingException e) {
                out.html("Couldn't parse String as json (thus no pretty printing),"
                        + " so here it is unparsed.<br>");
                out.html("<div class='matsbm_box_call_or_state_div'>").DATA(stringData).html("</div>\n");
            }
        }
        else if (data instanceof byte[]) {
            byte[] byteData = (byte[]) data;
            out.html("byte[").DATA(byteData.length).html(" bytes]<br>\n");
        }
    }

    private static String debugInfoToHtml(String debugInfo) {
        if ((debugInfo == null) || (debugInfo.trim().isEmpty())) {
            debugInfo = "{none present}";
        }
        debugInfo = Outputter.ESCAPE(debugInfo).replace(";", "<br>\n");
        return "<code class='matsbm_table_browse_breakall'>" + debugInfo + "</code>";
    }
}
