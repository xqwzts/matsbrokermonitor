package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.serial.MatsSerializer;
import io.mats3.serial.MatsSerializer.DeserializedMatsTrace;
import io.mats3.serial.MatsTrace;
import io.mats3.serial.MatsTrace.Call;
import io.mats3.serial.MatsTrace.Call.CallType;
import io.mats3.serial.MatsTrace.KeepMatsTrace;
import io.mats3.serial.MatsTrace.StackState;

/**
 * @author Endre Stølsvik 2022-03-13 23:33 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ExamineMessage {

    static void gui_ExamineMessage(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions, MatsSerializer<?> matsSerializer,
            Appendable out, String destinationId, String messageSystemId) throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.append("<div class='matsbm_report matsbm_examine_message'>\n");
        out.append("<div class='matsmb_actionbuttons'>\n");
        out.append("<a href='?'>Back to Broker Overview</a><br />\n");
        out.append("<a href='?browse&destinationId=").append(destinationId)
                .append("'>Back to Queue</a> - ");

        String queueId = destinationId.substring("queue:".length());
        out.append(queueId).append("<br />\n");

        Optional<MatsBrokerMessageRepresentation> matsBrokerMessageRepresentationO = matsBrokerBrowseAndActions
                .examineMessage(queueId, messageSystemId);
        if (!matsBrokerMessageRepresentationO.isPresent()) {
            out.append("<h1>No such message!</h1><br/>\n");
            out.append("MessageSystemId: [" + messageSystemId + "].<br/>\n");
            out.append("Queue:[" + queueId + "]<br/>\n");
            out.append("</div>");
            out.append("</div>");
            return;
        }

        MatsBrokerMessageRepresentation matsMsg = matsBrokerMessageRepresentationO.get();

        MatsTrace<?> matsTrace = null;
        int matsTraceDecompressedLength = 0;
        if ((matsSerializer != null)
                && matsMsg.getMatsTraceBytes().isPresent() && matsMsg.getMatsTraceMeta().isPresent()) {
            byte[] matsTraceBytes = matsMsg.getMatsTraceBytes().get();
            String matsTraceMeta = matsMsg.getMatsTraceMeta().get();
            DeserializedMatsTrace<?> deserializedMatsTrace = matsSerializer.deserializeMatsTrace(matsTraceBytes,
                    matsTraceMeta);
            matsTraceDecompressedLength = deserializedMatsTrace.getSizeDecompressed();
            matsTrace = deserializedMatsTrace.getMatsTrace();
        }

        // :: ACTION BUTTONS

        out.append("<div class='matsmb_button matsmb_button_reissue'"
                + " onclick='matsmb_reissue_single(event, \"" + queueId + "\")'>"
                + "Reissue [R]</div>");
        out.append("<div id='matsmb_delete_single' class='matsmb_button matsmb_button_delete'"
                + " onclick='matsmb_delete_propose_single(event)'>"
                + "Delete [D]</div>");
        out.append("<div id='matsmb_delete_cancel_single'"
                + " class='matsmb_button matsmb_button_delete_cancel matsmb_button_hidden'"
                + "onclick='matsmb_delete_cancel_single(event)'>"
                + "Cancel Delete [Esc]</div>");
        out.append("<div id='matsmb_delete_confirm_single'"
                + " class='matsmb_button matsmb_button_delete matsmb_button_hidden'"
                + " onclick='matsmb_delete_confirmed_single(event, \"" + queueId + "\")'>"
                + "Confirm Delete [X]</div>");
        out.append("</div>");
        out.append("<br/>");

        // :: FLOW AND MESSAGE PROPERTIES

        examineMessage_FlowAndMessageProperties(out, matsMsg, matsTrace, matsTraceDecompressedLength);

        // :: Incoming message and state

        Optional<MatsTrace<String>> stringMatsTraceO = getStringMatsTrace(matsTrace);

        if (stringMatsTraceO.isPresent()) {
            examineMessage_currentCallMatsTrace(out, stringMatsTraceO.get());
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

    private static void examineMessage_FlowAndMessageProperties(Appendable out,
            MatsBrokerMessageRepresentation brokerMsg,
            MatsTrace<?> matsTrace,
            int matsTraceDecompressedLength) throws IOException {
        out.append("<table class='matsbm_table_flow_and_message'><tr>"); // start Flow/Message table
        out.append("<td>\n"); // start Flow information cell
        out.append("<h2>Flow information</h2>\n");

        // :: FLOW PROPERTIES
        out.append("<table class='matsbm_table_message_props'>");
        out.append("<thead>");
        out.append("<tr>");
        out.append("<th>Property</th>");
        out.append("<th>Value</th>");
        out.append("</tr>\n");
        out.append("</thead>");
        out.append("<tbody>");

        out.append("<tr>");
        out.append("<td>TraceId</td>");
        out.append("<td>").append(brokerMsg.getTraceId()).append("</td>");
        out.append("</tr>\n");

        String initializingApp = "{no info present}";
        String initiatorId = "{no info present}";
        if (matsTrace != null) {
            initializingApp = matsTrace.getInitializingAppName() + "; v." + matsTrace.getInitializingAppVersion();
            initiatorId = matsTrace.getInitiatorId();
        }
        // ?: Do we have InitializingApp from MsgSys?
        // TODO: Remove this "if" in 2023.
        else if (brokerMsg.getInitializingApp() != null) {
            initializingApp = brokerMsg.getInitializingApp();
            initiatorId = brokerMsg.getInitiatorId();
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
            out.append("<td>Init debug info</td>");
            String debugInfo = matsTrace.getDebugInfo();
            if ((debugInfo == null) || (debugInfo.isEmpty())) {
                debugInfo = "{none present}";
            }
            out.append("<td>").append(debugInfoToHtml(debugInfo)).append("</td>");
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
            out.append("<td>Mats Flow Initialized Timestamp</td>");
            out.append("<td>").append(Statics.formatTimestamp(matsTrace.getInitializedTimestamp())).append("</td>");
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
        out.append("<td>").append(brokerMsg.isPersistent() ? "Persistent" : "Non-Persistent").append("</td>");
        out.append("</tr>\n");

        out.append("<tr>");
        out.append("<td>&nbsp;&nbsp;Interactive</td>");
        out.append("<td>").append(brokerMsg.isInteractive() ? "Interactive" : "Non-Interactive").append("</td>");
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

        out.append("<tr>");
        out.append("<td>Type</td>");
        out.append("<td>").append(brokerMsg.getMessageType()).append("</td>");
        out.append("</tr>\n");

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
        out.append("<td>").append(brokerMsg.getFromStageId()).append("</td>");
        out.append("</tr>\n");

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Call debug info</td>");
            String debugInfo = matsTrace.getCurrentCall().getDebugInfo();
            if ((debugInfo == null) || (debugInfo.trim().isEmpty())) {
                debugInfo = "{none present}";
            }
            debugInfo = debugInfoToHtml(debugInfo);
            out.append("<td>").append(debugInfo.replace(";", "<br>\n")).append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Mats Message Id</td>");
            out.append("<td>").append(matsTrace.getCurrentCall().getMatsMessageId()).append("</td>");
            out.append("</tr>\n");
        }

        if (matsTrace != null) {
            out.append("<tr>");
            out.append("<td>Mats Message Timestamp</td>");
            out.append("<td>").append(Statics.formatTimestamp(matsTrace.getCurrentCall().getCalledTimestamp()))
                    .append("</td>");
            out.append("</tr>\n");
        }

        out.append("<tr>");
        out.append("<td>To (this)</td>");
        out.append("<td>").append(brokerMsg.getToStageId()).append("</td>");
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
            String size = brokerMsg.getMatsTraceBytes().get().length == matsTraceDecompressedLength
                    ? matsTraceDecompressedLength + " bytes uncompressed"
                    : brokerMsg.getMatsTraceBytes().get().length + " bytes compressed, "
                            + matsTraceDecompressedLength + " bytes decompressed";
            out.append("<td>").append(size).append("</td>");
            out.append("</tr>\n");
        }

        out.append("<tr>");
        out.append("<td>MsgSys Message Timestamp</td>");
        out.append("<td>").append(Statics.formatTimestamp(brokerMsg.getTimestamp())).append("</td>");
        out.append("</tr>\n");

        out.append("<tr>");
        out.append("<td>MsgSys Message Id</td>");
        out.append("<td>").append(brokerMsg.getMessageSystemId()).append("</td>");
        out.append("</tr>\n");

        out.append("<tr>");
        out.append("<td>MsgSys Expires</td>");
        out.append("<td>").append(brokerMsg.getExpirationTimestamp() == 0
                ? "Never expires"
                : Statics.formatTimestamp(brokerMsg.getExpirationTimestamp()))
                .append("</td>");
        out.append("</tr>");

        out.append("</tbody>");
        out.append("</table>");

        out.append("</td>\n"); // end Message information cell
        out.append("</tr></table>"); // end Flow/Message table
    }

    private static void examineMessage_MatsTrace(Appendable out, MatsTrace<?> matsTrace) throws IOException {

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

        List<? extends Call<?>> callFlow = matsTrace.getCallFlow();
        List<? extends StackState<?>> stateFlow = matsTrace.getStateFlow();

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
        out.append("Thus, it is the REQUEST, REPLY and NEXT rows (the calls) in the table that are the real info"
                + " carriers - the <i>\"Processed on\"</i> rows are synthesized with stageId taken from the previous"
                + " call's \"to\", and the <i>app/host</i> and <i>DebugInfo</i> from current call -"
                + " just to aid your intuition.<br />\n");

        // .. SVG-sprite: Arrow down (slanted and colored using CSS)
        // (from font-awesome, via https://leungwensen.github.io/svg-icon/#awesome)
        out.append("<svg display='none'>\n"
                + "  <symbol viewBox='0 0 1558 1483' id='arrow-down'>\n"
                + "    <path d='M1558 704q0 53-37 90l-651 652q-39 37-91 37-53 0-90-37L38 794Q0 758 0 704q0-53"
                + "             38-91l74-75q39-37 91-37 53 0 90 37l294 294V128q0-52 38-90t90-38h128q52 0 90 38t38"
                + "             90v704l294-294q37-37 90-37 52 0 91 37l75 75q37 39 37 91z'/>"
                + "  </symbol>"
                + "</svg>");

        int highestStackHeight = 0;
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            highestStackHeight = Math.max(highestStackHeight, currentCall.getReplyStackHeight());
        }

        out.append("<table class='matsbm_table_matstrace' id='matsbm_table_matstrace'>");
        out.append("<thead>");
        out.append("<tr>");
        out.append("<th>Call#</th>");
        out.append("<th>time</th>");
        out.append("<th>diff</th>");
        out.append("<th colspan='" + (highestStackHeight + 1) + "'>Call/Processing</th>");
        out.append("<th>Application</th>");
        out.append("<th>Host</th>");
        out.append("<th>DebugInfo</th>");
        out.append("</tr>");
        out.append("</thead>");

        // :: Flow
        out.append("<tbody>");

        // :: MatsTrace's Initiation
        out.append("<tr class='call'>");
        out.append("<td>#0</td>");
        out.append("<td>0 ms</td>");
        out.append("<td></td>");
        out.append("<td colspan=100>");
        out.append("INIT<br />from: ").append(matsTrace.getInitiatorId());
        out.append("</td>");
        out.append("</tr>\n");

        // :: IF we're in MINIMAL mode, output empty rows to represent the missing calls.
        if (matsTrace.getKeepTrace() == KeepMatsTrace.MINIMAL) {
            int currentCallNumber = matsTrace.getCallNumber();
            for (int i = 0; i < currentCallNumber - 1; i++) {
                out.append("<tr><td colspan=100></td></tr>");
            }
        }

        // :: MATSTRACE Calls table

        long initializedTimestamp = matsTrace.getInitializedTimestamp();
        // NOTE: If we are KeepMatsTrace.MINIMAL, then there is only 1 entry here
        String prevIndent = "";
        int prevIndentLevel = 0;
        long previousCalledTimestamp = matsTrace.getInitializedTimestamp();
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            // If there is only one call, then it is either first, or MINIMAL and last.
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
            int reverseIndent = highestStackHeight - indentLevel;

            // :: PROCESSING row
            out.append("<tr class='processing' id='matsbm_processrow_" + currentCallNumber + "'"
                    + " onmouseover='matsmb_hover_call(event)' onmouseout='matsmb_hover_call_out(event)' data-callno='"
                    + currentCallNumber + "'>");
            out.append("<td></td>");
            out.append("<td></td>");
            out.append("<td></td>");
            out.append(prevIndent).append("<td onclick='matsbm_callmodal(event)' colspan='" + (highestStackHeight
                    - prevIndentLevel + 1) + "'>")
                    .append("<i>Processed&nbsp;on&nbsp;</i>");
            prevIndent = indent;
            prevIndentLevel = indentLevel;
            if (prevCall != null) {
                out.append(prevCall.getTo().getId());
            }
            else {
                out.append("Initiation");
            }
            out.append("<br />\n");
            out.append("</td>");
            out.append("<td>@");
            if (prevCall != null) {
                out.append(currentCall.getCallingAppName())
                        .append("; v.").append(currentCall.getCallingAppVersion());
            }
            else {
                out.append(matsTrace.getInitializingAppName())
                        .append("; v.").append(matsTrace.getInitializingAppVersion());
            }
            out.append("</td><td>@");
            if (prevCall != null) {
                out.append(currentCall.getCallingHost());
            }
            else {
                out.append(matsTrace.getInitializingHost());
            }
            out.append("</td><td class='matsbm_from_info'>");
            if (prevCall != null) {
                out.append(debugInfoToHtml(currentCall.getDebugInfo()));
            }
            else {
                out.append(debugInfoToHtml(matsTrace.getDebugInfo()));
            }
            out.append("</td>");
            out.append("</tr>");

            // :: CALL row
            out.append("<tr class='call' id='matsbm_callrow_" + currentCallNumber + "'"
                    + " onmouseover='matsmb_hover_call(event)' onmouseout='matsmb_hover_call_out(event)' data-callno='"
                    + currentCallNumber + "'>");
            out.append("<td>#");
            out.append(Integer.toString(currentCallNumber));
            out.append("</td>");
            long currentCallTimestamp = currentCall.getCalledTimestamp();
            out.append("<td>").append(Long.toString(currentCallTimestamp - initializedTimestamp))
                    .append("&nbsp;ms</td>");
            long diffBetweenLast = currentCallTimestamp - previousCalledTimestamp;
            previousCalledTimestamp = currentCallTimestamp;
            out.append("<td>").append(Long.toString(diffBetweenLast)).append("&nbsp;ms</td>"); // Proc

            out.append(indent).append("<td onclick='matsbm_callmodal(event)'"
                    + " colspan='" + (highestStackHeight - indentLevel + 5) + "'>");
            String callType;
            switch (currentCall.getCallType()) {
                case REQUEST:
                    callType = "<svg class='matsmb_arrow_req'><use xlink:href=\"#arrow-down\" /></svg> this is a REQUEST";
                    break;
                case REPLY:
                    callType = "<svg class='matsmb_arrow_rep'><use xlink:href=\"#arrow-down\" /></svg> this is a REPLY";
                    break;
                case NEXT:
                    callType = "<svg class='matsmb_arrow_next'><use xlink:href=\"#arrow-down\" /></svg> this is a "
                            + currentCall.getCallType();
                    break;
                case GOTO:
                    callType = "<svg class='matsmb_arrow_goto'><use xlink:href=\"#arrow-down\" /></svg> this is a "
                            + currentCall.getCallType();
                    break;
                default:
                    callType = "this is a " + currentCall.getCallType();
            }
            out.append(callType).append(" call");
            StackState<?> stackState = callToState.get(currentCall);
            if (stackState != null) {
                out.append(i == 0 ? " w/ initial state" : " w/ state");
            }
            out.append(" - <a href='//show call' onclick='matsbm_noclick(event)'>show</a>");
            out.append("<br/>");

            out.append("<i>to:</i>&nbsp;").append(currentCall.getTo().getId());
            out.append("</td>");

            out.append("</tr>");
        }
        out.append("</table>");

        // :: MODALS: Calls and optionally also state

        // The "modal underlay", i.e. "gray out" - starts out 'display: none', visible if modal is showing.
        out.append("<div id='matsmb_callmodalunderlay' class='matsmb_callmodalunderlay'"
                + " onclick='matsmb_clearcallmodal(event)'>");

        String previousTo = "Initiation";
        for (int i = 0; i < callFlow.size(); i++) {
            Call<?> currentCall = callFlow.get(i);
            // If there is only one call, then it is either first, or MINIMAL and last.
            int currentCallNumber = callFlow.size() == 1 ? matsTrace.getCallNumber() : i + 1;
            out.append("<div class='matsbm_box_call_and_state_modal' id='matsbm_callmodal_" + currentCallNumber
                    + "'>\n");
            String from = matsTrace.getKeepTrace() == KeepMatsTrace.MINIMAL
                    ? currentCall.getFrom()
                    : previousTo;
            String appAndVer = currentCallNumber == 1
                    ? matsTrace.getInitializingAppName() + "; v." + matsTrace.getInitializingAppVersion()
                    : currentCall.getCallingAppName() + "; v." + currentCall.getCallingAppVersion();
            String host = currentCallNumber == 1
                    ? matsTrace.getInitializingHost()
                    : currentCall.getCallingHost();

            out.append("This is a message from <b>").append(from)
                    .append("</b><br/>on application <b>").append(appAndVer)
                    .append("</b><br/>running on node <b>").append(host)
                    .append("</b><br/>.. and it is a<br />\n");
            out.append("<h3>").append(currentCall.getCallType().toString())
                    .append(" call to <b>").append(currentCall.getTo().getId())
                    .append("</b></h3><br/>\n");
            previousTo = currentCall.getTo().getId();
            // State:
            out.append("<div class='matsbm_box_call_or_state'>\n");
            StackState<?> stackState = callToState.get(currentCall);
            if (stackState != null) {
                out.append("Incoming state: ");
                out_displaySerializedRepresentation(out, stackState.getState());
            }
            else {
                out.append("<i>-no incoming state-</i>");
            }
            out.append("</div><br/>\n");

            // Message:
            out.append("<div class='matsbm_box_call_or_state'>\n");
            out.append("Incoming message: ");
            out_displaySerializedRepresentation(out, currentCall.getData());
            out.append("</div><br />\n");

            out.append("</div><br/>\n");
        }
        out.append("</div>");

        // TEMP:
        out.append("<br /><br /><br /><br />");
        out.append("Temporary! MatsTrace.toString()");
        out.append("<pre>");
        out.append(matsTrace.toString());
        out.append("</pre>");
    }

    private static void examineMessage_currentCallMatsTrace(Appendable out, MatsTrace<String> stringMatsTrace)
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

    /**
     * If String, try to display as JSON, if not just raw. If byte, just display array size.
     */
    private static void out_displaySerializedRepresentation(Appendable out, Object data) throws IOException {
        if (data instanceof String) {
            String stringData = (String) data;
            out.append("String[").append(Integer.toString(stringData.length())).append(" chars]<br/>\n");

            try {
                String jsonData = new ObjectMapper().readTree(stringData).toPrettyString();
                out.append("<div class='matsbm_box_call_or_state_div'>").append(jsonData).append("</div>");
            }
            catch (JsonProcessingException e) {
                out.append(
                        "Couldn't parse incoming String as json (thus no pretty printing), so here it is unparsed.<br/>");
                out.append(stringData);
            }
        }
        if (data instanceof byte[]) {
            byte[] byteData = (byte[]) data;
            out.append("byte[").append(Integer.toString(byteData.length)).append(" bytes]<br/>\n");
        }
    }

    private static String debugInfoToHtml(String debugInfo) {
        if ((debugInfo == null) || (debugInfo.trim().isEmpty())) {
            debugInfo = "{none present}";
        }
        debugInfo = debugInfo.replace("<", "&lt;").replace(">", "&gt;")
                .replace(";", "<br>\n");
        return "<code>" + debugInfo + "</code>";
    }

    private static Optional<MatsTrace<String>> getStringMatsTrace(MatsTrace<?> matsTrace) throws IOException {
        Object data = matsTrace.getCurrentCall().getData();
        if (data instanceof String) {
            @SuppressWarnings("unchecked")
            MatsTrace<String> casted = (MatsTrace<String>) matsTrace;
            return Optional.of(casted);
        }
        return Optional.empty();
    }
}
