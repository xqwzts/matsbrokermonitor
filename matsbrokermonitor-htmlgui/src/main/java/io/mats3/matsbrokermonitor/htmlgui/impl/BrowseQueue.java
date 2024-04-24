package io.mats3.matsbrokermonitor.htmlgui.impl;

import static io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType.STANDARD;
import static io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType.UNKNOWN;
import static io.mats3.matsbrokermonitor.htmlgui.impl.BrokerOverview.out_queueCount;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.BrokerIOException;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageIterable;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerSnapshot;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;
import io.mats3.matsbrokermonitor.api.MatsFabricAggregatedRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricAggregatedRepresentation.MatsStageBrokerRepresentation;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.AccessControl;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.BrowseQueueTableAddition;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.MonitorAddition;

/**
 * @author Endre Stølsvik 2022-03-13 23:33 - http://stolsvik.com/, endre@stolsvik.com
 */
class BrowseQueue {
    private static final Logger log = LoggerFactory.getLogger(BrowseQueue.class);

    // Note: The queue-browser of ActiveMQ has a default max, from the server side, of 400.
    private static final int MAX_MESSAGES_BROWSER = 2000;

    static void gui_BrowseQueue(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions, List<? super MonitorAddition> monitorAdditions,
            Outputter out, String queueId, AccessControl ac, boolean autoJumpIfSingleMessage) throws IOException {
        out.html("<div id='matsbm_page_browse_queue' class='matsbm_report'>\n");
        out.html("<div class='matsbm_actionbuttons'>\n");

        out.html("<a id='matsbm_back_broker_overview' href='?'>Back to Broker Overview [Esc]</a><br>\n");

        Collection<MatsBrokerDestination> matsBrokerDestinations = matsBrokerMonitor.getSnapshot()
                .map(BrokerSnapshot::getMatsDestinations)
                .map(SortedMap::values)
                .orElseGet(Collections::emptySet);

        MatsBrokerDestination matsBrokerDestination = null;
        for (MatsBrokerDestination dest : matsBrokerDestinations) {
            if ((dest.getDestinationType() == MatsBrokerDestination.DestinationType.QUEUE) && queueId.equals(dest
                    .getDestinationName())) {
                matsBrokerDestination = dest;
            }
        }

        // "Stack up" into a Mats Fabric representation
        MatsFabricAggregatedRepresentation stack = MatsFabricAggregatedRepresentation
                .stack(matsBrokerDestinations);

        // :: HANDLING MISSING DESTINATION

        out.html("<div class='matsbm_heading'>");
        if (matsBrokerDestination == null) {
            out.html("<h1>No info about queue!</h1><br>\n");
            out.html("<b>Queue:</b> ").DATA(queueId).html("<br>\n");
            out.html("</div>");
            out.html("</div>");
            return;
        }

        // :: CONTEXT FOR JAVASCRIPT: Move some data to JS context: Number of messages on queue.

        long numberOfQueuedMessages = matsBrokerDestination.getNumberOfQueuedMessages();

        out.html("<script>\n    const matsbm_number_of_messages_on_queue = ")
                .DATA(numberOfQueuedMessages)
                .html(";\n</script>\n");

        // :: HEADING

        // ?: Is this the Global DLQ?
        if (matsBrokerDestination.isBrokerDefaultGlobalDlq()) {
            // -> Yes, global DLQ
            out.html("<h1>Browsing the <i>Global DLQ</i></h1>");
        }
        else {
            // -> No, not the Global DLQ
            // ?: Is this a MatsStage Queue or DLQ?
            if (matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> Mats stage queue.
                String typeName = matsBrokerDestination.getStageDestinationType().orElse(UNKNOWN).getTypeName();
                out.html("<h1>Browsing <i><b>").DATA(typeName)
                        .html("</b></i> for <div class='matsbm_stageid'>")
                        .DATA(matsBrokerDestination.getMatsStageId().get()).html("</div></h1>");
            }
            else {
                // -> Non-Mats Queue. Not really supported, but just to handle it.
                out.html("<h1>Browsing <i>non-" + Statics.MATS3_HTML + " ")
                        .DATA(matsBrokerDestination.isDlq() ? "DLQ" : "Queue ");
                out.html("</i> named ").DATA(matsBrokerDestination.getDestinationName()).html("</h1>");
            }
        }
        out.html("<br/>Fully Qualified Queue Name: <code><b>").DATA(matsBrokerDestination.getFqDestinationName())
                .html("</b></code><br>\n");
        out.html("</div>\n"); // /matsbm_heading

        out.html("At ").DATA(Statics.formatTimestampSpan(matsBrokerDestination.getLastUpdateLocalMillis()))
                .html(" it had <b>").DATA(numberOfQueuedMessages).html(" messages</b>");
        if (matsBrokerDestination.getNumberOfInflightMessages().isPresent()) {
            out.html(" of which ").DATA(matsBrokerDestination.getNumberOfInflightMessages().getAsLong())
                    .html(" were in-flight");
        }
        out.html(".");
        if (matsBrokerDestination.getLastUpdateBrokerMillis().isPresent()) {
            out.html(" (broker time: ")
                    .DATA(Statics.formatTimestamp(matsBrokerDestination.getLastUpdateBrokerMillis().getAsLong()))
                    .html(")");
        }
        out.html("<br>\n");

        Optional<MatsStageBrokerRepresentation> stageO = stack.findStageForDestinationName(matsBrokerDestination
                .getDestinationName());

        if (stageO.isPresent()) {
            out.html("<div class='matsbm_other_queues_for_stage'>");
            MatsStageBrokerRepresentation stage = stageO.get();
            Optional<MatsBrokerDestination> incomingDest = stage.getDestination(STANDARD);
            if (incomingDest.isPresent()) {
                out_queueCount(out, incomingDest.get(), true);
            }

            Optional<MatsBrokerDestination> npiaDest = stage.getDestination(
                    StageDestinationType.NON_PERSISTENT_INTERACTIVE);
            if (npiaDest.isPresent()) {
                out_queueCount(out, npiaDest.get(), true);
            }

            Optional<MatsBrokerDestination> dlqDest = stage.getDestination(
                    StageDestinationType.DEAD_LETTER_QUEUE);
            if (dlqDest.isPresent()) {
                out_queueCount(out, dlqDest.get(), true);
            }

            Optional<MatsBrokerDestination> npiaDlqDest = stage.getDestination(
                    StageDestinationType.DEAD_LETTER_QUEUE_NON_PERSISTENT_INTERACTIVE);
            if (npiaDlqDest.isPresent()) {
                out_queueCount(out, npiaDlqDest.get(), true);
            }

            Optional<MatsBrokerDestination> mutedDlqDest = stage.getDestination(
                    StageDestinationType.DEAD_LETTER_QUEUE_MUTED);
            if (mutedDlqDest.isPresent()) {
                out_queueCount(out, mutedDlqDest.get(), true);
            }

            Optional<MatsBrokerDestination> wiretapDest = stage.getDestination(
                    StageDestinationType.WIRETAP);
            if (wiretapDest.isPresent()) {
                out_queueCount(out, wiretapDest.get(), true);
            }
            out.html("</div>");
        }

        // :: BUTTONS: REISSUE, MUTE, DELETE, FORCE UPDATE

        // REISSUE AND MUTE selected:
        // ?: Is this a DLQ? (Both Normal and Muted DLQ)
        if (matsBrokerDestination.isDlq()) {
            // Reissue selected (instant, no confirm)
            if (ac.reissueMessage(queueId)) {
                out.html("<button id='matsbm_reissue_selected'"
                        + " class='matsbm_button matsbm_button_reissue matsbm_button_disabled'"
                        + " onclick='matsbm_reissue_selected(event, \"")
                        .DATA(queueId).html("\")'>Reissue [r]</button>");
            }

            // ?: Is this a NOT a Muted DLQ?
            if (ac.muteMessage(queueId) && matsBrokerDestination.getStageDestinationType().isPresent() &&
                    (matsBrokerDestination.getStageDestinationType()
                            .get() != StageDestinationType.DEAD_LETTER_QUEUE_MUTED)) {
                // -> No, normal DLQ, so output Mute button
                out.html("<button id='matsbm_mute_selected'"
                        + " class='matsbm_button matsbm_button_mute matsbm_button_disabled'"
                        + " onclick='matsbm_mute_selected(event,\"")
                        .DATA(queueId).html("\")'>Mute [m]</button>");
            }
        }

        // DELETE selected
        if (ac.deleteMessage(queueId)) {
            // Propose delete selected
            out.html("<button id='matsbm_delete_selected'"
                    + " class='matsbm_button matsbm_button_delete matsbm_button_disabled'"
                    + " onclick='matsbm_delete_selected_propose(event)'>Delete... [d]</button>");
            // Cancel delete selected
            out.html("<button id='matsbm_delete_selected_cancel'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete_cancel matsbm_button_hidden'"
                    + " onclick='matsbm_delete_mute_or_reissue_cancel(event)'>Cancel <b>Delete Selected</b> [Esc]</button>");
            // Confirm delete selected
            out.html("<button id='matsbm_delete_selected_confirm'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete matsbm_button_hidden'"
                    + " onclick='matsbm_delete_selected_confirm(event, \"").DATA(queueId).html("\")'>"
                    + "Confirm <b>Delete Selected</b> [x]</button>");
        }

        out.html("&nbsp;&nbsp;&nbsp;");

        // REISSUE AND MUTE all:
        // ?: Is this a DLQ? (Both Normal and Muted DLQ)
        if (matsBrokerDestination.isDlq()) {
            if (ac.reissueMessage(queueId)) {
                // Propose reissue all
                out.html("<button id='matsbm_reissue_all'"
                        + " class='matsbm_button matsbm_button_reissue"
                        + (numberOfQueuedMessages > 0 ? "" : " matsbm_button_disabled")
                        + "' onclick='matsbm_reissue_all_propose(event)'>Reissue All... [r]</button>");
                // Cancel reissue all
                out.html("<button id='matsbm_reissue_all_cancel'"
                        + " class='matsbm_button matsbm_button_wider matsbm_button_reissue_cancel matsbm_button_hidden'"
                        + " onclick='matsbm_delete_mute_or_reissue_cancel(event)'>Cancel <b>Reissue All</b> [Esc]</button>");
                // Confirm reissue all
                out.html("<button id='matsbm_reissue_all_confirm'"
                        + " class='matsbm_button matsbm_button_wider matsbm_button_reissue matsbm_button_hidden'"
                        + " onclick='matsbm_reissue_all_confirm(event, \"").DATA(queueId).html("\")'>"
                        + "Confirm <b>Reissue All</b> [x]</button>");
            }
            // ?: Is this a NOT a Muted DLQ?
            if (ac.muteMessage(queueId) && matsBrokerDestination.getStageDestinationType().isPresent() &&
                    (matsBrokerDestination.getStageDestinationType()
                            .get() != StageDestinationType.DEAD_LETTER_QUEUE_MUTED)) {
                // -> No, normal DLQ, so output Mute buttons
                // Propose mute all
                out.html("<button id='matsbm_mute_all'"
                        + " class='matsbm_button matsbm_button_mute"
                        + (numberOfQueuedMessages > 0 ? "" : " matsbm_button_disabled")
                        + "' onclick='matsbm_mute_all_propose(event)'>Mute All... [m]</button>");
                // Cancel mute all
                out.html("<button id='matsbm_mute_all_cancel'"
                        + " class='matsbm_button matsbm_button_wider matsbm_button_mute_cancel matsbm_button_hidden'"
                        + " onclick='matsbm_delete_mute_or_reissue_cancel(event)'>Cancel <b>Mute All</b> [Esc]</button>");
                // Confirm mute all
                out.html("<button id='matsbm_mute_all_confirm'"
                        + " class='matsbm_button matsbm_button_wider matsbm_button_mute matsbm_button_hidden'"
                        + " onclick='matsbm_mute_all_confirm(event, \"").DATA(queueId).html("\")'>"
                                + "Confirm <b>Mute All</b> [x]</button>");
            }
        }

        // DELETE all
        if (ac.deleteMessage(queueId)) {
            // Propose delete all
            out.html("<button id='matsbm_delete_all'"
                    + " class='matsbm_button matsbm_button_delete"
                    + (numberOfQueuedMessages > 0 ? "" : " matsbm_button_disabled")
                    + "' onclick='matsbm_delete_all_propose(event)'>Delete All... [d]</button>");
            // Cancel delete all
            out.html("<button id='matsbm_delete_all_cancel'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete_cancel matsbm_button_hidden'"
                    + " onclick='matsbm_delete_mute_or_reissue_cancel(event)'>Cancel <b>Delete All</b> [Esc]</button>");
            // Confirm delete all
            out.html("<button id='matsbm_delete_all_confirm'"
                    + " class='matsbm_button matsbm_button_wider matsbm_button_delete matsbm_button_hidden'"
                    + " onclick='matsbm_delete_all_confirm(event, \"").DATA(queueId).html("\")'>"
                    + "Confirm <b>Delete All</b> [x]</button>");
        }

        // Limit messages input field (for all of reissue all, mute all, and delete all)
        out.html("<div id='matsbm_all_limit_div' class='matsbm_input_limit_div matsbm_input_limit_div_hidden'>")
                .html("<label for='matsbm_reissue_all_max_messages'>Limit: </label>"
                        + "<input type='text' id='matsbm_all_limit_messages' value='")
                .DATA(numberOfQueuedMessages)
                .html("' autocomplete='off' inputmode='numeric' class='matsbm_input_max'> messages</div>");

        // Force update
        out.html("<button id='matsbm_button_forceupdate'"
                + " class='matsbm_button matsbm_button_forceupdate"
                + "' onclick='matsbm_button_forceupdate(event)'>Update Now! [u]</button>");
        out.html("<span id='matsbm_action_message'></span>");
        out.html("<br>");
        out.html("</div>\n"); // /matsbm_actionbuttons

        out.html("<br>\n");

        // :: MESSAGES TABLE

        // Note: Getting the iterator first, so that we can output any error before the table.
        // Will be closed by try-with-resources later.
        MatsBrokerMessageIterable matsBrokerMessageIterable;
        try {
            matsBrokerMessageIterable = matsBrokerBrowseAndActions.browseQueue(queueId);
        }
        catch (BrokerIOException e) {
            out.html("<h1>Got BrokerIOException when trying to browse queue!</h1><br>\n");
            log.error("Got BrokerIOException when trying to browse queue!", e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            out.html("<pre>").DATA(sw.toString()).html("</pre>");
            return;
        }

        out.html("<div class='matsbm_table_browse_queue_container'>"); // To make room for text "number of messages"
        out.html("<table id='matsbm_table_browse_queue'>");
        out.html("<thead>");
        out.html("<th><input type='checkbox' id='matsbm_checkall' autocomplete='off'"
                + " onchange='matsbm_checkall(event)'></th>");
        out.html("<th><input type='button' value='\u2b05 Invert' id='matsbm_checkinvert'"
                + " onclick='matsbm_checkinvert(event)'> Sent</th>");
        out.html("<th>View</th>");

        // :: Include the "additions" table cells for the message.
        List<BrowseQueueTableAddition> tableAdditions = monitorAdditions.stream()
                .filter(o -> o instanceof BrowseQueueTableAddition)
                .map(o -> (BrowseQueueTableAddition) o)
                .collect(Collectors.toList());
        // Store for whether this column want to be included for this queue.
        IdentityHashMap<BrowseQueueTableAddition, Boolean> additionsIncluded = new IdentityHashMap<>();
        for (BrowseQueueTableAddition tableAddition : tableAdditions) {
            String columnHeadingHtml = tableAddition.getColumnHeadingHtml(queueId);
            // Want to be included?
            boolean include = columnHeadingHtml != null;
            additionsIncluded.put(tableAddition, include);
            out.html(include ? columnHeadingHtml : "");
        }
        out.html("<th>TraceId</th>");
        out.html("<th>Init App</th>");
        out.html("<th>InitatorId</th>");
        out.html("<th>Type</th>");
        out.html("<th>From Id</th>");
        out.html("<th>Persistent</th>");
        out.html("<th>Interactive</th>");
        out.html("<th>Expires</th>");
        out.html("</thead>");
        out.html("<tbody>");
        int messageCount = 0;
        String firstMessageId = null;
        // Note: AutoCloseable, try-with-resources
        try (matsBrokerMessageIterable) {
            // Notice: Outputting the information in a streaming fashion, so that we don't have to hold on to the
            // MatsBrokerMessageRepresentation instances, which can be large. (They can thus be GCed as soon as we've
            // output their info.)
            for (MatsBrokerMessageRepresentation msgRepr : matsBrokerMessageIterable) {
                if (firstMessageId == null) {
                    firstMessageId = msgRepr.getMessageSystemId();
                }
                out.html("<tr id='matsbm_msgid_").DATA(msgRepr.getMessageSystemId()).html("'>");

                out.html("<td><div class='matsbm_table_browse_nobreak'>");
                out.html("<input type='checkbox' class='matsbm_checkmsg' autocomplete='off' data-msgid='")
                        .DATA(msgRepr.getMessageSystemId()).html("' onchange='matsbm_checkmsg(event)'>");
                out.html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_nobreak'>");
                Instant instant = Instant.ofEpochMilli(msgRepr.getTimestamp());
                out.html(Statics.formatTimestampSpan(instant.toEpochMilli()));
                out.html("</div></td>");

                // View/Examine-button
                out.html("<td><div class='matsbm_table_browse_nobreak'>");
                out.html("<a class='matsbm_table_examinemsg' href='?examineMessage&destinationId=queue:").DATA(queueId)
                        .html("&messageSystemId=").DATA(msgRepr.getMessageSystemId()).html("'>");
                out.html("View</a>");
                out.html("</div></td>");

                // :: Include the "additions" table cells for the message.
                for (BrowseQueueTableAddition tableAddition : tableAdditions) {
                    // ?: Did it choose to be included?
                    if (additionsIncluded.get(tableAddition)) {
                        // -> Yes, so include it.
                        String html = tableAddition.convertMessageToHtml(msgRepr);
                        out.html(html != null ? html : "<td><div></div></td>");
                    }
                }

                out.html("<td><div class='matsbm_table_browse_breakall'>").DATA(msgRepr.getTraceId())
                        .html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_breakall'>");
                out.DATA(msgRepr.getInitiatingApp() != null ? msgRepr.getInitiatingApp() : "{missing init app}");
                out.html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_breakall'>");
                out.DATA(msgRepr.getInitiatorId() != null ? msgRepr.getInitiatorId() : "{missing init id}");
                out.html("</div></td>");

                String dispatchType = msgRepr.getDispatchType() != null ? msgRepr.getDispatchType() : "{missing type}";
                String messageType = msgRepr.getMessageType() != null ? msgRepr.getMessageType() : "{missing}";
                out.html("<td><div class='matsbm_table_browse_nobreak'>").DATA(dispatchType).html(" / ")
                        .DATA(messageType.replace("SUBSCRIPTION", "SUB")).html(" from</div></td>");

                out.html("<td><div class='matsbm_table_browse_breakall'>").DATA(msgRepr.getFromStageId())
                        .html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_nobreak'>").html(msgRepr.isPersistent()
                        ? "Persistent"
                        : "<b>Non-Persistent</b>").html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_nobreak'>").html(msgRepr.isInteractive()
                        ? "<b>Interactive</b>"
                        : "Non-Interactive").html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_nobreak'>").html(msgRepr.getExpirationTimestamp() == 0
                        ? "Never expires"
                        : "<b>" + Statics.formatTimestampSpan(msgRepr.getExpirationTimestamp()) + "</b>")
                        .html("</div></td>");

                out.html("</tr>\n");

                // Max out
                if (++messageCount >= MAX_MESSAGES_BROWSER) {
                    break;
                }
            }
        }
        catch (BrokerIOException e) {
            throw new IOException("Can't talk with broker.", e);
        }
        out.html("</tbody>");
        out.html("</table>");

        // This text is displayed above the table. THE MAGIC OF CSS!!!
        out.html("<div id='matsbm_num_messages_shown'>Browsing ").DATA(messageCount).html(
                " messages directly from queue.");
        if (messageCount > 100) {
            out.html(" <i>(Note: Our max is ").DATA(MAX_MESSAGES_BROWSER).html(
                    ", but the message broker might have a smaller max browse. ActiveMQ default is 400)</i>\n");
        }
        out.html("</div></div>");

        if (autoJumpIfSingleMessage && (messageCount == 1)) {
            out.html("<div class='matsbm_autojump'>Single message!</div>\n");
            out.html("<script>\n"
                    + "  setTimeout(() => {\n"
                    + "    window.location.href = window.location.origin + window.location.pathname"
                    + " + '?examineMessage&destinationId=queue:")
                    .DATA(queueId).html("&messageSystemId=").DATA(firstMessageId)
                    .html("';\n  }, 1000);\n</script>\n");
        }

        // If there are no messages, say so.
        if (messageCount == 0) {
            out.html("<h1>No messages!</h1><br>\n");
        }

        // Call into JavaScript to set state for buttons etc.
        out.html("<script>matsbm_browse_queue_view_loaded();</script>\n");

        // Don't output last </div>, as caller does it.
    }
}
