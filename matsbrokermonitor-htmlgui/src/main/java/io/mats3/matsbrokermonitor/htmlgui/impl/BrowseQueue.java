package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.BrokerIOException;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageIterable;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerSnapshot;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.AccessControl;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.BrowseQueueTableAddition;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.MonitorAddition;

/**
 * @author Endre Stølsvik 2022-03-13 23:33 - http://stolsvik.com/, endre@stolsvik.com
 */
class BrowseQueue {

    // Note: The queue-browser of ActiveMQ has a default max, from the server side, of 400.
    private static final int MAX_MESSAGES_BROWSER = 2000;

    static void gui_BrowseQueue(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions, List<? super MonitorAddition> monitorAdditions,
            Outputter out, String queueId, AccessControl ac) throws IOException {
        out.html("<div id='matsbm_page_browse_queue' class='matsbm_report'>\n");
        out.html("<div class='matsbm_actionbuttons'>\n");

        out.html("<a id='matsbm_back_broker_overview' href='?'>Back to Broker Overview [Esc]</a><br>\n");

        Collection<MatsBrokerDestination> values = matsBrokerMonitor.getSnapshot()
                .map(BrokerSnapshot::getMatsDestinations)
                .map(SortedMap::values)
                .orElseGet(Collections::emptySet);

        MatsBrokerDestination matsBrokerDestination = null;
        for (MatsBrokerDestination dest : values) {
            if ((dest.getDestinationType() == DestinationType.QUEUE) && queueId.equals(dest.getDestinationName())) {
                matsBrokerDestination = dest;
            }
        }

        out.html("<div class='matsbm_heading'>");
        if (matsBrokerDestination == null) {
            out.html("<h1>No info about queue!</h1><br>\n");
            out.html("<b>Queue:</b> ").DATA(queueId).html("<br>\n");
            out.html("</div>");
            out.html("</div>");
            return;
        }

        // ?: Is this the Global DLQ?
        if (matsBrokerDestination.isDefaultGlobalDlq()) {
            // -> Yes, global DLQ
            out.html("Browsing the <h1>Global DLQ</h1>, fully qualified name: '")
                    .DATA(matsBrokerDestination.getFqDestinationName()).html("'");
        }
        else {
            // -> No, not the Global DLQ
            // ?: Is this a MatsStage Queue or DLQ?
            if (matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> Mats stage queue.
                out.html("<h1>Browsing ");
                out.DATA(matsBrokerDestination.isDlq() ? "DLQ" : "Incoming Queue");
                out.html(" for <div class='matsbm_stageid'>")
                        .DATA(matsBrokerDestination.getMatsStageId().get()).html("</div></h1>");
            }
            else {
                // -> Non-Mats Queue. Not really supported, but just to handle it.
                out.html("<h1>Browsing ").DATA(matsBrokerDestination.isDlq() ? "DLQ" : "Queue");
                out.html(" named ").DATA(matsBrokerDestination.getDestinationName()).html("</h1>");
            }
        }
        out.html("</div>\n"); // /matsbm_heading

        out.html("Broker Queue '").DATA(queueId).html("'<br>\n");

        out.html("At ").DATA(Statics.formatTimestampSpan(matsBrokerDestination.getLastUpdateLocalMillis()))
                .html(" it had ").DATA(matsBrokerDestination.getNumberOfQueuedMessages()).html(" messages");
        if (matsBrokerDestination.getNumberOfInflightMessages().isPresent()) {
            out.html(" of which ")
                    .DATA(matsBrokerDestination.getNumberOfInflightMessages().getAsLong())
                    .html(" were in-flight");
        }
        out.html(".");
        if (matsBrokerDestination.getLastUpdateBrokerMillis().isPresent()) {
            out.html(" (broker time: ")
                    .DATA(Statics.formatTimestamp(matsBrokerDestination.getLastUpdateBrokerMillis().getAsLong()))
                    .html(")");
        }
        out.html("<br>\n");

        // :: BUTTONS: REISSUE, DELETE

        out.html("<input type='button' id='matsbm_reissue_bulk' value='Reissue [R]'"
                + " class='matsbm_button matsbm_button_reissue matsbm_button_disabled'"
                + " onclick='matsbm_reissue_bulk(event, \"").DATA(queueId).html("\")'>");
        out.html("<input type='button' id='matsbm_delete_bulk' value='Delete [D]'"
                + " class='matsbm_button matsbm_button_delete matsbm_button_disabled'"
                + " onclick='matsbm_delete_propose_bulk(event)'>");
        out.html("<input type='button' id='matsbm_delete_cancel_bulk' value='Cancel Delete [Esc]'"
                + " class='matsbm_button matsbm_button_delete_cancel matsbm_button_hidden'"
                + " onclick='matsbm_delete_cancel_bulk(event)'>");
        out.html("<input type='button' id='matsbm_delete_confirm_bulk' value='Confirm Delete [X]'"
                + " class='matsbm_button matsbm_button_delete matsbm_button_hidden'"
                + " onclick='matsbm_delete_confirmed_bulk(event, \"").DATA(queueId).html("\")'>");
        out.html("<input type='button' id='matsbm_button_forceupdate' value='Update Now!'"
                + " class='matsbm_button matsbm_button_forceupdate"
                + "' onclick='matsbm_button_forceupdate(event)'>");
        out.html("<span id='matsbm_action_message'></span>");
        out.html("<br>");
        out.html("</div>\n"); // /matsbm_actionbuttons

        out.html("<br>\n");

        // :: TABLE

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
        // Note: AutoCloseable, try-with-resources
        try (MatsBrokerMessageIterable messages = matsBrokerBrowseAndActions.browseQueue(queueId)) {
            for (MatsBrokerMessageRepresentation msgRepr : messages) {
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
                out.DATA(msgRepr.getInitializingApp() != null ? msgRepr.getInitializingApp() : "{missing init app}");
                out.html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_breakall'>");
                out.DATA(msgRepr.getInitiatorId() != null ? msgRepr.getInitiatorId() : "{missing init id}");
                out.html("</div></td>");

                out.html("<td><div class='matsbm_table_browse_nobreak'>").DATA(msgRepr.getMessageType())
                        .html(" from</div></td>");

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
        if (messageCount > 200) {
            out.html(" <i>(Note: Our max is ").DATA(MAX_MESSAGES_BROWSER).html(
                    ", but the message broker might have a smaller max browse. ActiveMQ default is 400)</i>\n");
        }
        out.html("</div></div>");

        // If there are no messages, say so.
        if (messageCount == 0) {
            out.html("<h1>No messages!</h1><br>\n");
        }

        // Don't output last </div>, as caller does it.
    }
}
