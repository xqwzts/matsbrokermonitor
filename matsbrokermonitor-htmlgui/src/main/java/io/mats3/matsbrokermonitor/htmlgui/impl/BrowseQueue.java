package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.BrokerIOException;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageIterable;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.AccessControl;

/**
 * @author Endre Stølsvik 2022-03-13 23:33 - http://stolsvik.com/, endre@stolsvik.com
 */
class BrowseQueue {

    // Note: The queue-browser of ActiveMQ has a default max, from the server side, of 400.
    private static final int MAX_MESSAGES_BROWSER = 2000;

    static void gui_BrowseQueue(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions, Outputter out, String destinationId,
            AccessControl ac) throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.html("<div id='matsbm_page_browse_queue' class='matsbm_report'>\n");
        out.html("<a id='matsbm_back_broker_overview' href='?'>Back to Broker Overview [Esc]</a><br>\n");

        String queueId = destinationId.substring("queue:".length());

        Collection<MatsBrokerDestination> values = matsBrokerMonitor.getMatsDestinations().values();
        MatsBrokerDestination matsBrokerDestination = null;
        for (MatsBrokerDestination dest : values) {
            if (dest.getDestinationType() == DestinationType.QUEUE
                    && queueId.equals(dest.getDestinationName())) {
                matsBrokerDestination = dest;
            }
        }
        if (matsBrokerDestination == null) {
            // TODO: Write something sane here instead!
            throw new IllegalArgumentException("Unknown destination!");
        }

        out.html("Broker Queue '").DATA(queueId).html("'");
        // ?: Is this the Global DLQ?
        if (matsBrokerDestination.isGlobalDlq()) {
            // -> Yes, global DLQ
            out.html(" is the Global DLQ, fully qualified name: [")
                    .DATA(matsBrokerDestination.getFqDestinationName())
                    .html("]<br>\n");
        }
        else {
            // -> No, not the Global DLQ
            // ?: Is this a MatsStage Queue or DLQ?
            if (matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> Mats stage queue.
                out.html(" is the ");
                out.DATA(matsBrokerDestination.isDlq() ? "DLQ" : "incoming Queue");
                out.html(" for Mats Stage '")
                        .DATA(matsBrokerDestination.getMatsStageId().get()).html("'");
            }
            else {
                // -> Non-Mats Queue. Not really supported, but just to handle it.
                out.html(" is a ");
                out.DATA(matsBrokerDestination.isDlq() ? "DLQ" : "Queue");
            }
            out.html("<br>\n");
        }

        long lastUpdate = matsBrokerDestination.getLastUpdateBrokerMillis()
                .orElse(matsBrokerDestination.getLastUpdateLocalMillis());
        LocalDateTime lastUpdateDt = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastUpdate), ZoneId.systemDefault());

        out.html("At ").DATA(lastUpdateDt.toString()).html(" it had ")
                .DATA(matsBrokerDestination.getNumberOfQueuedMessages())
                .html(" messages");
        if (matsBrokerDestination.getNumberOfInflightMessages().isPresent()) {
            out.html(" of which ")
                    .DATA(matsBrokerDestination.getNumberOfInflightMessages().getAsLong())
                    .html(" were in-flight.");
        }
        out.html("<br>\n");

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
        out.html("<span id='matsbm_action_message'></span>");
        out.html("<br>");

        out.html("<br>\n");

        boolean anyMessages = false;
        out.html("<div class='matsbm_table_container'>"); // For
        out.html("<table class='matsbm_table_browse_queue'>");
        out.html("<thead>");
        out.html("<th><input type='checkbox' id='matsbm_checkall' autocomplete='off'"
                + " onchange='matsbm_checkall(event)'></th>");
        out.html("<th><input type='button' value='\u2b05 Invert' id='matsbm_checkinvert'"
                + " onclick='matsbm_checkinvert(event)'> Sent</th>");
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
        int count = 0;
        try (MatsBrokerMessageIterable messages = matsBrokerBrowseAndActions.browseQueue(queueId)) {
            for (MatsBrokerMessageRepresentation matsMsg : messages) {
                anyMessages = true;
                out.html("<tr id='matsbm_msgid_").DATA(matsMsg.getMessageSystemId()).html("'>");

                out.html("<td>");
                out.html("<input type='checkbox' class='matsbm_checkmsg' autocomplete='off' data-msgid='")
                        .DATA(matsMsg.getMessageSystemId()).html("' onchange='matsbm_checkmsg(event)'>");
                out.html("</td>");

                out.html("<td>");
                out.html("<a href='?examineMessage&destinationId=").DATA(destinationId)
                        .html("&messageSystemId=").DATA(matsMsg.getMessageSystemId()).html("'>");
                Instant instant = Instant.ofEpochMilli(matsMsg.getTimestamp());
                out.html(Statics.formatTimestamp(instant));
                out.html("</a>");
                out.html("</td>");

                // Found MessageSystemId to be pretty irrelevant in this overview.

                out.html("<td>").DATA(matsMsg.getTraceId()).html("</td>");

                out.html("<td>");
                out.DATA(matsMsg.getInitializingApp() != null ? matsMsg.getInitializingApp() : "{missing init app}");
                out.html("</td>");

                out.html("<td>");
                out.DATA(matsMsg.getInitiatorId() != null ? matsMsg.getInitiatorId() : "{missing init id}");
                out.html("</td>");

                out.html("<td>").DATA(matsMsg.getMessageType()).html(" from").html("</td>");

                out.html("<td>").DATA(matsMsg.getFromStageId()).html("</td>");

                out.html("<td>").DATA(matsMsg.isPersistent() ? "Persistent" : "Non-Persistent").html("</td>");

                out.html("<td>").DATA(matsMsg.isInteractive() ? "Interactive" : "Non-Interactive").html("</td>");

                out.html("<td>");
                out.DATA(matsMsg.getExpirationTimestamp() == 0
                        ? "Never expires"
                        : Statics.formatTimestamp(matsMsg.getExpirationTimestamp()));
                out.html("</td>");

                out.html("</tr>\n");

                // Max out
                if (++count >= MAX_MESSAGES_BROWSER) {
                    break;
                }

            }
        }
        catch (BrokerIOException e) {
            throw new IOException("Can't talk with broker.", e);
        }
        out.html("</tbody>");
        out.html("</table>");
        out.html("<div id='matsbm_num_messages_shown'>Browsing ").DATA(count).html(" messages directly from queue.");
        if (count > 200) {
            out.html(" <i>(Note: Our max is ").DATA(MAX_MESSAGES_BROWSER).html(
                    ", but the message broker might have a smaller max browse. ActiveMQ default is 400)</i>\n");
        }
        out.html("</div></div>");
        if (!anyMessages) {
            out.html("<h1>No messages!</h1>");
        }
        out.html("</div>");
    }
}
