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
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions, Appendable out, String destinationId,
            AccessControl ac) throws IOException {
        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        out.append("<div id='matsbm_page_browse_queue' class='matsbm_report'>\n");
        out.append("<a id='matsbm_back_broker_overview' href='?'>Back to Broker Overview [Esc]</a><br />\n");

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
                out.append(" for Mats Stage '")
                        .append(matsBrokerDestination.getMatsStageId().get()).append("'");
            }
            else {
                // -> Non-Mats Queue. Not really supported, but just to handle it.
                out.append(" is a ");
                out.append(matsBrokerDestination.isDlq() ? "DLQ" : "Queue");
            }
            out.append("<br />\n");
        }

        long lastUpdate = matsBrokerDestination.getLastUpdateBrokerMillis()
                .orElse(matsBrokerDestination.getLastUpdateLocalMillis());
        LocalDateTime lastUpdateDt = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastUpdate), ZoneId.systemDefault());

        out.append("At ").append(lastUpdateDt.toString()).append(" it had ")
                .append(Long.toString(matsBrokerDestination.getNumberOfQueuedMessages()))
                .append(" messages");
        if (matsBrokerDestination.getNumberOfInflightMessages().isPresent()) {
            out.append(" of which ")
                    .append(Long.toString(matsBrokerDestination.getNumberOfInflightMessages().getAsLong()))
                    .append(" were in-flight.");
        }
        out.append("<br />\n");

        out.append("<input type='button' id='matsbm_reissue_bulk' value='Reissue [R]'"
                + " class='matsbm_button matsbm_button_reissue matsbm_button_disabled'"
                + " onclick='matsbm_reissue_bulk(event, \"" + queueId + "\")'>");
        out.append("<input type='button' id='matsbm_delete_bulk' value='Delete [D]'"
                + " class='matsbm_button matsbm_button_delete matsbm_button_disabled'"
                + " onclick='matsbm_delete_propose_bulk(event)'>");
        out.append("<input type='button' id='matsbm_delete_cancel_bulk' value='Cancel Delete [Esc]'"
                + " class='matsbm_button matsbm_button_delete_cancel matsbm_button_hidden'"
                + " onclick='matsbm_delete_cancel_bulk(event)'>");
        out.append("<input type='button' id='matsbm_delete_confirm_bulk' value='Confirm Delete [X]'"
                + " class='matsbm_button matsbm_button_delete matsbm_button_hidden'"
                + " onclick='matsbm_delete_confirmed_bulk(event, \"" + queueId + "\")'>");
        out.append("<span id='matsbm_action_message'></span>");
        out.append("<br/>");

        out.append("<br />\n");

        boolean anyMessages = false;
        out.append("<div class='matsbm_table_container'>"); // For
        out.append("<table class='matsbm_table_browse_queue'>");
        out.append("<thead>");
        out.append("<th><input type='checkbox' id='matsbm_checkall' autocomplete='off'"
                + " onchange='matsbm_checkall(event)'></th>");
        out.append("<th><input type='button' value='\u2b05 Invert' id='matsbm_checkinvert'"
                + " onclick='matsbm_checkinvert(event)'> Sent</th>");
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
        int count = 0;
        try (MatsBrokerMessageIterable messages = matsBrokerBrowseAndActions.browseQueue(queueId)) {
            for (MatsBrokerMessageRepresentation matsMsg : messages) {
                anyMessages = true;
                out.append("<tr id='matsbm_msgid_").append(matsMsg.getMessageSystemId()).append("'>");

                out.append("<td>");
                out.append("<input type='checkbox' class='matsbm_checkmsg' autocomplete='off' data-msgid='")
                        .append(matsMsg.getMessageSystemId()).append("' onchange='matsbm_checkmsg(event)'>");
                out.append("</td>");

                out.append("<td>");
                out.append("<a href='?examineMessage&destinationId=").append(destinationId)
                        .append("&messageSystemId=").append(matsMsg.getMessageSystemId()).append("'>");
                Instant instant = Instant.ofEpochMilli(matsMsg.getTimestamp());
                out.append(Statics.formatTimestamp(instant));
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
                        : Statics.formatTimestamp(matsMsg.getExpirationTimestamp()));
                out.append("</td>");

                out.append("</tr>\n");

                // Max out
                if (++count >= MAX_MESSAGES_BROWSER) {
                    break;
                }

            }
        }
        catch (BrokerIOException e) {
            throw new IOException("Can't talk with broker.", e);
        }
        out.append("</tbody>");
        out.append("</table>");
        out.append("<div id='matsbm_num_messages_shown'>Browsing " + count + " messages directly from queue.");
        if (count > 200) {
            out.append(" <i>(Note: Our max is " + MAX_MESSAGES_BROWSER + ", but the message broker might have a smaller"
                    + " max browse. ActiveMQ default is 400)</i>\n");
        }
        out.append("</div></div>");
        if (!anyMessages) {
            out.append("<h1>No messages!</h1>");
        }
        out.append("</div>");
    }
}
