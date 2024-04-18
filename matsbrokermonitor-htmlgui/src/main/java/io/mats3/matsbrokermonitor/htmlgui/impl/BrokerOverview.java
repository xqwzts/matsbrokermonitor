package io.mats3.matsbrokermonitor.htmlgui.impl;

import static io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType.STANDARD;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfo;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerSnapshot;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;
import io.mats3.matsbrokermonitor.api.MatsFabricAggregatedRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricAggregatedRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricAggregatedRepresentation.MatsEndpointGroupBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricAggregatedRepresentation.MatsStageBrokerRepresentation;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui.AccessControl;

/**
 * @author Endre Stølsvik 2022-03-13 23:32 - http://stolsvik.com/, endre@stolsvik.com
 */
class BrokerOverview {

    private static final long TOO_OLD = 10 * 60 * 1000;

    static void gui_BrokerOverview(MatsBrokerMonitor matsBrokerMonitor, Outputter out,
            Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        out.html("<div id='matsbm_page_broker_overview' class='matsbm_report'>\n");
        out.html("<div class='matsbm_heading'>");

        Optional<BrokerSnapshot> snapshotO = matsBrokerMonitor.getSnapshot();
        if (!snapshotO.isPresent()) {
            out.html("<h1>Have not gotten an update from the broker yet!</h1>");
            out.html("</div>\n");
            // Don't output last </div>, as caller does it.
            return;
        }

        BrokerSnapshot snapshot = snapshotO.get();

        Optional<BrokerInfo> brokerInfoO = snapshot.getBrokerInfo();
        if (brokerInfoO.isPresent()) {
            BrokerInfo brokerInfo = brokerInfoO.get();
            out.html("Broker <h1>'").DATA(brokerInfo.getBrokerName()).html("'</h1>");
            out.html("   of type ").DATA(brokerInfo.getBrokerType());
        }
        else {
            out.html("<h2>Unknown broker</h2>");
        }
        out.html("</div>\n"); // /matsbm_heading

        Collection<MatsBrokerDestination> destinations = snapshot.getMatsDestinations().values();

        // "Stack up" into a Mats Fabric representation
        MatsFabricAggregatedRepresentation stack = MatsFabricAggregatedRepresentation
                .stack(destinations);

        // ===== OVERALL FABRIC INFORMATION

        out.html("Updated as of: <b>").DATA(Statics.formatTimestampSpan(snapshot.getLastUpdateLocalMillis()));
        if (snapshot.getLastUpdateBrokerMillis().isPresent()) {
            out.html("</b> - broker time: <b>")
                    .DATA(Statics.formatTimestamp(snapshot.getLastUpdateBrokerMillis().getAsLong()));
        }
        out.html("</b> <i>(all ages on queues are wrt. this update time)</i><br>\n");
        int dlqs = 0;
        int queues = 0;
        int topics = 0;
        for (MatsBrokerDestination destination : destinations) {
            if (destination.isDlq()) {
                dlqs++;
            }
            else if (destination.getDestinationType() == DestinationType.QUEUE) {
                queues++;
            }
            else {
                topics++;
            }
        }
        out.html("<i>(<b>").DATA(stack.getEndpoints().size())
                .html("</b> Mats Endpoints over <b>").DATA(stack.getEndpointGroups().size())
                .html("</b> EndpointGroups | Incoming Queues: <b>").DATA(queues)
                .html("</b>, Topics: <b>").DATA(topics)
                .html("</b>, Dead Letter Queues: <b>").DATA(dlqs)
                .html("</b> = total Mats relevant destinations: <b>").DATA(queues + topics + dlqs)
                .html("</b>");
        if (snapshot.getStatisticsRequestReplyLatencyMillis().isPresent()) {
            out.html(" - Stats update time: <b>")
                    .DATA(Math.round(snapshot.getStatisticsRequestReplyLatencyMillis().getAsDouble() * 100d) / 100d)
                    .html(" ms</b>");
        }
        out.html(")</i><br>");

        // ==== QUEUE & DLQ SUMMARY

        // ::: Heading Queued Messages
        boolean brokerHasTooOldMsgs = false;
        out.html("<div class='matsbm_overview_message'>\n");
        long totalNumberOfIncomingMessages = destinations.stream()
                .filter(dest -> !dest.isDlq())
                .mapToLong(MatsBrokerDestination::getNumberOfQueuedMessages)
                .sum();
        out.html("Total queued messages: <b>").DATA(totalNumberOfIncomingMessages).html("</b>");
        if (totalNumberOfIncomingMessages > 0) {
            long maxStage = destinations.stream()
                    .filter(dest -> !dest.isDlq())
                    .mapToLong(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .max().orElse(0);
            out.html(", worst queue has <b>").DATA(maxStage).html("</b> message").html(maxStage > 1 ? "s" : "");
        }
        OptionalLong oldestIncomingO = destinations.stream()
                .filter(dest -> !dest.isDlq())
                .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .max();
        if (oldestIncomingO.isPresent()) {
            long oldestIncoming = oldestIncomingO.getAsLong();
            brokerHasTooOldMsgs = (oldestIncoming > TOO_OLD);
            out.html(", ")
                    .html(brokerHasTooOldMsgs ? "<span class='matsbm_messages_old'>" : "")
                    .html("oldest message is ")
                    .html("<b>").DATA(Statics.millisSpanToHuman(oldestIncoming)).html("</b> old.")
                    .html(brokerHasTooOldMsgs ? "</span>" : "");
        }
        out.html("<br>\n");

        // ::: Heading DLQed Messages
        boolean brokerHasDlqMsgs;
        long totalNumberOfDlqMessages = destinations.stream()
                .filter(MatsBrokerDestination::isDlq)
                .mapToLong(MatsBrokerDestination::getNumberOfQueuedMessages)
                .sum();
        brokerHasDlqMsgs = totalNumberOfDlqMessages > 0;
        out.html(brokerHasDlqMsgs ? "<span class='matsbm_messages_dlq'>" : "")
                .html("Total DLQed messages: <b>").DATA(totalNumberOfDlqMessages).html("</b>");
        if (brokerHasDlqMsgs) {
            long maxQueue = destinations.stream()
                    .filter(MatsBrokerDestination::isDlq)
                    .mapToLong(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .max().orElse(0);
            out.html(", worst DLQ has <b>").DATA(maxQueue).html("</b> message").html(maxQueue > 1 ? "s" : "");
        }
        out.html(brokerHasDlqMsgs ? "</span>" : "");
        OptionalLong oldestDlq = destinations.stream().filter(MatsBrokerDestination::isDlq)
                .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .max();
        if (oldestDlq.isPresent()) {
            out.html(", oldest DLQ message is <b>").DATA(Statics.millisSpanToHuman(oldestDlq.getAsLong()))
                    .html("</b> old.");
        }
        out.html("</div>\n");

        // :: Decide whether to show all or bad only.
        // Start with deciding based on whether there are "bad" (DLQ or old)..
        boolean showBadOnly = brokerHasDlqMsgs || brokerHasTooOldMsgs;
        // .. override with any "show" parameter
        String[] showParam = requestParameters.get("show");
        if (showParam != null) {
            if ("bad".equals(showParam[0])) {
                showBadOnly = true;
            }
            else if ("all".equals(showParam[0])) {
                showBadOnly = false;
            }
            else {
                throw new IllegalArgumentException("Parameter 'show' can only be 'all' or 'bad'");
            }
        }

        // ===== BUTTONS: View All vs View Bad

        out.html("<input type='button' id='matsbm_button_show_all' value='Show All [a]'"
                + " class='matsbm_button matsbm_button_show_all" + (showBadOnly ? "" : " matsbm_button_active")
                + "' onclick='matsbm_button_show_all_destinations(event)'>");

        out.html("<input type='button' id='matsbm_button_show_bad' value='Show Bad [b]'"
                + " class='matsbm_button matsbm_button_show_bad" + (showBadOnly ? " matsbm_button_active" : "")
                + "' onclick='matsbm_button_show_bad_destinations(event)'>");

        out.html("<input type='button' id='matsbm_button_forceupdate' value='Update Now! [u]'"
                + " class='matsbm_button matsbm_button_forceupdate"
                + "' onclick='matsbm_button_forceupdate(event)'>");
        out.html("<span id='matsbm_action_message'></span>");
        out.html("<br>\n");

        // ===== TABLE OF CONTENTS

        out.html("<div id='matsbs_toc_heading'>EndpointGroups ToC</div>\n");
        out.html("<table id='matsbm_table_toc'>\n");
        for (MatsEndpointGroupBrokerRepresentation endpointGroup : stack.getEndpointGroups().values()) {
            OptionalLong oldestIncomO = endpointGroup.getOldestStageHeadMessageAgeMillis(STANDARD);
            long oldestIncoming = oldestIncomO.orElse(-1);
            boolean hasOldMsgs = oldestIncoming > TOO_OLD;
            long dlqMessages = endpointGroup.getTotalNumberOfQueuedMessages(StageDestinationType.DEAD_LETTER_QUEUE);
            boolean hasDlqMsgs = dlqMessages > 0;

            // ?: Should we only show bad, but there is no DLQs or old messages?
            if (showBadOnly && !(hasDlqMsgs || hasOldMsgs)) {
                // -> Yes, only show bad, but this is not bad, so skip.
                continue;
            }

            out.html("<tr class='matsbm_toc_endpointgroup")
                    .html(hasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                    .html(hasDlqMsgs ? " matsbm_marker_has_dlqs" : "")
                    .html("'>");
            out.html("<td><div class='matsbm_toc_content'>");
            String endpointGroupId = endpointGroup.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : endpointGroup.getEndpointGroup();
            out.html("<a href='#").DATA(endpointGroupId).html("'>")
                    .DATA(endpointGroupId)
                    .html("</a>");
            out.html("</div></td><td><div class='matsbm_toc_content'>");
            long incomingMessages = endpointGroup.getTotalNumberOfQueuedMessages(STANDARD);
            out.html("ΣQ:<b>").DATA(incomingMessages).html("</b>");
            if (incomingMessages > 0) {
                long maxStage = endpointGroup.getMaxStageNumberOfMessages(STANDARD);
                out.html(", worst:<b>").DATA(maxStage).html("</b>");
            }
            if (oldestIncomO.isPresent()) {
                out.html(", ").html(hasOldMsgs ? "<span class='matsbm_messages_old'>" : "")
                        .html("oldest:")
                        .html("<b>").DATA(Statics.millisSpanToHuman(oldestIncoming)).html("</b>")
                        .html(hasOldMsgs ? "</span>" : "");
            }
            out.html("</div></td><td><div class='matsbm_toc_content'>");
            out.html(dlqMessages > 0 ? "<span class='matsbm_messages_dlq'>" : "")
                    .html("ΣDLQ:<b>").DATA(dlqMessages).html("</b>");
            if (dlqMessages > 0) {
                long maxQueue = endpointGroup.getMaxStageNumberOfMessages(StageDestinationType.DEAD_LETTER_QUEUE);
                out.html(", worst:<b>").DATA(maxQueue).html("</b>");
            }
            out.html("</span>");
            OptionalLong oldestDlqO = endpointGroup.getOldestStageHeadMessageAgeMillis(
                    StageDestinationType.DEAD_LETTER_QUEUE);
            if (oldestDlqO.isPresent()) {
                out.html(", oldest:<b>").DATA(Statics.millisSpanToHuman(oldestDlqO.getAsLong()))
                        .html("</b>.");
            }
            out.html("</div></td></tr>");
        }
        out.html("</table>");
        out.html("<br>\n");

        // ===== ENDPOINT GROUP BOXES

        // :: Global DLQ
        if (stack.getBrokerDefaultGlobalDlq().isPresent()) {
            MatsBrokerDestination globalDlq = stack.getBrokerDefaultGlobalDlq().get();
            boolean hasDlqMsgs = globalDlq.getNumberOfQueuedMessages() > 0;
            // ?: Do we have DLQs, or should we show all?
            if (hasDlqMsgs || !showBadOnly) {

                out.html("<div class='matsbm_endpoint_group")
                        .html(hasDlqMsgs ? " matsbm_marker_has_dlqs" : "")
                        .html("'>");
                out.html("<h2>Global DLQ</h2><br>");
                out.html("<table class='matsbm_table_endpointgroup'>");
                out.html("<tr class='matsbm_endpoint_group_row")
                        .html(hasDlqMsgs ? " matsbm_marker_has_dlqs" : "")
                        .html("'>");
                out.html("<td>");
                out.html("<div class='matsbm_bo_stageid matsbm_bo_stageid_queue'>")
                        .DATA(globalDlq.getDestinationName())
                        .html("</div>");
                out.html("</td><td><div class='matsbm_label matsbm_label_queue'>Queue</div></td>");

                out.html("<td>");
                out.html("<div class='matsbm_stage_box'>")
                        .DATA(globalDlq.getFqDestinationName());
                out_queueCount(out, globalDlq);
                out.html("</div>");
                out.html("</td>");
                out.html("</table>");

                out.html("</div>");
            }
        }

        // :: Foreach EndpointGroup
        for (MatsEndpointGroupBrokerRepresentation endpointGroup : stack.getEndpointGroups().values()) {
            // :: EndpointGroup

            boolean epgrHasOldMsgs = endpointGroup.getAllDestinations().stream()
                    .filter(dest -> !dest.isDlq())
                    .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                    .anyMatch(age -> age.orElse(-1) > TOO_OLD);

            boolean epgrHasDlqsmsgs = endpointGroup.getAllDestinations().stream()
                    .filter(MatsBrokerDestination::isDlq)
                    .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .anyMatch(count -> count > 0);

            // ?: Should we only show bad, but there is no DLQs or old messages in this endpoint group?
            if (showBadOnly && !(epgrHasDlqsmsgs || epgrHasOldMsgs)) {
                // -> Yes, only show bad, but this is not bad, so skip.
                continue;
            }

            String endpointGroupId = endpointGroup.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : endpointGroup.getEndpointGroup();
            out.html("<div class='matsbm_endpoint_group")
                    .html(epgrHasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                    .html(epgrHasDlqsmsgs ? " matsbm_marker_has_dlqs" : "")
                    .html("' id='").DATA(endpointGroupId).html("'>\n");
            out.html("<a href='#").DATA(endpointGroupId).html("'>");
            out.html("<h2>").DATA(endpointGroupId).html("</h2></a><br>\n");

            // :: Foreach Endpoint
            out.html("<table class='matsbm_table_endpointgroup'>");
            for (MatsEndpointBrokerRepresentation endpoint : endpointGroup.getEndpoints().values()) {

                boolean epHasOldMsgs = endpoint.getAllDestinations().stream()
                        .filter(dest -> !dest.isDlq())
                        .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                        .anyMatch(age -> age.orElse(-1) > TOO_OLD);

                boolean epHasDlqsMsgs = endpoint.getAllDestinations().stream()
                        .filter(MatsBrokerDestination::isDlq)
                        .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                        .anyMatch(count -> count > 0);

                // ?: Should we only show bad, but there is no DLQs or old messages in this endpoint?
                if (showBadOnly && !(epHasDlqsMsgs || epHasOldMsgs)) {
                    // -> Yes, only show bad, but this is not bad, so skip.
                    continue;
                }

                out.html("<tr class='matsbm_endpoint_group_row")
                        .html(epHasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                        .html(epHasDlqsMsgs ? " matsbm_marker_has_dlqs" : "")
                        .html("'>");
                String endpointId = endpoint.getEndpointId();
                Map<Integer, MatsStageBrokerRepresentation> stages = endpoint.getStages();

                // :: Find whether endpoint is a queue or topic.
                // There will always be at least one stage, otherwise the endpoint wouldn't be defined.
                MatsStageBrokerRepresentation first = stages.values().iterator().next();
                // There will either be an incoming, or a DLQ, otherwise the stage wouldn't be defined.
                MatsBrokerDestination firstDestinationOrDlq = first.getDestination(STANDARD)
                        .orElseGet(() -> first.getDestination(StageDestinationType.DEAD_LETTER_QUEUE)
                                .orElseThrow(() -> new AssertionError("Missing both Incoming and DLQ destinations!")));

                boolean privateEp = endpointId.contains(".private.");
                boolean queue = firstDestinationOrDlq
                        .getDestinationType() == DestinationType.QUEUE;

                out.html("<td><div class='matsbm_bo_stageid")
                        .html(" matsbm_bo_stageid").html(queue ? "_queue" : "_topic").html(privateEp ? "_private" : "")
                        .html("'>")
                        .DATA(endpointId).html("</div></td>");

                out.html("<td><div class='matsbm_label")
                        .html(" matsbm_label").html(queue ? "_queue" : "_topic").html(privateEp ? "_private" : "")
                        .html("'>")
                        .DATA(queue ? "Queue" : "Topic").html("</div></td>");

                // :: Foreach Stage
                out.html("<td>");
                boolean single = (stages.size() == 1) && (stages.values().iterator().next().getStageIndex() == 0);
                for (MatsStageBrokerRepresentation stage : stages.values()) {
                    boolean initial = stage.getStageIndex() == 0;
                    out.html("<div class='matsbm_stage_box'>");
                    out.html(initial
                            ? ("<div class='matsbm_stage_initial'>" + (single ? "single" : "initial") + "</div>")
                            : "S" + stage.getStageIndex());

                    Optional<MatsBrokerDestination> incomingDest = stage.getDestination(STANDARD);
                    boolean incomingPresent = incomingDest.isPresent();
                    if (incomingPresent) {
                        out_queueCount(out, incomingDest.get());
                    }

                    int elidedCount = 0;
                    Optional<MatsBrokerDestination> npiaDest = stage.getDestination(
                            StageDestinationType.NON_PERSISTENT_INTERACTIVE);
                    if (npiaDest.isPresent()) {
                        if (!incomingPresent || (npiaDest.get().getNumberOfQueuedMessages() > 0)) {
                            out_queueCount(out, npiaDest.get());
                        }
                        else {
                            elidedCount++;
                        }
                    }

                    Optional<MatsBrokerDestination> dlqDest = stage.getDestination(
                            StageDestinationType.DEAD_LETTER_QUEUE);
                    if (dlqDest.isPresent()) {
                        if (!incomingPresent || (dlqDest.get().getNumberOfQueuedMessages() > 0)) {
                            out_queueCount(out, dlqDest.get());
                        }
                        else {
                            elidedCount++;
                        }
                    }

                    Optional<MatsBrokerDestination> npiaDlqDest = stage.getDestination(
                            StageDestinationType.DEAD_LETTER_QUEUE_NON_PERSISTENT_INTERACTIVE);
                    if (npiaDlqDest.isPresent()) {
                        if (!incomingPresent || (npiaDlqDest.get().getNumberOfQueuedMessages() > 0)) {
                            out_queueCount(out, npiaDlqDest.get());
                        }
                        else {
                            elidedCount++;
                        }
                    }

                    Optional<MatsBrokerDestination> mutedDlqDest = stage.getDestination(
                            StageDestinationType.DEAD_LETTER_QUEUE_MUTED);
                    if (mutedDlqDest.isPresent()) {
                        if (!incomingPresent || (mutedDlqDest.get().getNumberOfQueuedMessages() > 0)) {
                            out_queueCount(out, mutedDlqDest.get());
                        }
                        else {
                            elidedCount++;
                        }
                    }

                    Optional<MatsBrokerDestination> wiretapDest = stage.getDestination(
                            StageDestinationType.WIRETAP);
                    if (wiretapDest.isPresent()) {
                        if (!incomingPresent || (wiretapDest.get().getNumberOfQueuedMessages() > 0)) {
                            out_queueCount(out, wiretapDest.get());
                        }
                        else {
                            elidedCount++;
                        }
                    }

                    if (elidedCount > 0) {
                        out.html("<i><b>+").DATA(elidedCount).html("q</b></i>");
                    }

                    out.html("</div>"); // /matsbm_stage_box
                }
                out.html("</td>");
                out.html("</tr>\n");
            }
            out.html("</table>\n");
            out.html("</div>\n");
        }

        // :: Remaining Queues and Topics.
        boolean rqtHasOldMsgs = stack.getRemainingDestinations().stream()
                .anyMatch(dest -> dest.getHeadMessageAgeMillis().orElse(0) > TOO_OLD);

        boolean rqtHasDlqs = stack.getRemainingDestinations().stream()
                .filter(MatsBrokerDestination::isDlq)
                .anyMatch(dest -> dest.getNumberOfQueuedMessages() > 0);

        // ?: Remaining Qs/Ts: If we're NOT only showing bad, OR there are bads, then show it.
        if (!showBadOnly || (rqtHasOldMsgs || rqtHasDlqs)) {
            // -> Let's show it.
            String endpointGroupId = "Remaining Queues and Topics";
            out.html("<br/><div class='matsbm_endpoint_group matsbm_endpoint_group_remaining")
                    .html(rqtHasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                    .html("' id='").DATA(endpointGroupId).html("'>\n");
            out.html("<a href='#").DATA(endpointGroupId).html("'>");
            out.html("<h2><i>Remaining Queues and Topics (not"
                    + " " + Statics.MATS3_HTML + "-related)</i></h2></a><br>\n");

            // :: Foreach Endpoint
            out.html("<table class='matsbm_table_endpointgroup'>");
            for (MatsBrokerDestination destination : stack.getRemainingDestinations()) {

                boolean destinationHasOldMsgs = destination.getHeadMessageAgeMillis().orElse(0) > TOO_OLD;
                boolean destinationHasDlqs = destination.isDlq() && (destination.getNumberOfQueuedMessages() > 0);

                // ?: Should we only show bad, but there is no DLQs or old messages in this endpoint?
                if (showBadOnly && !(destinationHasOldMsgs || destinationHasDlqs)) {
                    // -> Yes, only show bad, but this is not bad, so skip.
                    continue;
                }

                out.html("<tr class='matsbm_endpoint_group_row")
                        .html(destinationHasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                        .html(destinationHasDlqs ? " matsbm_marker_has_dlqs" : "")
                        .html("'>");
                String endpointId = destination.getDestinationName();

                // :: Find whether endpoint is a queue or topic.
                // There will either be an incoming, or a DLQ, otherwise the stage wouldn't be defined.
                boolean queue = destination.getDestinationType() == DestinationType.QUEUE;

                out.html("<td><div class='matsbm_bo_stageid")
                        .html(" matsbm_bo_stageid").html(queue ? "_queue" : "_topic")
                        .html("'>")
                        .DATA(endpointId).html("</div></td>");

                out.html("<td><div class='matsbm_label")
                        .html(" matsbm_label").html(queue ? "_queue" : "_topic")
                        .html("'>")
                        .DATA(queue ? "Queue" : "Topic").html("</div></td>");

                // :: Foreach Stage
                out.html("<td>");
                out.html("<div class='matsbm_stage_box'>");
                out_queueCount(out, destination);
                out.html("<div class='matsbm_stage_initial'> <i>&nbsp;&nbsp;")
                        .DATA(destination.getFqDestinationName())
                        .html("</i></div>");
                out.html("</div>"); // /matsbm_stage_box
                out.html("</td>");
                out.html("</tr>\n");
            }
            out.html("</table>\n");
            out.html("</div>\n");
        }

        // Don't output last </div>, as caller does it.
    }

    static void out_queueCount(Outputter out, MatsBrokerDestination destination) throws IOException {
        out_queueCount(out, destination, false);
    }

    static void out_queueCount(Outputter out, MatsBrokerDestination destination, boolean fromQueueBrowse)
            throws IOException {
        boolean isDlq = destination.isDlq();
        long numberOfQueuedMessages = destination.getNumberOfQueuedMessages();
        if (destination.getDestinationType() == DestinationType.QUEUE) {
            // -> Queue

            String cssClass;
            String text;

            if (destination.getStageDestinationType().isPresent()) {
                switch (destination.getStageDestinationType().get()) {
                    case STANDARD:
                        cssClass = numberOfQueuedMessages == 0 ? "queue_zero" : "queue";
                        text = fromQueueBrowse ? "Standard:" : "";
                        break;
                    case NON_PERSISTENT_INTERACTIVE:
                        cssClass = numberOfQueuedMessages == 0 ? "queue_zero" : "queue";
                        text = fromQueueBrowse ? "NonPersistent Interactive:" : "npia:";
                        break;
                    case DEAD_LETTER_QUEUE:
                        cssClass = numberOfQueuedMessages == 0 ? "dlq_zero" : "dlq";
                        text = "DLQ:";
                        break;
                    case DEAD_LETTER_QUEUE_NON_PERSISTENT_INTERACTIVE:
                        cssClass = numberOfQueuedMessages == 0 ? "dlq_zero" : "dlq";
                        text = fromQueueBrowse ? "DLQ NonPersistent Interactive:" : "DLQ npia:";
                        break;
                    case DEAD_LETTER_QUEUE_MUTED:
                        // FIX:
                        cssClass = numberOfQueuedMessages == 0 ? "queue_zero" : "queue";
                        text = "Muted DLQ:";
                        break;
                    case WIRETAP:
                        cssClass = numberOfQueuedMessages == 0 ? "queue_zero" : "queue";
                        text = "Wiretap:";
                        break;
                    default:
                        // ?? How did this happen.
                        cssClass = "queue";
                        text = "Unknown:";
                }
            }
            else {
                cssClass = isDlq
                        ? numberOfQueuedMessages == 0 ? "dlq_zero" : "dlq"
                        : numberOfQueuedMessages == 0 ? "queue_zero" : "queue";
                text = isDlq
                        ? "Non-" + Statics.MATS3_HTML + " DLQ: "
                        : "Non-" + Statics.MATS3_HTML + " Queue: ";
            }
            // Use these resolves to get the correct css class and text
            out.html("<a class='").html(cssClass).html("' href='?browse&destinationId=")
                    .html("queue:")
                    .DATA(destination.getDestinationName())
                    .html(numberOfQueuedMessages == 1 ? "&autojump" : "")
                    .html("'>")
                    .html(text);
            out.DATA(numberOfQueuedMessages);
            out.html("</a>");
        }
        else {
            // -> Topic (These won't have several StageDestinationTypes)
            out.html("<div class='topic'>");
            if (destination.getStageDestinationType().isEmpty()) {
                out.html("Non-" + Statics.MATS3_HTML + " Topic: ");
            }
            out.DATA(numberOfQueuedMessages);
            out.html("</div>");
        }

        long age = destination.getHeadMessageAgeMillis().orElse(0);
        if (age > 0) {
            boolean markOld = (age > TOO_OLD) && (!isDlq);
            out.html("<div class='matsbm_age")
                    .html(markOld ? " matsbm_messages_old" : "")
                    .html("'>(")
                    .DATA(Statics.millisSpanToHuman(age))
                    .html(")</div>");
        }
    }
}
