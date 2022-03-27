package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfo;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerSnapshot;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointGroupBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsStageBrokerRepresentation;
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
        out.html("</div>\n");

        Collection<MatsBrokerDestination> destinations = snapshot.getMatsDestinations().values();

        // :: "Stack up" into a Mats Fabric representation
        MatsFabricBrokerRepresentation stack = MatsFabricBrokerRepresentation
                .stack(destinations);

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
        out.html("<i>(Number of Mats incoming Queues: <b>").DATA(queues)
                .html("</b>, Topics: <b>").DATA(topics)
                .html("</b>, Dead Letter Queues: <b>").DATA(dlqs)
                .html("</b>, Total Mats relevant destinations: <b>").DATA(queues + topics + dlqs)
                .html("</b>)</i><br>");

        // :: Summary

        boolean brokerHasOldMsgs = false;
        boolean brokerHasDlqMsgs = false;
        out.html("<div class='matsbm_overview_message'>\n");
        long totalNumberOfIncomingMessages = stack.getTotalNumberOfIncomingMessages();
        out.html("Total queued messages: <b>").DATA(totalNumberOfIncomingMessages).html("</b>");
        if (totalNumberOfIncomingMessages > 0) {
            long maxStage = stack.getMaxStageNumberOfIncomingMessages();
            out.html(", worst queue has <b>").DATA(maxStage).html("</b> message").html(maxStage > 1 ? "s" : "");
        }
        OptionalLong oldestIncomingO = stack.getOldestIncomingMessageAgeMillis();
        if (oldestIncomingO.isPresent()) {
            long oldestIncoming = oldestIncomingO.getAsLong();
            brokerHasOldMsgs = (oldestIncoming > TOO_OLD);
            out.html(", ").html(brokerHasOldMsgs ? "<span class='matsbm_messages_old'>" : "")
                    .html("oldest message is ")
                    .html("<b>").DATA(Statics.millisSpanToHuman(oldestIncoming)).html("</b> old.")
                    .html(brokerHasOldMsgs ? "</span>" : "");
        }
        out.html("<br>\n");
        long totalNumberOfDeadLetterMessages = stack.getTotalNumberOfDlqMessages();
        out.html("Total DLQed messages: <b>").DATA(totalNumberOfDeadLetterMessages).html("</b>");
        if (totalNumberOfDeadLetterMessages > 0) {
            brokerHasDlqMsgs = true;
            long maxQueue = stack.getMaxStageNumberOfDlqMessages();
            out.html(", worst DLQ has <b>").DATA(maxQueue).html("</b> message").html(maxQueue > 1 ? "s" : "");
        }
        OptionalLong oldestDlq = stack.getOldestDlqMessageAgeMillis();
        if (oldestDlq.isPresent()) {
            out.html(", oldest DLQ message is <b>").DATA(Statics.millisSpanToHuman(oldestDlq.getAsLong()))
                    .html("</b> old.");
        }
        out.html("</div>\n");

        boolean startWithBad = brokerHasDlqMsgs || brokerHasOldMsgs;

        // :: BUTTONS: View All vs View Bad

        out.html("<input type='button' id='matsbm_button_viewall' value='View All'"
                + " class='matsbm_button matsbm_button_viewall" + (startWithBad ? "" : " matsbm_button_active")
                + "' onclick='matsbm_view_all_destinations(event)'>");
        out.html("<input type='button' id='matsbm_button_viewbad' value='View Bad'"
                + " class='matsbm_button matsbm_button_viewbad" + (startWithBad ? " matsbm_button_active" : "")
                + "' onclick='matsbm_view_bad_destinations(event)'>");
        out.html("<br>\n");

        // :: ToC
        out.html("<div id='matsbs_toc_heading'>EndpointGroups ToC</div>\n");
        out.html("<table id='matsbm_table_toc'>\n");
        for (MatsEndpointGroupBrokerRepresentation endpointGroup : stack.getEndpointGroups().values()) {
            OptionalLong oldestIncomO = endpointGroup.getOldestIncomingMessageAgeMillis();
            long oldestIncoming = oldestIncomO.orElse(-1);
            boolean hasOldMsgs = oldestIncoming > TOO_OLD;
            long dlqMessages = endpointGroup.getTotalNumberOfDlqMessages();
            boolean hasDlqMsgs = dlqMessages > 0;

            out.html("<tr class='matsbm_toc_endpointgroup")
                    .html(hasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                    .html(hasDlqMsgs ? " matsbm_marker_has_dlqs" : "")
                    .html(startWithBad && !(hasOldMsgs || hasDlqMsgs) ? " matsbm_marker_hidden_toc" : "")
                    .html("'>");
            out.html("<td><div class='matsbm_toc_content'>");
            String endpointGroupId = endpointGroup.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : endpointGroup.getEndpointGroup();
            out.html("<a href='#").DATA(endpointGroupId).html("'>")
                    .DATA(endpointGroupId)
                    .html("</a>");
            out.html("</div></td><td><div class='matsbm_toc_content'>");
            long incomingMessages = endpointGroup.getTotalNumberOfIncomingMessages();
            out.html("Q:<b>").DATA(incomingMessages).html("</b>");
            if (incomingMessages > 0) {
                long maxStage = endpointGroup.getMaxStageNumberOfIncomingMessages();
                out.html(", worst:<b>").DATA(maxStage).html("</b>");
            }
            if (oldestIncomO.isPresent()) {
                out.html(", ").html(hasOldMsgs ? "<span class='matsbm_messages_old'>" : "")
                        .html("oldest:")
                        .html("<b>").DATA(Statics.millisSpanToHuman(oldestIncoming)).html("</b>")
                        .html(hasOldMsgs ? "</span>" : "");
            }
            out.html("</div></td><td><div class='matsbm_toc_content'>");
            out.html("DLQ:<b>").DATA(dlqMessages).html("</b>");
            if (dlqMessages > 0) {
                long maxQueue = endpointGroup.getMaxStageNumberOfDlqMessages();
                out.html(", worst:<b>").DATA(maxQueue).html("</b>");
            }
            OptionalLong oldestDlqO = endpointGroup.getOldestDlqMessageAgeMillis();
            if (oldestDlqO.isPresent()) {
                out.html(", oldest:<b>").DATA(Statics.millisSpanToHuman(oldestDlqO.getAsLong()))
                        .html("</b>.");
            }
            out.html("</div></td></tr>");
        }
        out.html("</table>");
        out.html("<br>\n");

        // :: Global DLQ
        if (stack.getDefaultGlobalDlq().isPresent()) {
            MatsBrokerDestination globalDlq = stack.getDefaultGlobalDlq().get();
            boolean hasDlqMsgs = globalDlq.getNumberOfQueuedMessages() > 0;
            out.html("<div class='matsbm_endpoint_group")
                    .html(hasDlqMsgs ? " matsbm_marker_has_dlqs" : "")
                    .html(startWithBad && !hasDlqMsgs ? " matsbm_marker_hidden_epgrp" : "")
                    .html("'>");
            out.html("<h2>Global DLQ</h2><br>");
            out.html("<table class='matsbm_table_endpointgroup'>");
            out.html("<tr class='matsbm_endpoint_group_row")
                    .html(hasDlqMsgs ? " matsbm_marker_has_dlqs" : "")
                    .html(startWithBad && !hasDlqMsgs ? " matsbm_marker_hidden_row" : "")
                    .html("'>");
            out.html("<td>");
            out.html("<div class='matsbm_epid matsbm_epid_queue'>")
                    .DATA(globalDlq.getDestinationName())
                    .html("</div>");
            out.html("</td><td><div class='matsbm_label matsbm_label_queue'>Queue</div></td>");

            out.html("<td>");
            out.html("<div class='matsbm_stage'>")
                    .DATA(globalDlq.getFqDestinationName());
            out_queueCount(out, globalDlq);
            out.html("</div>");
            out.html("</td>");
            out.html("</table>");

            out.html("</div>");
        }

        // :: Foreach EndpointGroup
        for (MatsEndpointGroupBrokerRepresentation endpointGroup : stack.getEndpointGroups().values()) {
            // :: EndpointGroup

            boolean epgrHasOldMsgs = endpointGroup.getOldestIncomingMessageAgeMillis().orElse(-1) > TOO_OLD;
            boolean epgrHasDlqsmsgs = endpointGroup.getTotalNumberOfDlqMessages() > 0;

            String endpointGroupId = endpointGroup.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : endpointGroup.getEndpointGroup();
            out.html("<div class='matsbm_endpoint_group")
                    .html(epgrHasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                    .html(epgrHasDlqsmsgs ? " matsbm_marker_has_dlqs" : "")
                    .html(startWithBad && !(epgrHasOldMsgs || epgrHasDlqsmsgs) ? " matsbm_marker_hidden_epgrp" : "")
                    .html("' id='").DATA(endpointGroupId).html("'>\n");
            out.html("<a href='#").DATA(endpointGroupId).html("'>");
            out.html("<h2>").DATA(endpointGroupId).html("</h2></a><br>\n");

            // :: Foreach Endpoint
            out.html("<table class='matsbm_table_endpointgroup'>");
            for (MatsEndpointBrokerRepresentation endpoint : endpointGroup.getEndpoints().values()) {

                boolean epHasOldMsgs = endpoint.getOldestIncomingMessageAgeMillis().orElse(-1) > TOO_OLD;
                boolean epHasDlqsMsgs = endpoint.getTotalNumberOfDlqMessages() > 0;

                out.html("<tr class='matsbm_endpoint_group_row")
                        .html(epHasOldMsgs ? " matsbm_marker_has_old_msgs" : "")
                        .html(epHasDlqsMsgs ? " matsbm_marker_has_dlqs" : "")
                        .html(startWithBad && !(epHasOldMsgs || epHasDlqsMsgs) ? " matsbm_marker_hidden_row" : "")
                        .html("'>");
                String endpointId = endpoint.getEndpointId();
                Map<Integer, MatsStageBrokerRepresentation> stages = endpoint.getStages();

                // :: Find whether endpoint is a queue or topic.
                // There will always be at least one stage, otherwise the endpoint wouldn't be defined.
                MatsStageBrokerRepresentation first = stages.values().iterator().next();
                // There will either be an incoming, or a DLQ, otherwise the stage wouldn't be defined.
                MatsBrokerDestination firstDestinationOrDlq = first.getIncomingDestination()
                        .orElseGet(() -> first.getDlqDestination()
                                .orElseThrow(() -> new AssertionError("Missing both Incoming and DLQ destinations!")));

                boolean privateEp = endpointId.contains(".private.");
                boolean queue = firstDestinationOrDlq.getDestinationType() == DestinationType.QUEUE;

                out.html("<td><div class='matsbm_epid matsbm_epid")
                        .html(queue ? "_queue" : "_topic")
                        .html(privateEp ? "_private" : "")
                        .html("'>")
                        .DATA(endpointId).html("</div></td>");

                out.html("<td><div class='matsbm_label matsbm_label")
                        .html(queue ? "_queue" : "_topic")
                        .html(privateEp ? "_private" : "")
                        .html("'>")
                        .DATA(queue ? "Queue" : "Topic").html("</div></td>");

                // :: Foreach Stage
                out.html("<td>");
                boolean single = (stages.size() == 1) && (stages.values().iterator().next().getStageIndex() == 0);
                for (MatsStageBrokerRepresentation stage : stages.values()) {
                    boolean initial = stage.getStageIndex() == 0;
                    out.html("<div class='matsbm_stage'>");
                    out.html(initial
                            ? ("<div class='matsbm_stage_initial'>" + (single ? "single" : "initial") + "</div>")
                            : "S" + stage.getStageIndex());
                    Optional<MatsBrokerDestination> incomingDest = stage.getIncomingDestination();
                    if (incomingDest.isPresent()) {
                        out_queueCount(out, incomingDest.get());
                    }

                    Optional<MatsBrokerDestination> dlqDest = stage.getDlqDestination();
                    if (dlqDest.isPresent()) {
                        out_queueCount(out, dlqDest.get());
                    }
                    out.html("</div>"); // /matsbm_stage
                }
                out.html("</td>");
                out.html("</tr>\n");
            }
            out.html("</table>\n");
            out.html("</div>\n");
        }
        out.html("</div>\n");
    }

    private static void out_queueCount(Outputter out, MatsBrokerDestination destination) throws IOException {
        boolean isDlq = destination.isDlq();
        if (destination.getDestinationType() == DestinationType.QUEUE) {
            // -> Queue
            String style = isDlq
                    ? destination.getNumberOfQueuedMessages() == 0 ? "dlq_zero" : "dlq"
                    : destination.getNumberOfQueuedMessages() == 0 ? "queue_zero" : "queue";
            out.html("<a class='").html(style).html("' href='?browse&destinationId=")
                    .html("queue:")
                    .DATA(destination.getDestinationName())
                    .html("'>");
        }
        else {
            // -> Topic
            out.html("<div class='topic'>");
        }
        out.html(isDlq ? "DLQ:" : "")
                .DATA(destination.getNumberOfQueuedMessages());
        out.html(destination.getDestinationType() == DestinationType.QUEUE ? "</a>" : "</div>");

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
