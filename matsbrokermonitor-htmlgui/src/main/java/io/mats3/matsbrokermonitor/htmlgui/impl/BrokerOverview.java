package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfo;
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
    static void gui_BrokerOverview(MatsBrokerMonitor matsBrokerMonitor, Appendable out,
            Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        out.append("<div class='matsbm_report matsbm_broker'>\n");
        out.append("  <div class='matsbm_heading'>");
        Optional<BrokerInfo> brokerInfoO = matsBrokerMonitor.getBrokerInfo();
        if (brokerInfoO.isPresent()) {
            BrokerInfo brokerInfo = brokerInfoO.get();
            out.append("Broker <h1>'").append(brokerInfo.getBrokerName()).append("'</h1>");
            out.append("   of type ").append(brokerInfo.getBrokerType());
        }
        else {
            out.append("<h2>Unknown broker</h2>");
        }
        out.append("  </div>\n");

        Map<String, MatsBrokerDestination> matsDestinations = matsBrokerMonitor.getMatsDestinations();
        MatsFabricBrokerRepresentation stack = MatsFabricBrokerRepresentation.stack(matsDestinations.values());

        // :: ToC
        out.append("<b>EndpointGroups ToC</b><br />\n");
        for (MatsEndpointGroupBrokerRepresentation service : stack.getMatsEndpointGroupBrokerRepresentations()
                .values()) {
            String endpointGroupId = service.getEndpointGroup().trim().isEmpty()
                    ? "{empty string}"
                    : service.getEndpointGroup();
            out.append("&nbsp;&nbsp;<b><a href='#").append(endpointGroupId).append("'>")
                    .append(endpointGroupId)
                    .append("</a></b><br />\n");
        }
        out.append("<br />\n");

        // :: Global DLQ
        if (stack.getGlobalDlq().isPresent()) {
            out.append("<div class='matsbm_endpoint_group'>\n");
            out.append("<h2>Global DLQ</h2><br />");
            MatsBrokerDestination globalDlq = stack.getGlobalDlq().get();
            out.append("<div class='matsbm_epid'>")
                    .append(globalDlq.getDestinationName())
                    .append("</div>");
            out.append("<div class='matsbm_stage'>")
                    .append(globalDlq.getFqDestinationName());

            out_queueCount(out, globalDlq);

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
            out.append("<div class='matsbm_endpoint_group' id='").append(endpointGroupId).append("'>\n");
            out.append("<a href='#").append(endpointGroupId).append("'>");
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
                        ? "<div class='matsbm_queue'>Queue</div>"
                        : "<div class='matsbm_topic'>Topic</div>";

                out.append("<div class='matsbm_epid'>").append(endpointId).append("</div>");
                out.append(" ").append(endpointType);

                // :: Foreach Stage
                for (MatsStageBrokerRepresentation stage : stages.values()) {
                    out.append("<div class='matsbm_stage'>");
                    out.append(stage.getStageIndex() == 0 ? "Initial" : "S" + stage.getStageIndex());
                    Optional<MatsBrokerDestination> incomingDest = stage.getIncomingDestination();
                    if (incomingDest.isPresent()) {
                        MatsBrokerDestination incoming = incomingDest.get();
                        out_queueCount(out, incoming);
                    }

                    Optional<MatsBrokerDestination> dlqDest = stage.getDlqDestination();
                    if (dlqDest.isPresent()) {
                        out_queueCount(out, dlqDest.get());
                    }
                    out.append("</div>"); // /matsbm_stage
                }
                out.append("<br />\n");
            }
            out.append("</div>\n");
        }
        out.append("</div>\n");
    }

    private static void out_queueCount(Appendable out, MatsBrokerDestination destination) throws IOException {
        String style = destination.isDlq()
                ? "dlq"
                : destination.getNumberOfQueuedMessages() == 0 ? "incoming_zero" : "incoming";
        if (destination.getDestinationType() == DestinationType.QUEUE) {
            out.append("<a class='").append(style).append("' href='?browse&destinationId=")
                    .append(destination.getDestinationType() == DestinationType.QUEUE ? "queue:" : "topic:")
                    .append(destination.getDestinationName())
                    .append("'>");
        }
        else {
            out.append("<div class='matsbm_topic'>");
        }
        out.append(destination.isDlq() ? "DLQ:" : "")
                .append(Long.toString(destination.getNumberOfQueuedMessages()));
        out.append(destination.getDestinationType() == DestinationType.QUEUE ? "</a>" : "</div>");

        long age = destination.getHeadMessageAgeMillis().orElse(0);
        if (age > 0) {
            out.append("<div class='matsbm_age'>(").append(Statics.millisSpanToHuman(age)).append(")</div>");
        }
    }
}
