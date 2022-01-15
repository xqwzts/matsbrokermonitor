package io.mats3.matsbrokermonitor.htmlgui;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfo;
import io.mats3.matsbrokermonitor.api.DestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointGroupBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsStageBrokerRepresentation;

/**
 * Instantiate a <b>singleton</b> of this class, supplying it a {@link MatsBrokerMonitor} instance, <b>to which this
 * instance will register as listener</b>. Again: You are NOT supposed to instantiate an instance of this class per
 * rendering, as this instance is "active" and will register as listener and may instantiate threads.
 *
 * @author Endre Stølsvik 2021-12-17 10:22 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerMonitorHtmlGuiImpl implements MatsBrokerMonitorHtmlGui {
    public static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor) {
        return new MatsBrokerMonitorHtmlGuiImpl(matsBrokerMonitor);
    }

    private MatsBrokerMonitor _matsBrokerMonitor;

    MatsBrokerMonitorHtmlGuiImpl(MatsBrokerMonitor matsBrokerMonitor) {
        _matsBrokerMonitor = matsBrokerMonitor;
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getStyleSheet(Appendable out) throws IOException {
        // Regular fonts: Using the "Native font stack" of Bootstrap 5
        String font_regular = ""
                // Cross-platform generic font family (default user interface font)
                // + "system-ui,"
                // Safari for macOS and iOS (San Francisco)
                + " -apple-system,"
                // Chrome < 56 for macOS (San Francisco)
                + " BlinkMacSystemFont,"
                // Windows
                + " \"Segoe UI\","
                // Android
                + " Roboto,"
                // Basic web fallback
                + " \"Helvetica Neue\", Arial,"
                // Linux
                + " \"Noto Sans\","
                + " \"Liberation Sans\","
                // Sans serif fallback
                + " sans-serif,"
                // Emoji fonts
                + " \"Apple Color Emoji\", \"Segoe UI Emoji\", \"Segoe UI Symbol\", \"Noto Color Emoji\"";

        // Monospaced fonts: Using the "Native font stack" of Bootstrap 5
        String font_mono = "SFMono-Regular,Menlo,Monaco,Consolas,\"Liberation Mono\",\"Courier New\",monospace;";

        out.append(".mats_report {\n"
                + "  font-family: " + font_regular + ";\n"
                + "  font-weight: 400;\n"
                + "  font-size: 80%;\n"
                + "  line-height: 1.35;\n"
                + "  color: #212529;\n"
                + "}\n");

        // :: Fonts and headings
        out.append(".mats_report h1, .mats_report h2, .mats_report h3, .mats_report h4 {\n"
                // Have to re-set font here, otherwise Bootstrap 3 takes over.
                + "  font-family: " + font_regular + ";\n"
                + "  display: inline-block;\n"
                + "  line-height: 1.2;\n"
                + "  margin: 0.15em 0 0.3em 0;\n"
                + "}\n");
        out.append(".mats_report h1 {\n"
                + "  font-size: 1.8em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_report h2 {\n"
                + "  font-size: 1.5em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_report h3 {\n"
                + "  font-size: 1.4em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_report h4 {\n"
                + "  font-size: 1.3em;\n"
                + "  font-weight: 400;\n"
                + "}\n");
        out.append(".mats_heading {\n"
                + "  display: block;\n"
                + "  margin: 0em 0em 0.5em 0em;\n"
                + "}\n");

        // .. integers in timings (i.e. ms >= 500)
        // NOTE! The point of this class is NOT denote "high timings", but to denote that there are no
        // decimals, to visually make it easier to compare a number '1 235' with '1.235'.
        out.append(".mats_integer {\n"
                + "  color: #b02a37;\n"
                + "}\n");

        // :: TEXT: EndpointId
        out.append(".mats_endpoint_id {\n"
                + "}\n");

        // :: TEXT: Age
        out.append(".mats_age {\n"
                + "  display: inline;\n"
                + "  font-size: 75%;\n"
                + "  font-style: italic;\n"
                + "  margin: 0 0 0 0.2em;\n"
                + "}\n");

        // TEXT: Ids
        out.append(".mats_epid, .mats_queue, .mats_topic {\n"
                + "  display: inline-block;\n"
                + "  font-family: " + font_mono + ";\n"
                + "  font-size: 1em;\n"
                + "  margin: 0 0 0 0.5em;\n"
                + "  padding: 2px 4px 1px 4px;\n"
                + "  border-radius: 3px;\n"
                + "}\n");

        // TEXT: EndpointIds
        out.append(".mats_epid {\n"
                + "  margin: 0 0 0 1.5em;\n"
                + "  font-size: 1.1em;\n"
                + "  background-color: rgba(0, 0, 255, 0.04);\n"
                + "}\n");

        // TEXT: Queue
        out.append(".mats_queue {\n"
                + "  font-size: 0.9em;\n"
                + "  font-style: italic;\n"
                + "  background-color: rgba(0, 255, 0, 0.15);\n"
                + "}\n");
        // TEXT: Topic
        out.append(".mats_topic {\n"
                + "  font-size: 0.9em;\n"
                + "  font-style: italic;\n"
                + "  background-color: rgba(0, 255, 255, 0.2);\n"
                + "}\n");

        // :: Destinations-button-links: Incoming and DLQ
        out.append(".mats_report .incoming_zero,.incoming,.dlq {\n"
                + "  text-decoration: none;\n"
                + "  background-color: initial;\n"
                + "  background-image: linear-gradient(#00D775, #00BD68);\n"
                + "  box-shadow: rgba(0, 0, 0, .3) 0 5px 15px;\n"
                + "  border-radius: 6px;\n"
                + "  box-shadow: rgba(0, 0, 0, 0.1) 0 2px 4px;\n"
                + "  color: #FFFFFF;\n"
                + "  display: inline-block;\n"
                + "  margin: 0 0 0 0.4em;\n" // spacing between "S1" and this box
                + "  padding: 0px 10px;\n" // Extra width to sides of queue count
                + "  text-align: center;\n"
                + "  border: 0;\n"
                + "  transition: box-shadow 0.4s;\n"
                + "}\n"
                + "\n"
                + ".mats_report .incoming_zero:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(13, 112, 234, 0.9) 0 3px 8px;\n"
                + "}");

        out.append(".mats_report .incoming_zero {\n"
                + "  background-image: linear-gradient(#00D775, #00BD68);\n"
                + "}\n"
                + "\n"
                + ".mats_report .incoming_zero:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(13, 112, 234, 0.9) 0 3px 8px;\n"
                + "}");

        out.append(".mats_report .incoming {\n"
                + "  background-image: linear-gradient(#0dccea, #0d70ea);\n"
                + "}\n"
                + "\n"
                + ".mats_report .incoming:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(13, 112, 234, 0.9) 0 3px 8px;\n"
                + "}");

        out.append(".mats_report .dlq {\n"
                + "  background-image: linear-gradient(#FF7E31, #E62C03);\n"
                + "}\n"
                + "\n"
                + ".mats_report .dlq:hover {\n"
                + "  text-decoration: underline;\n"
                + "  box-shadow: rgba(253, 76, 0, 0.9) 0 3px 8px;\n"
                + "}");

        // :: The different parts of the report
        out.append(".mats_info {\n"
                + "  margin: 0em 0em 0em 0.5em;\n"
                + "}\n");

        // Boxes:
        out.append(".mats_broker, .mats_endpoint_group {\n"
                + "  border-radius: 3px;\n"
                + "  box-shadow: 2px 2px 2px 0px rgba(0,0,0,0.37);\n"
                + "  border: thin solid #a0a0a0;\n"
                + "  margin: 0.5em 0.5em 0.7em 0.5em;\n"
                + "  padding: 0.1em 0.5em 0.5em 0.5em;\n"
                + "}\n");
        out.append(".mats_broker {\n"
                + "  background: #f0f0f0;\n"
                + "}\n");
        out.append(".mats_endpoint_group {\n"
                + "  background: #e8f0e8;\n"
                + "  margin: 0.5em 0.5em 1em 0.5em;\n"
                + "}\n");

        // :: Stage small-box
        out.append(".mats_stage {\n"
                + "  background: #f0f0f0;\n"
                + "  display: inline-block;\n"
                + "  border-radius: 3px;\n"
                + "  box-shadow: 2px 2px 2px 0px rgba(0,0,0,0.37);\n"
                + "  border: thin solid #a0a0a0;\n"
                + "  margin: 0.2em 0 0.2em 0.5em;\n"
                + "  padding: 0.2em 0.2em 0.2em 0.2em;\n"
                + "}\n");
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getJavaScript(Appendable out) throws IOException {
        out.append("");
    }

    public void createOverview(Appendable out, Map<String, String[]> requestParameters) throws IOException {
        out.append("<div class=\"mats_report mats_broker\">\n");
        out.append("  <div class=\"mats_heading\">");
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

        // :: Global DLQ
        if (stack.getGlobalDlq().isPresent()) {
            out.append("<div class=\"mats_endpoint_group\">\n");
            out.append("<h2>Global DLQ</h2><br />");
            MatsBrokerDestination globalDlq = stack.getGlobalDlq().get();
            out.append("<div class=\"mats_epid\">")
                    .append(globalDlq.getDestinationName())
                    .append("</div>");
            out.append("<div class=\"mats_stage\">")
                    .append(globalDlq.getFqDestinationName());

            queueCount(out, globalDlq);

            out.append("</div>");
            out.append("</div>");
        }

        // :: Foreach EndpointGroup
        for (MatsEndpointGroupBrokerRepresentation service : stack.getMatsEndpointGroupBrokerRepresentations()
                .values()) {
            // :: EndpointGroup
            out.append("<div class=\"mats_endpoint_group\">\n");
            out.append("EndpointGroup <h2>")
                    .append(service.getEndpointGroup().trim().isEmpty()
                            ? "{empty string}"
                            : service.getEndpointGroup())
                    .append("</h2><br />\n");

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
                        ? "<div class=\"mats_queue\">Queue</div>"
                        : "<div class=\"mats_topic\">Topic</div>";

                out.append("<div class=\"mats_epid\">").append(endpointId).append("</div>");
                out.append(" ").append(endpointType);

                // :: Foreach Stage
                for (MatsStageBrokerRepresentation stage : stages.values()) {
                    out.append("<div class=\"mats_stage\">");
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
                    out.append("</div>"); // /mats_stage
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
        out.append("<a class=\"").append(style).append("\" href=\"?")
                .append(destination.getDestinationType() == DestinationType.QUEUE ? "queue=" : "topic=")
                .append(destination.getDestinationName())
                .append("\">")
                .append(destination.isDlq() ? "DLQ:" : "")
                .append(Long.toString(destination.getNumberOfQueuedMessages()))
                .append("</a>");
        long age = destination.getHeadMessageAgeMillis().orElse(0);
        if (age > 0) {
            out.append("<div class=\"mats_age\">(").append(millisToHuman(age)).append(")</div>");
        }
    }

    private String millisToHuman(long millis) {
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
