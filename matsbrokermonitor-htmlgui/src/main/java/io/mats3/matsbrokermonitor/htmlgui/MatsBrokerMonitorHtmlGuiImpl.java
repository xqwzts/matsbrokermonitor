package io.mats3.matsbrokermonitor.htmlgui;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;

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
                + "  font-size: 95%;\n"
                + "  line-height: 1.35;\n"
                + "  color: #212529;\n"
                + "}\n");

        out.append(".mats_report hr {\n"
                + "  border: 1px dashed #aaa;\n"
                + "  margin: 0.2em 0 0.8em 0;\n"
                + "  color: inherit;\n"
                + "  background-color: currentColor;\n"
                + "  opacity: 0.25;\n"
                + "}\n");

        // :: Fonts and headings
        out.append(".mats_report h2, .mats_report h3, .mats_report h4 {\n"
                // Have to re-set font here, otherwise Bootstrap 3 takes over.
                + "  font-family: " + font_regular + ";\n"
                + "  display: inline;\n"
                + "  line-height: 1.2;\n"
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
        out.append(".mats_report code {\n"
                + "  font-family: " + font_mono + ";\n"
                + "  font-size: .875em;\n"
                + "  color: #d63384;\n"
                + "  background-color: rgba(0, 0, 0, 0.07);\n"
                + "  padding: 2px 4px 1px 4px;\n"
                + "  border-radius: 3px;\n"
                + "}\n");

        // .. InitiatorIds and EndpointIds
        out.append(".mats_epid {\n"
                + "  font-family: " + font_mono + ";\n"
                + "  font-size: .875em;\n"
                + "  color: #d63384;\n"
                + "  background-color: rgba(0, 0, 255, 0.07);\n"
                + "  padding: 2px 4px 1px 4px;\n"
                + "  border-radius: 3px;\n"
                + "}\n");
        out.append(".mats_appname {\n"
                // + " color: #d63384;\n"
                + "  background-color: rgba(0, 255, 255, 0.07);\n"
                + "  padding: 2px 4px 1px 4px;\n"
                + "  border-radius: 3px;\n"
                + "}\n");

        // .. integers in timings (i.e. ms >= 500)
        // NOTE! The point of this class is NOT denote "high timings", but to denote that there are no
        // decimals, to visually make it easier to compare a number '1 235' with '1.235'.
        out.append(".mats_integer {\n"
                + "  color: #b02a37;\n"
                + "}\n");

        // :: The different parts of the report
        out.append(".mats_info {\n"
                + "  margin: 0em 0em 0em 0.5em;\n"
                + "}\n");

        out.append(".mats_factory {\n"
                + "  background: #f0f0f0;\n"
                + "}\n");
        out.append(".mats_initiator {\n"
                + "  background: #e0f0e0;\n"
                + "}\n");
        out.append(".mats_endpoint {\n"
                + "  background: #e0e0f0;\n"
                + "}\n");
        out.append(".mats_stage {\n"
                + "  background: #f0f0f0;\n"
                + "}\n");
        // Boxes:
        out.append(".mats_factory, .mats_initiator, .mats_endpoint, .mats_stage {\n"
                + "  border-radius: 3px;\n"
                + "  box-shadow: 2px 2px 2px 0px rgba(0,0,0,0.37);\n"
                + "  border: thin solid #a0a0a0;\n"
                + "  margin: 0.5em 0.5em 0.7em 0.5em;\n"
                + "  padding: 0.1em 0.5em 0.5em 0.5em;\n"
                + "}\n");

        out.append(".mats_initiator, .mats_endpoint {\n"
                + "  margin: 0.5em 0.5em 2em 0.5em;\n"
                + "}\n");
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getJavaScript(Appendable out) throws IOException {
        out.append("");
    }

    public void createOverview(Appendable out, Map<String, String[]> requestParameters) throws IOException {
        out.append("<div class=\"mats_report mats_factory\">\n");
        out.append("  <div class=\"mats_heading\">MatsFactory <h2>Test</h2>\n");
        out.append("   - <b>Known number of CPUs: Elg</b>");
        out.append("   - <b>Concurrency:</b> Belg");
        out.append("   - <b>Running:</b> Selg");
        out.append("  </div>\n");
        Map<String, MatsBrokerDestination> matsDestinations = _matsBrokerMonitor.getMatsDestinations();
        for (Entry<String, MatsBrokerDestination> entry : matsDestinations.entrySet()) {
            out.append("FQDI: [" + entry.getKey() + "]: " + entry.getValue() + "<br />\n");
        }
        out.append("  <hr />\n");
        out.append("</div>\n");
    }
}
