package io.mats3.matsbrokermonitor.htmlgui;

import io.mats3.matsbrokermonitor.spi.MatsBrokerInterface;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * @author Endre Stølsvik 2021-12-17 10:22 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerHtmlGui {
    public static MatsBrokerHtmlGui create(MatsBrokerInterface matsBrokerInterface) {
        return new MatsBrokerHtmlGui(matsBrokerInterface);
    }

    private MatsBrokerInterface _matsBrokerInterface;

    MatsBrokerHtmlGui(MatsBrokerInterface matsBrokerInterface) {
        _matsBrokerInterface = matsBrokerInterface;
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
        // .. min and max (timings)
        out.append(".mats_min {\n"
                + "  top: +0.15em;\n"
                + "  position: relative;\n"
                + "  font-size: 0.75em;\n"
                + "}\n");
        out.append(".mats_max {\n"
                + "  top: -0.45em;\n"
                + "  position: relative;\n"
                + "  font-size: 0.75em;\n"
                + "}\n");

        // .. InitiatorIds and EndpointIds
        out.append(".mats_iid {\n"
                + "  font-family: " + font_mono + ";\n"
                + "  font-size: .875em;\n"
                + "  color: #d63384;\n"
                + "  background-color: rgba(0, 255, 0, 0.07);\n"
                + "  padding: 2px 4px 1px 4px;\n"
                + "  border-radius: 3px;\n"
                + "}\n");
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

        out.append(".mats_hot {\n"
                + "  box-shadow: #FFF 0 -1px 4px, #ff0 0 -2px 10px, #ff8000 0 -10px 20px, red 0 -18px 40px, 5px 5px 15px 5px rgba(0,0,0,0);\n"
                + "  border: 0.2em solid red;\n"
                + "  background: #ECEFCF;\n"
                + "}\n");
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getJavaScript(Appendable out) throws IOException {
        out.append("");
    }

    public void createFactoryReport(Appendable out) throws IOException {
    }

    static final DecimalFormatSymbols NF_SYMBOLS;
    static final DecimalFormat NF_INTEGER;

    static final DecimalFormat NF_0_DECIMALS;
    static final DecimalFormat NF_1_DECIMALS;
    static final DecimalFormat NF_2_DECIMALS;
    static final DecimalFormat NF_3_DECIMALS;

    static {
        NF_SYMBOLS = new DecimalFormatSymbols(Locale.US);
        NF_SYMBOLS.setDecimalSeparator('.');
        NF_SYMBOLS.setGroupingSeparator('\u202f');

        NF_INTEGER = new DecimalFormat("#,##0");
        NF_INTEGER.setMaximumFractionDigits(0);
        NF_INTEGER.setDecimalFormatSymbols(NF_SYMBOLS);

        // NOTE! The point of this class is NOT denote "high timings", but to denote that there are no
        // decimals, to visually make it easier to compare a number '1 235' with '1.235'.
        NF_0_DECIMALS = new DecimalFormat("<span class=\"mats_integer\">#,##0</span>");
        NF_0_DECIMALS.setMaximumFractionDigits(0);
        NF_0_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_1_DECIMALS = new DecimalFormat("#,##0.0");
        NF_1_DECIMALS.setMaximumFractionDigits(1);
        NF_1_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_2_DECIMALS = new DecimalFormat("#,##0.00");
        NF_2_DECIMALS.setMaximumFractionDigits(2);
        NF_2_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_3_DECIMALS = new DecimalFormat("#,##0.000");
        NF_3_DECIMALS.setMaximumFractionDigits(3);
        NF_3_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);
    }

    String formatInt(long number) {
        return NF_INTEGER.format(number);
    }

    String formatNanos(double nanos) {
        if (Double.isNaN(nanos)) {
            return "NaN";
        }
        if (nanos == 0d) {
            return "0";
        }
        // >=500 ms?
        if (nanos >= 1_000_000L * 500) {
            // -> Yes, >500ms, so chop off fraction entirely, e.g. 612
            return NF_0_DECIMALS.format(Math.round(nanos / 1_000_000d));
        }
        // >=50 ms?
        if (nanos >= 1_000_000L * 50) {
            // -> Yes, >50ms, so use 1 decimal, e.g. 61.2
            return NF_1_DECIMALS.format(Math.round(nanos / 100_000d) / 10d);
        }
        // >=5 ms?
        if (nanos >= 1_000_000L * 5) {
            // -> Yes, >5ms, so use 2 decimal, e.g. 6.12
            return NF_2_DECIMALS.format(Math.round(nanos / 10_000d) / 100d);
        }
        // Negative? (Can happen when we to 'avg - 2 x std.dev', the result becomes negative)
        if (nanos < 0) {
            // -> Negative, so use three digits
            return NF_3_DECIMALS.format(Math.round(nanos / 1_000d) / 1_000d);
        }
        // E-> <5 ms
        // Use 3 decimals, e.g. 0.612
        double round = Math.round(nanos / 1_000d) / 1_000d;
        // ?: However, did we round to zero?
        if (round == 0) {
            // -> Yes, round to zero, so show special case
            return "~>0";
        }
        return NF_3_DECIMALS.format(round);
    }

}
