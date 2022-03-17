package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.htmlgui.MatsBrokerMonitorHtmlGui;
import io.mats3.serial.MatsSerializer;

/**
 * Instantiate a <b>singleton</b> of this class, supplying it a {@link MatsBrokerMonitor} instance, <b>to which this
 * instance will register as listener</b>. Again: You are NOT supposed to instantiate an instance of this class per
 * rendering, as this instance is "active" and will register as listener and may instantiate threads.
 *
 * @author Endre Stølsvik 2021-12-17 10:22 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerMonitorHtmlGuiImpl implements MatsBrokerMonitorHtmlGui, Statics {
    private final Logger log = LoggerFactory.getLogger(MatsBrokerMonitorHtmlGuiImpl.class);

    private final MatsBrokerMonitor _matsBrokerMonitor;
    private final MatsBrokerBrowseAndActions _matsBrokerBrowseAndActions;
    private final MatsSerializer<?> _matsSerializer;

    /**
     * <b>DO NOT USE THIS CONSTRUCTOR</b>, use factories on {@link MatsBrokerMonitorHtmlGui}!
     */
    public MatsBrokerMonitorHtmlGuiImpl(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions,
            MatsSerializer<?> matsSerializer) {
        _matsBrokerMonitor = matsBrokerMonitor;
        _matsBrokerBrowseAndActions = matsBrokerBrowseAndActions;
        _matsSerializer = matsSerializer;
    }

    private String _jsonUrlPath = null;

    public void setJsonUrlPath(String jsonUrlPath) {
        _jsonUrlPath = jsonUrlPath;
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getStyleSheet(Appendable out) throws IOException {
        includeFile(out, "matsbrokermonitor.css");
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    public void getJavaScript(Appendable out) throws IOException {
        includeFile(out, "matsbrokermonitor.js");
    }

    private static void includeFile(Appendable out, String file) throws IOException {
        String filename = MatsBrokerMonitorHtmlGuiImpl.class.getPackage().getName().replace('.', '/') + '/' + file;
        InputStream is = MatsBrokerMonitorHtmlGuiImpl.class.getClassLoader().getResourceAsStream(filename);
        if (is == null) {
            throw new IllegalStateException("Missing '" + file + "' from ClassLoader.");
        }
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        while (true) {
            String line = br.readLine();
            if (line == null) {
                break;
            }
            out.append(line).append('\n');
        }
    }

    @Override
    public void gui(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        if (requestParameters.containsKey("browse")) {
            String destinationId = getBrowseDestinationId(requestParameters, ac);
            // ----- Passed Access Control for browse of specific destination, render it.

            // move programmatically configured json-path over to static javascript:
            out.append("<script>window.matsmb_json_path = ").append(_jsonUrlPath != null
                    ? "'" + _jsonUrlPath + "'"
                    : "null").append(";</script>");

            BrowseQueue.gui_BrowseQueue(_matsBrokerMonitor, _matsBrokerBrowseAndActions, out, destinationId, ac);
            return;
        }

        else if (requestParameters.containsKey("examineMessage")) {
            String destinationId = getBrowseDestinationId(requestParameters, ac);

            String[] messageSystemIds = requestParameters.get("messageSystemId");
            if (messageSystemIds == null) {
                throw new IllegalArgumentException("Missing messageSystemIds");
            }
            if (messageSystemIds.length > 1) {
                throw new IllegalArgumentException(">1 messageSystemId args");
            }
            String messageSystemId = messageSystemIds[0];

            // move programmatically configured json-path over to static javascript:
            out.append("<script>window.matsmb_json_path = ").append(_jsonUrlPath != null
                    ? "'" + _jsonUrlPath + "'"
                    : "null").append(";</script>");

            ExamineMessage.gui_ExamineMessage(_matsBrokerMonitor, _matsBrokerBrowseAndActions, _matsSerializer,
                    out, destinationId, messageSystemId);
            return;
        }

        // E-> No special argument, assume broker overview
        boolean overview = ac.overview();
        if (!overview) {
            throw new AccessDeniedException("Not allowed to see broker overview!");
        }
        // ----- Passed Access Control for overview, render it.
        BrokerOverview.gui_BrokerOverview(_matsBrokerMonitor, out, requestParameters, ac);
    }

    private String getBrowseDestinationId(Map<String, String[]> requestParameters, AccessControl ac) {
        String[] destinationIds = requestParameters.get("destinationId");
        if (destinationIds == null) {
            throw new IllegalArgumentException("Missing destinationId");
        }
        if (destinationIds.length > 1) {
            throw new IllegalArgumentException(">1 browse args");
        }
        String destinationId = destinationIds[0];
        if (!(destinationId.startsWith("queue:") || destinationId.startsWith("topic:"))) {
            throw new IllegalArgumentException("the browse arg should start with queue: or topic:");
        }
        boolean browseAllowed = ac.browse(destinationId);
        if (!browseAllowed) {
            throw new AccessDeniedException("Not allowed to browse destination!");
        }
        return destinationId;
    }

    @Override
    public void json(Appendable out, Map<String, String[]> requestParameters, String requestBody,
            AccessControl ac) throws IOException, AccessDeniedException {
        log.info("RequestBody: " + requestBody);
        CommandDto command = Statics.createMapper().readValue(requestBody, CommandDto.class);

        if (command.action == null) {
            throw new IllegalArgumentException("command.action is null");
        }

        // --- if delete or reissue

        if ("delete".equals(command.action) || "reissue".equals(command.action)) {
            if (command.queueId == null) {
                throw new IllegalArgumentException("command.queueId is null");
            }

            if (command.msgSysMsgIds == null) {
                throw new IllegalArgumentException("command.msgSysMsgIds is null");
            }

            log.info("action:[" + command.action + "], queueId: [" + command.queueId
                    + "], msgSysMsgIds:[" + command.msgSysMsgIds + "]");

            if ("delete".equals(command.action)) {
                _matsBrokerBrowseAndActions.deleteMessages(command.queueId, command.msgSysMsgIds);
            }
            else if ("reissue".equals(command.action)) {
                _matsBrokerBrowseAndActions.reissueMessages(command.queueId, command.msgSysMsgIds);
            }

            out.append("{}");
        }
    }

    private static class CommandDto {
        String action;
        String queueId;
        List<String> msgSysMsgIds;
    }

    @Override
    public void html(Appendable out, Map<String, String[]> requestParameters,
            AccessControl ac) throws IOException, AccessDeniedException {

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
