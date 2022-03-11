package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MatsBrokerMonitorHtmlGuiImpl implements MatsBrokerMonitorHtmlGui {
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
            ExamineMessage.gui_ExamineMessage(_matsBrokerMonitor, _matsBrokerBrowseAndActions, _matsSerializer,
                    _jsonUrlPath, out, destinationId, messageSystemId);
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

    private static final Pattern __actionPattern = Pattern.compile("\"action\"\\s*:\\s*\"(.*?)\"");
    private static final Pattern __queueIdPattern = Pattern.compile("\"queueId\"\\s*:\\s*\"(.*?)\"");
    private static final Pattern __msgSysMsgIdPattern = Pattern.compile("\"msgSysMsgId\"\\s*:\\s*\"(.*?)\"");

    @Override
    public void json(Appendable out, Map<String, String[]> requestParameters, String requestBody,
            AccessControl ac) throws IOException, AccessDeniedException {
        log.info("RequestBody: " + requestBody);
        Matcher action_Matcher = __actionPattern.matcher(requestBody);
        if (!action_Matcher.find()) {
            throw new IllegalArgumentException("Missing 'action'!");
        }
        String action = action_Matcher.group(1);

        // --- if delete or reissue

        if ("delete".equals(action) || "reissue".equals(action)) {

            Matcher queueId_Matcher = __queueIdPattern.matcher(requestBody);
            if (!queueId_Matcher.find()) {
                throw new IllegalArgumentException("Missing 'queueId'!");
            }
            String queueId = queueId_Matcher.group(1);

            Matcher msgSysMsgId_Matcher = __msgSysMsgIdPattern.matcher(requestBody);
            if (!msgSysMsgId_Matcher.find()) {
                throw new IllegalArgumentException("Missing 'msgSysMsgId'!");
            }
            String msgSysMsgId = msgSysMsgId_Matcher.group(1);

            log.info("action:[" + action + "], queueId: [" + queueId + "], msgSysMsgId:[" + msgSysMsgId + "]");

            if ("delete".equals(action)) {
                _matsBrokerBrowseAndActions.deleteMessages(queueId, Collections.singleton(msgSysMsgId));
            }
            else if ("reissue".equals(action)) {
                _matsBrokerBrowseAndActions.reissueMessages(queueId, Collections.singleton(msgSysMsgId));
            }

            out.append("{}");
        }
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
