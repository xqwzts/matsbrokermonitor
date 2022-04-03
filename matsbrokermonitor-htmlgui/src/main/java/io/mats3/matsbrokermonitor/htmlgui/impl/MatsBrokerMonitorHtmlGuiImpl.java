package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.UpdateEvent;
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
    private final List<? super MonitorAddition> _monitorAdditions;
    private final MatsSerializer<?> _matsSerializer;

    /**
     * <b>DO NOT USE THIS CONSTRUCTOR</b>, use factories on {@link MatsBrokerMonitorHtmlGui}!
     */
    public MatsBrokerMonitorHtmlGuiImpl(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions,
            List<? super MonitorAddition> monitorAdditions,
            MatsSerializer<?> matsSerializer) {
        _matsBrokerMonitor = matsBrokerMonitor;
        _matsBrokerBrowseAndActions = matsBrokerBrowseAndActions;
        _monitorAdditions = monitorAdditions == null ? Collections.emptyList() : monitorAdditions;
        _matsSerializer = matsSerializer;

        _matsBrokerMonitor.registerListener(new UpdateEventListener());
    }

    private final ConcurrentHashMap<String, CountDownLatch> _updateEventWaiters = new ConcurrentHashMap<>();

    private class UpdateEventListener implements Consumer<UpdateEvent> {
        @Override
        public void accept(UpdateEvent updateEvent) {
            if (updateEvent.getCorrelationId().isPresent()) {
                CountDownLatch waitingLatch = _updateEventWaiters.get(updateEvent.getCorrelationId().get());
                if (waitingLatch != null) {
                    log.info("Got update event, found waiter for: " + updateEvent);
                    waitingLatch.countDown();
                }
            }
        }
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
    public void gui(Appendable appendable, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException {
        long nanosAsStart_fullRender = System.nanoTime();

        Outputter out = new Outputter(appendable);
        if (requestParameters.containsKey("browse")) {
            // -> Browse Queue
            String queueId = getBrowseQueueId(requestParameters, ac);
            // ----- Passed BROWSE Access Control for specific queueId.

            // move programmatically configured json-path over to static javascript:
            out.html("<script>window.matsbm_json_path = ").DATA(_jsonUrlPath != null
                    ? "'" + _jsonUrlPath + "'"
                    : "null").html(";</script>\n");

            BrowseQueue.gui_BrowseQueue(_matsBrokerMonitor, _matsBrokerBrowseAndActions, _monitorAdditions, out,
                    queueId, ac);
        }
        else if (requestParameters.containsKey("examineMessage")) {
            // -> Examine Message
            String queueId = getBrowseQueueId(requestParameters, ac);
            // ----- Passed BROWSE Access Control for specific queueId.

            // ACCESS CONTROL: examine message
            if (!ac.examineMessage(queueId)) {
                throw new AccessDeniedException("Not allowed to examine message!");
            }

            // ----- Passed EXAMINE MESSAGE Access Control for specific queueId.

            String[] messageSystemIds = requestParameters.get("messageSystemId");
            if (messageSystemIds == null) {
                throw new IllegalArgumentException("Missing messageSystemIds");
            }
            if (messageSystemIds.length > 1) {
                throw new IllegalArgumentException(">1 messageSystemId args");
            }
            String messageSystemId = messageSystemIds[0];

            // move programmatically configured json-path over to static javascript:
            out.html("<script>window.matsbm_json_path = ").DATA(_jsonUrlPath != null
                    ? "'" + _jsonUrlPath + "'"
                    : "null").html(";</script>\n");

            ExamineMessage.gui_ExamineMessage(_matsBrokerMonitor, _matsBrokerBrowseAndActions, _matsSerializer,
                    _monitorAdditions, out, queueId, messageSystemId);
        }
        else {
            // E-> No view argument: Broker Overview

            // ACCESS CONTROL: examine message
            if (!ac.overview()) {
                throw new AccessDeniedException("Not allowed to see broker overview!");
            }
            // ----- Passed Access Control for overview, render it.

            BrokerOverview.gui_BrokerOverview(_matsBrokerMonitor, out, requestParameters, ac);
        }

        long nanosTaken_fullRender = System.nanoTime() - nanosAsStart_fullRender;
        out.html("Render time: ").DATA(Math.round(nanosTaken_fullRender / 1000d) / 1000d).html(" ms.");
        out.html("</div>");
    }

    private String getBrowseQueueId(Map<String, String[]> requestParameters, AccessControl ac) {
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

        boolean queue = destinationId.startsWith("queue:");
        if (!queue) {
            throw new IllegalArgumentException("Cannot browse anything other than queues!");
        }
        String queueId = destinationId.substring("queue:".length());

        // :: ACCESS CONTROL
        boolean browseAllowed = ac.browseQueue(queueId);
        if (!browseAllowed) {
            throw new AccessDeniedException("Not allowed to browse queue!");
        }
        return queueId;
    }

    private static final ObjectMapper MAPPER = Statics.createMapper();

    @Override
    public void json(Appendable out, Map<String, String[]> requestParameters, String requestBody,
            AccessControl ac) throws IOException, AccessDeniedException {
        log.info("JSON RequestBody: " + requestBody);
        CommandDto command = MAPPER.readValue(requestBody, CommandDto.class);

        if (command.action == null) {
            throw new IllegalArgumentException("'command.action' is null.");
        }

        if ("delete".equals(command.action) || "reissue".equals(command.action)) {
            if (command.queueId == null) {
                throw new IllegalArgumentException("command.queueId is null.");
            }

            if (command.msgSysMsgIds == null) {
                throw new IllegalArgumentException("command.msgSysMsgIds is null.");
            }

            log.info("action:[" + command.action + "], queueId: [" + command.queueId
                    + "], msgSysMsgIds:[" + command.msgSysMsgIds + "]");

            List<String> affectedMsgSysMsgIds;
            Map<String, String> reissuedMsgSysMsgIds = null;
            if ("delete".equals(command.action)) {
                // ACCESS CONTROL: delete message
                if (!ac.deleteMessage(command.queueId)) {
                    throw new AccessDeniedException("Not allowed to delete messages from queue.");
                }
                // ----- Passed Access Control for deleteMessage; Perform operation

                affectedMsgSysMsgIds = _matsBrokerBrowseAndActions.deleteMessages(command.queueId,
                        command.msgSysMsgIds);
            }
            else if ("reissue".equals(command.action)) {
                // ACCESS CONTROL: reissue message
                if (!ac.reissueMessage(command.queueId)) {
                    throw new AccessDeniedException("Not allowed to reissue messages from DLQ.");
                }
                // ----- Passed Access Control for reissueMessage; Perform operation

                reissuedMsgSysMsgIds = _matsBrokerBrowseAndActions.reissueMessages(command.queueId,
                        command.msgSysMsgIds);
                affectedMsgSysMsgIds = new ArrayList<>(reissuedMsgSysMsgIds.keySet());
            }
            else {
                throw new IllegalArgumentException("Unknown command.action.");
            }

            ResultDto result = new ResultDto();
            result.result = "ok";
            result.numberOfAffectedMessages = affectedMsgSysMsgIds.size();
            result.msgSysMsgIds = affectedMsgSysMsgIds;
            result.reissuedMsgSysMsgIds = reissuedMsgSysMsgIds;
            out.append(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        }
        else if ("update".equals(command.action)) {
            // ACCESS CONTROL: reissue message
            if (!ac.overview()) {
                throw new AccessDeniedException("Not allowed to see overview, thus not request update either.");
            }
            // ----- Passed Access Control for overview; Request update

            boolean updated;
            String correlationId = Long.toString(ThreadLocalRandom.current().nextLong(), 36);
            try {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                _updateEventWaiters.put(correlationId, countDownLatch);
                log.info("Update: executing matsBrokerMonitor.forceUpdate(\"" + correlationId + "\", false);");
                _matsBrokerMonitor.forceUpdate(correlationId, false);
                long nanosAtStart_wait = System.nanoTime();
                updated = countDownLatch.await(2500, TimeUnit.MILLISECONDS);
                long nanosTaken_wait = System.nanoTime() - nanosAtStart_wait;
                log.info("Update: updated: [" + updated + "] (waited [" + Math.round((nanosTaken_wait / 1000d) / 1000d)
                        + "] ms).");
            }
            catch (InterruptedException e) {
                log.info("Update: Got interrupted while waiting for matsBrokerMonitor.forceRefresh(" + correlationId
                        + ", false) - assuming shutdown, trying to reply 'no can do'.");
                updated = false;
            }
            finally {
                _updateEventWaiters.remove(correlationId);
            }
            ResultDto result = new ResultDto();
            result.result = updated ? "ok" : "not_ok";
            out.append(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        }
        else {
            throw new IllegalArgumentException("Don't understand that 'command.action'.");
        }
    }

    private static class CommandDto {
        String action;
        String queueId;
        List<String> msgSysMsgIds;
    }

    private static class ResultDto {
        String result;
        int numberOfAffectedMessages;
        List<String> msgSysMsgIds;
        Map<String, String> reissuedMsgSysMsgIds;
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
