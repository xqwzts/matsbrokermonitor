package io.mats3.matsbrokermonitor.htmlgui;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions.MatsBrokerMessageRepresentation;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.htmlgui.impl.MatsBrokerMonitorHtmlGuiImpl;
import io.mats3.serial.MatsSerializer;

/**
 * @author Endre Stølsvik 2022-01-02 12:19 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerMonitorHtmlGui {

    static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions,
            List<? super MonitorAddition> monitorAdditions,
            MatsSerializer<?> matsSerializer) {
        return new MatsBrokerMonitorHtmlGuiImpl(matsBrokerMonitor, matsBrokerBrowseAndActions, monitorAdditions,
                matsSerializer);
    }

    static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions) {
        return create(matsBrokerMonitor, matsBrokerBrowseAndActions, null, null);
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    void getStyleSheet(Appendable out) throws IOException;

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    void getJavaScript(Appendable out) throws IOException;

    /**
     * The "main", embeddable HTML GUI. This might call to {@link #json(Appendable, Map, String, AccessControl)} and
     * {@link #html(Appendable, Map, AccessControl)}.
     */
    void gui(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException, AccessDeniedException;

    void json(Appendable out, Map<String, String[]> requestParameters, String requestBody, AccessControl ac)
            throws IOException, AccessDeniedException;

    void html(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException, AccessDeniedException;

    interface MonitorAddition {
        String convertMessageToHtml(MatsBrokerMessageRepresentation message);
    }

    interface BrowseQueueTableAddition extends MonitorAddition {
        /**
         * @return the output wanted for this table column's heading, <b>which must include the <code>&lt;th&gt;</code>
         *         and <code>&lt;/th&gt;</code> HTML</b>. If null is returned, the entire column is elided.
         */
        String getColumnHeadingHtml(String queueId);

        /**
         * @param message
         *            the {@link MatsBrokerMessageRepresentation} being printed
         * @return the output wanted for this table cell, raw HTML, <b>which must include the
         *         "<code>&lt;td&gt;&lt;div&gt;</code><i>contents here</i><code>&lt;/div&gt;&lt;/td&gt;</code>" for the
         *         table cell</b> - remember both the td and the inner div! If you return <code>null</code>, an empty
         *         cell will be output.
         */
        String convertMessageToHtml(MatsBrokerMessageRepresentation message);
    }

    interface ExamineMessageAddition extends MonitorAddition {
        /**
         * @param message
         *            the {@link MatsBrokerMessageRepresentation} being printed
         * @return the output wanted for this message, raw HTML.
         */
        String convertMessageToHtml(MatsBrokerMessageRepresentation message);
    }

    interface AccessControl {
        default boolean overview() {
            return false;
        }

        default boolean browseQueue(String queueId) {
            return false;
        }

        default boolean examineMessage(String fromQueueId) {
            return false;
        }

        default boolean deleteMessage(String fromQueueId) {
            return false;
        }

        default boolean reissueMessage(String fromDeadLetterQueueId) {
            return false;
        }
    }

    class AllowAllAccessControl implements AccessControl {
        @Override
        public boolean overview() {
            return true;
        }

        @Override
        public boolean browseQueue(String queueId) {
            return true;
        }

        @Override
        public boolean examineMessage(String fromQueueId) {
            return true;
        }

        @Override
        public boolean deleteMessage(String fromQueueId) {
            return true;
        }

        @Override
        public boolean reissueMessage(String fromDeadLetterQueueId) {
            return true;
        }
    }

    class AccessDeniedException extends RuntimeException {
        public AccessDeniedException(String message) {
            super(message);
        }
    }
}
