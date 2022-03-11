package io.mats3.matsbrokermonitor.htmlgui;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.htmlgui.impl.MatsBrokerMonitorHtmlGuiImpl;
import io.mats3.serial.MatsSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * @author Endre Stølsvik 2022-01-02 12:19 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerMonitorHtmlGui {

    static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions,
            MatsSerializer<?> matsSerializer) {
        return new MatsBrokerMonitorHtmlGuiImpl(matsBrokerMonitor, matsBrokerBrowseAndActions, matsSerializer);
    }

    static MatsBrokerMonitorHtmlGuiImpl create(MatsBrokerMonitor matsBrokerMonitor,
            MatsBrokerBrowseAndActions matsBrokerBrowseAndActions) {
        return create(matsBrokerMonitor, matsBrokerBrowseAndActions, null);
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

    interface AccessControl {
        default boolean overview() {
            return false;
        }

        default boolean browse(String destinationId) {
            return false;
        }

        default boolean deleteMessages(String fromQueueId) {
            return false;
        }

        default boolean moveMessages(String sourceQueueId, String targetQueueId) {
            return false;
        }
    }

    class AllowAllAccessControl implements AccessControl {
        @Override
        public boolean overview() {
            return true;
        }

        @Override
        public boolean browse(String destinationId) {
            return true;
        }

        @Override
        public boolean deleteMessages(String fromQueueId) {
            return true;
        }

        @Override
        public boolean moveMessages(String sourceQueueId, String targetQueueId) {
            return true;
        }
    }

    class AccessDeniedException extends RuntimeException {
        public AccessDeniedException(String message) {
            super(message);
        }
    }
}
