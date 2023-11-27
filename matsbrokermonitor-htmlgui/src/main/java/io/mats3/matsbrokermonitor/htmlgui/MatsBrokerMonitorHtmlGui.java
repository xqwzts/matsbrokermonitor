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
     * Note: The output from this method is static, it can be written directly to the HTML page in a script-tag, or
     * included as a separate file (with hard caching).
     */
    void outputStyleSheet(Appendable out) throws IOException;

    /**
     * Note: The output from this method is static, it can be written directly to the HTML page in a style-tag, or
     * included as a separate file (with hard caching).
     */
    void outputJavaScript(Appendable out) throws IOException;

    /**
     * The embeddable HTML GUI - map this to GET, content type is <code>"text/html; charset=utf-8"</code>. This might
     * call to {@link #json(Appendable, Map, String, AccessControl)}.
     */
    void html(Appendable out, Map<String, String[]> requestParameters, AccessControl ac)
            throws IOException, AccessDeniedException;

    /**
     * The HTML GUI will invoke JSON-over-HTTP to the same URL it is located at - map this to PUT and DELETE, content
     * type is <code>"application/json; charset=utf-8"</code>.
     * <p/>
     * NOTICE: If you need to change the JSON Path, i.e. the path which this GUI employs to do "active" operations, you
     * can do so by setting the JS global variable "matsbm_json_path" when outputting the HTML, overriding the default
     * which is to use the current URL path (i.e. the same as the GUI is served on). They may be on the same path since
     * the HTML is served using GET, while the JSON uses PUT and DELETE with header "Content-Type: application/json".
     * (Read the 'matsbrokermonitor.js' file for details.)
     */
    void json(Appendable out, Map<String, String[]> requestParameters, String requestBody, AccessControl ac)
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
        default String username() {
            return "{unknown}";
        }

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

    /**
     * @param username the username to use in the {@link AccessControl#username()} method.
     * @return an {@link AccessControl} which allows all operations.
     */
    static AccessControl getAccessControlAllowAll(String username) {
        return new AccessControl() {
            @Override
            public String username() {
                return username;
            }

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
        };
    }

    AccessControl ACCESS_CONTROL_ALLOW_ALL = getAccessControlAllowAll("{unknown}");

    class AccessDeniedException extends RuntimeException {
        public AccessDeniedException(String message) {
            super(message);
        }
    }
}
