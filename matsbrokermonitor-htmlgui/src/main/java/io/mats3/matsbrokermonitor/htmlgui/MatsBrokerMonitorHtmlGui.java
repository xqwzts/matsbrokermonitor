package io.mats3.matsbrokermonitor.htmlgui;

import java.io.IOException;
import java.util.Map;

/**
 * @author Endre Stølsvik 2022-01-02 12:19 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerMonitorHtmlGui {

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    void getStyleSheet(Appendable out) throws IOException;

    /**
     * Note: The return from this method is static, and should only be included once per HTML page.
     */
    void getJavaScript(Appendable out) throws IOException;



    void createOverview(Appendable out, Map<String, String[]> requestParameters) throws IOException;

}
