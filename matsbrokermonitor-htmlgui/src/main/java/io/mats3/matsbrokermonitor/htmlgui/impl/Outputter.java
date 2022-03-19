package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.io.IOException;

/**
 * @author Endre Stølsvik 2022-03-19 22:50 - http://stolsvik.com/, endre@stolsvik.com
 */
class Outputter {
    private final Appendable _out;

    Outputter(Appendable out) {
        _out = out;
    }

    Outputter html(CharSequence html) throws IOException {
        _out.append(html);
        return this;
    }

    Outputter DATA(String data) throws IOException {
        _out.append(ESCAPE(data));
        return this;
    }

    Outputter DATA(Number number) throws IOException {
        _out.append(number.toString());
        return this;
    }

    Outputter DATA(Enum<?> enu) throws IOException {
        _out.append(enu.toString());
        return this;
    }

    static String ESCAPE(String data) {
        return data.replace("<", "&lt")
                .replace(">", "&gt;")
                .replace("&", "&amp;")
                .replace("\"", "&#x22;")
                .replace("'", "&#x27;");
    }
}
