package io.mats3.matsbrokermonitor.htmlgui.impl;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author Endre Stølsvik 2022-03-13 23:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface Statics {

    static String formatTimestamp(Instant instant) {
        long millisAgo = System.currentTimeMillis() - instant.toEpochMilli();
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault()) + " (" + millisSpanToHuman(millisAgo) + ")";
    }

    static String formatTimestamp(long timestamp) {
        return formatTimestamp(Instant.ofEpochMilli(timestamp));
    }

    static String millisSpanToHuman(long millis) {
        if (millis < 60_000) {
            return millis + " ms";
        }
        else {
            Duration d = Duration.ofMillis(millis);
            long days = d.toDays();
            d = d.minusDays(days);
            long hours = d.toHours();
            d = d.minusHours(hours);
            long minutes = d.toMinutes();
            d = d.minusMinutes(minutes);
            long seconds = d.getSeconds();

            StringBuilder buf = new StringBuilder();
            if (days > 0) {
                buf.append(days).append("d");
            }
            if ((hours > 0) || (buf.length() != 0)) {
                if (buf.length() != 0) {
                    buf.append(":");
                }
                buf.append(hours).append("h");
            }
            if ((minutes > 0) || (buf.length() != 0)) {
                if (buf.length() != 0) {
                    buf.append(":");
                }
                buf.append(minutes).append("m");
            }
            if (buf.length() != 0) {
                buf.append(":");
            }
            buf.append(seconds).append("s");
            return buf.toString();
        }
    }


}
