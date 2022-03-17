package io.mats3.matsbrokermonitor.htmlgui.impl;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

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

    static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Read and write any access modifier fields (e.g. private)
        mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        // Drop nulls
        mapper.setSerializationInclusion(Include.NON_NULL);

        // If props are in JSON that aren't in Java DTO, do not fail.
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Write e.g. Dates as "1975-03-11" instead of timestamp, and instead of array-of-ints [1975, 3, 11].
        // Uses ISO8601 with milliseconds and timezone (if present).
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;

    }

}