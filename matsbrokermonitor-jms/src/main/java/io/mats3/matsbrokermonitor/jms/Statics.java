package io.mats3.matsbrokermonitor.jms;

/**
 * @author Endre Stølsvik 2022-01-16 23:24 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface Statics {

    /**
     * For delete and reissue, how many milliseconds to wait in receive.
     * <p/>
     * Value is <code>750</code>.
     */
    long RECEIVE_TIMEOUT_MILLIS = 750;


    String MDC_MATS_MESSAGE_SYSTEM_ID = "mats.MsgSysId";

    String MDC_MATS_REISSUED_MESSAGE_SYSTEM_ID = "mats.ReissuedMsgSysId";
    String MDC_MATS_MESSAGE_ID = "mats.MatsMsgId";

    // COPIED FROM JmsMatsFactory

    // MDC Keys
    String MDC_TRACE_ID = "traceId";
    String MDC_MATS_STAGE_ID = "mats.StageId";

    // JMS Properties put on the JMSMessage via set[String|Long|Boolean]Property(..)
    String JMS_MSG_PROP_TRACE_ID = "mats_TraceId"; // String
    String JMS_MSG_PROP_MATS_MESSAGE_ID = "mats_MsgId"; // String
    String JMS_MSG_PROP_DISPATCH_TYPE = "mats_DispatchType"; // String
    String JMS_MSG_PROP_MESSAGE_TYPE = "mats_MsgType"; // String
    String JMS_MSG_PROP_INITIALIZING_APP = "mats_InitApp"; // String // note: Added 2022-01-21
    String JMS_MSG_PROP_INITIATOR_ID = "mats_InitId"; // String // note: Added 2022-01-21
    String JMS_MSG_PROP_FROM = "mats_From"; // String
    String JMS_MSG_PROP_TO = "mats_To"; // String
    String JMS_MSG_PROP_AUDIT = "mats_Audit"; // Boolean

    /**
     * Converts nanos to millis with a sane number of significant digits ("3.5" significant digits), but assuming that
     * this is not used to measure things that take less than 0.001 milliseconds (in which case it will be "rounded" to
     * 0.0001, 1e-4, as a special value). Takes care of handling the difference between 0 and >0 nanoseconds when
     * rounding - in that 1 nanosecond will become 0.0001 (1e-4 ms, which if used to measure things that are really
     * short lived might be magnitudes wrong), while 0 will be 0.0 exactly. Note that printing of a double always
     * include the ".0" (unless scientific notation kicks in), which can lead your interpretation slightly astray wrt.
     * accuracy/significant digits when running this over e.g. the number 555_555_555, which will print as "556.0", and
     * 5_555_555_555 prints "5560.0".
     */
    default double ms(long nanosTaken) {
        if (nanosTaken == 0) {
            return 0.0;
        }
        // >=500_000 ms?
        if (nanosTaken >= 1_000_000L * 500_000) {
            // -> Yes, >500_000ms, thus chop into the integer part of the number (zeroing 3 digits)
            // (note: printing of a double always include ".0"), e.g. 612000.0
            return Math.round(nanosTaken / 1_000_000_000d) * 1_000d;
        }
        // >=50_000 ms?
        if (nanosTaken >= 1_000_000L * 50_000) {
            // -> Yes, >50_000ms, thus chop into the integer part of the number (zeroing 2 digits)
            // (note: printing of a double always include ".0"), e.g. 61200.0
            return Math.round(nanosTaken / 100_000_000d) * 100d;
        }
        // >=5_000 ms?
        if (nanosTaken >= 1_000_000L * 5_000) {
            // -> Yes, >5_000ms, thus chop into the integer part of the number (zeroing 1 digit)
            // (note: printing of a double always include ".0"), e.g. 6120.0
            return Math.round(nanosTaken / 10_000_000d) * 10d;
        }
        // >=500 ms?
        if (nanosTaken >= 1_000_000L * 500) {
            // -> Yes, >500ms, so chop off fraction entirely
            // (note: printing of a double always include ".0"), e.g. 612.0
            return Math.round(nanosTaken / 1_000_000d);
        }
        // >=50 ms?
        if (nanosTaken >= 1_000_000L * 50) {
            // -> Yes, >50ms, so use 1 decimal, e.g. 61.2
            return Math.round(nanosTaken / 100_000d) / 10d;
        }
        // >=5 ms?
        if (nanosTaken >= 1_000_000L * 5) {
            // -> Yes, >5ms, so use 2 decimal, e.g. 6.12
            return Math.round(nanosTaken / 10_000d) / 100d;
        }
        // E-> <5 ms
        // Use 3 decimals, but at least '0.0001' if round to zero, so as to point out that it is NOT 0.0d
        // e.g. 0.612
        return Math.max(Math.round(nanosTaken / 1_000d) / 1_000d, 0.0001d);
    }
}
