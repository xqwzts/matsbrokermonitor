package io.mats3.matsbrokermonitor.jms;

import java.util.Random;

/**
 * @author Endre Stølsvik 2022-01-16 23:24 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface Statics {

    /**
     * For delete and reissue, how many milliseconds to wait in receive.
     * <p>
     * Value is <code>750</code>.
     */
    long RECEIVE_TIMEOUT_MILLIS = 750;

    // :: MDC Keys

    String MDC_MATS_MESSAGE_SYSTEM_ID = "mats.MsgSysId";
    String MDC_MATS_RESULTING_MESSAGE_SYSTEM_ID = "mats.ResultingMsgSysId";
    String MDC_MATS_MESSAGE_ID = "mats.MatsMsgId";
    String MDC_MATS_DESTINATION_TYPE = "mats.DestinationType";

    // .. the following are copied from JmsMatsFactory

    String MDC_TRACE_ID = "traceId";
    String MDC_MATS_STAGE_ID = "mats.StageId";

    // ===== JMS Properties put on the JMSMessage via set[String|Long|Boolean]Property(..)
    // NOTICE: "." is not allowed by JMS (and Apache Artemis complains!), so we use "_".

    String JMS_MSG_PROP_LAST_OPERATION_COOKIE = "mats_LastOperationCookie";
    String JMS_MSG_PROP_LAST_OPERATION_USERNAME = "mats_LastOperationUsername";
    String JMS_MSG_PROP_LAST_OPERATION_COMMENT = "mats_LastOperationComment";
    String JMS_MSG_PROP_OPERATION_FAILED_REASON = "mats_OperationFailedReason"; // If action fails.

    // .. the following are copied from JmsMatsFactory

    String JMS_MSG_PROP_TRACE_ID = "mats_TraceId"; // String
    String JMS_MSG_PROP_MATS_MESSAGE_ID = "mats_MsgId"; // String
    String JMS_MSG_PROP_DISPATCH_TYPE = "mats_DispatchType"; // String
    String JMS_MSG_PROP_MESSAGE_TYPE = "mats_MsgType"; // String
    String JMS_MSG_PROP_FROM = "mats_From"; // String
    String JMS_MSG_PROP_INITIATING_APP = "mats_InitApp"; // String
    String JMS_MSG_PROP_INITIATOR_ID = "mats_InitId"; // String
    String JMS_MSG_PROP_TO = "mats_To"; // String (needed if a message ends up on a global/common DLQ)
    // Four next are set if non-default:
    String JMS_MSG_PROP_INTERACTIVE = "mats_IA"; // Boolean - not set if false
    String JMS_MSG_PROP_NON_PERSISTENT = "mats_NP"; // Boolean - not set if false
    String JMS_MSG_PROP_NO_AUDIT = "mats_NA"; // Boolean - not set if false
    String JMS_MSG_PROP_EXPIRES = "mats_Expires"; // Long - not set if 'never expires'
    // TODO: Delete 'JMS_MSG_PROP_AUDIT' ASAP, latest 2025
    String JMS_MSG_PROP_AUDIT = "mats_Audit"; // Boolean - not set if false

    // :: For 'Mats Managed DLQ Divert' - note that most of these should be cleared when reissued from DLQ!
    String JMS_MSG_PROP_DLQ_EXCEPTION = "mats_dlq_Exception"; // String (not set if DLQed on receive-side)
    String JMS_MSG_PROP_DLQ_REFUSED = "mats_dlq_Refused"; // Boolean (not set if DLQed on receive-side)
    String JMS_MSG_PROP_DLQ_DELIVERY_COUNT = "mats_dlq_DeliveryCount"; // Integer
    String JMS_MSG_PROP_DLQ_DLQ_COUNT = "mats_dlq_DlqCount"; // Integer (NOTE: should be kept when reissued from DLQ!)
    String JMS_MSG_PROP_DLQ_APP_VERSION_AND_HOST = "mats_dlq_AppAndVersion"; // String
    String JMS_MSG_PROP_DLQ_STAGE_ORIGIN = "mats_dlq_StageOrigin"; // String

    Random RANDOM = new Random();

    default String random() {
        String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int length = 8;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }

    /**
     * Converts nanos to millis with a sane number of significant digits ("3.5" significant digits), but assuming that
     * this is not used to measure things that take less than 0.001 milliseconds (in which case it will be "rounded" to
     * 0.0001, 1e-4, as a special value). Takes care of handling the difference between 0 and &gt;0 nanoseconds when
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
