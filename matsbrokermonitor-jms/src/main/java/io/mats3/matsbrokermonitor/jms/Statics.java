package io.mats3.matsbrokermonitor.jms;

/**
 * @author Endre Stølsvik 2022-01-16 23:24 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface Statics {


    String MATS_DEAD_LETTER_ENDPOINT_ID = "MatsDeadLetterQueue";

    String MDC_MATS_MESSAGE_SYSTEM_ID = "mats.MsgSysId";
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
     * For delete and reissue, how many milliseconds to wait in receive.
     * <p/>
     * Value is <code>750</code>.
     */
    long RECEIVE_TIMEOUT_MILLIS = 750;
}
