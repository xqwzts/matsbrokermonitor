package io.mats3.matsbrokermonitor.activemq;

/**
 * @author Endre Stølsvik 2021-12-28 23:10 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface Statics {

    // :: The /type/ of this broker
    String BROKER_TYPE = "ActiveMQ";

    // :: For ActiveMqMatsBrokerMonitor + ActiveMqBrokerStatsQuerierImpl:

    String ACTIVE_MQ_GLOBAL_DLQ_NAME = "ActiveMQ.DLQ";
    String INDIVIDUAL_DLQ_PREFIX = "DLQ.";

    // :: Two different scavenge intervals - first the primary source, then "follower".

    int SCAVENGE_OLD_STATS_SECONDS = 10 * 60;
    int MAX_NUMBER_OF_OUTSTANDING_CORRELATION_IDS = 150;

    // :: For ActiveMqBrokerStatsQuerierImpl:

    int DEFAULT_UPDATE_INTERVAL_MILLIS = 150_000; // 2.5 minutes
    int CHILL_MILLIS_BEFORE_FIRST_STATS_REQUEST = 500;
    int CHILL_MILLIS_WAIT_AFTER_THROWABLE_IN_RECEIVE_LOOPS = 30 * 1000;
    int TIMEOUT_MILLIS_FOR_LAST_MESSAGE_IN_BATCH_FOR_DESTINATION_STATS = 250;
    int TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN = 500;

    // Time between each logline we want logged
    long LOGLINE_SUPPRESSION_MILLIS = 15 * 60 * 1000;

    // :: For ActiveMqMatsBrokerMonitor:

    // Time between automatic full updates from
    long FULL_UPDATE_INTERVAL = 20 * 60 * 1000;

    String QUERY_REQUEST_BROKER = "ActiveMQ.Statistics.Broker";
    /**
     * Note: This should be postfixed with ".{which destination}", which handles wildcards - so ".>" will return a
     * message for every destination of the same type as which the query was sent on (that is, if the query is sent on a
     * queue, you'll get answers for queues, and sent on topic gives answers for topics).
     */
    String QUERY_REQUEST_DESTINATION_PREFIX = "ActiveMQ.Statistics.Destination";
    String QUERY_REQUEST_DENOTE_END_LIST = "ActiveMQ.Statistics.Destination.List.End.With.Null";
    String QUERY_REQUEST_DESTINATION_INCLUDE_FIRST_MESSAGE_TIMESTAMP = "ActiveMQ.Statistics.Destination.Include.First.Message.Timestamp";

    String QUERY_REPLY_STATISTICS_TOPIC = "matsbrokermonitor.MatsBrokerMonitor.ActiveMQ.Statistics";

    /**
     * Truncate milliseconds to 3 decimals.
     */
    default double ms3(double ms) {
        return Math.round(ms * 1000d) / 1000d;
    }

    /**
     * Truncate milliseconds to 3 decimals.
     */
    default double nanos3(long nanosTaken) {
        return Math.round(nanosTaken / 1000d) / 1000d;
    }

}
