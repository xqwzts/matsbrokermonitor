package io.mats3.matsbrokermonitor.activemq;

import java.io.Closeable;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Consumer;

/**
 * @author Endre Stølsvik 2021-12-28 02:31 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface ActiveMqBrokerStatsQuerier extends Closeable {

    /**
     * @param nodeId
     *            the internal nodeId to decide whether an update came from "this node" or not, used for
     *            {@link ActiveMqBrokerStatsEvent#isStatsEventOriginatedOnThisNode()}. Must be different for each
     *            instance connected to the same ActiveMQ instance. Not really needed, since it works just nice with a
     *            random id (which it uses default), but it can be valuable for debugging.
     */
    void setNodeId(String nodeId);

    void start();

    void close();

    void setMatsDestinationPrefix(String matsDestinationPrefix);

    void registerListener(Consumer<ActiveMqBrokerStatsEvent> listener);

    interface ActiveMqBrokerStatsEvent {
        /**
         * @return the correlationId if this is an event in response to an invocation of
         *         {@link #forceUpdate(String, boolean)}
         */
        Optional<String> getCorrelationId();

        /**
         * @return <code>true</code> if this is an event in response to an invocation of
         *         {@link #forceUpdate(String, boolean)}, and the 'fullUpdate' parameter was true.
         */
        boolean isFullUpdate();

        /**
         * @return <code>true</code> if this event was the result from a request sent from this node.
         */
        boolean isStatsEventOriginatedOnThisNode();

        /**
         * @return the nodeId which performed the request which resulted in this update event - the nodeId is a random
         *         string unless set using {@link ActiveMqBrokerStatsQuerier#setNodeId(String)}.
         */
        Optional<String> getOriginatingNodeId();

        /**
         * @return the number of milliseconds between the statistics request(s) were started, to the (last) response was
         *         received.
         */
        OptionalDouble getStatsRequestReplyLatencyMillis();
    }

    /**
     * Requests ASAP update.
     *
     * @param correlationId
     *            this is only used to propagate through to the update event.
     * @param fullUpdate
     *            this is only used to propagate through to the update event.
     */
    void forceUpdate(String correlationId, boolean fullUpdate);

    Optional<BrokerStatsDto> getCurrentBrokerStatsDto();

    ConcurrentNavigableMap<String, DestinationStatsDto> getCurrentDestinationStatsDtos();

    /**
     * NOTE: Explanations from
     * <a href="https://www.mail-archive.com/users@activemq.apache.org/msg08847.html">users@activemq.apache.org
     * mail-archive: "Re: Topic counts explanation"</a>, or
     * <a href="https://stackoverflow.com/a/1719942/39334">Stackoverflow: What do some of the fields returned from
     * ActiveMQ.Agent query mean?</a>, or
     * <a href="https://activemq.apache.org/how-do-i-find-the-size-of-a-queue">ActiveMQ Docs: How do I find the Size of
     * a Queue</a>
     */
    class CommonStatsDto {
        Instant statsReceived;

        String brokerId;
        String brokerName;
        Instant brokerTime;

        /**
         * The number of messages that currently reside in the queue.
         * <p/>
         * (Endre: Seems to be {@link #enqueueCount} - {@link #dequeueCount}).
         */
        long size;
        /**
         * The number of messages that have been written to the queue over the lifetime of the queue (since last
         * restart).
         */
        long enqueueCount;
        /**
         * The number of messages that have been successfully (i.e., they’ve been acknowledged from the consumer) read
         * off the queue over the lifetime of the queue (since last restart).
         */
        long dequeueCount;
        /**
         * The number of messages that were not delivered because they were expired (since last restart).
         */
        long expiredCount;
        /**
         * The number of messages that have been dispatched (sent) to the consumer over the lifetime of the queue. Note
         * that dispatched messages may not have all been acknowledged (since last restart).
         * <p/>
         * (Endre: This number can evidently be higher than {@link #enqueueCount}, which I assume means that
         * redeliveries are included.)
         */
        long dispatchCount;
        /**
         * The number of messages that have been dispatched and are currently awaiting acknowledgment from the consumer.
         * So as this number decreases, the DequeueCount increases.
         * <p/>
         * (Endre: This number is included in the {@link #size} number, as inflight messages are not yet dequeued).
         */
        long inflightCount;

        long producerCount;
        long consumerCount;

        /**
         * The minimum amount of time that messages remained enqueued.
         */
        double minEnqueueTime;
        /**
         * On average, the amount of time (ms) that messages remained enqueued. Or average time it is taking the
         * consumers to successfully process messages.
         */
        double averageEnqueueTime;
        /**
         * The maximum amount of time that messages remained enqueued.
         */
        double maxEnqueueTime;

        long averageMessageSize;

        long messagesCached;

        long memoryUsage;
        long memoryLimit;
        int memoryPercentUsage;

        String getCommonToString() {
            return "statsReceivedTimeMillis=" + statsReceived +
                    ", brokerId='" + brokerId + '\'' +
                    ", brokerName='" + brokerName + '\'' +
                    ", brokerTime='" + brokerTime + '\'' +
                    ", size=" + size +
                    ", enqueueCount=" + enqueueCount +
                    ", dequeueCount=" + dequeueCount +
                    ", expiredCount=" + expiredCount +
                    ", dispatchCount=" + dispatchCount +
                    ", inflightCount=" + inflightCount +
                    ", producerCount=" + producerCount +
                    ", consumerCount=" + consumerCount +
                    ", minEnqueueTime=" + minEnqueueTime +
                    ", averageEnqueueTime=" + averageEnqueueTime +
                    ", maxEnqueueTime=" + maxEnqueueTime +
                    ", averageMessageSize=" + averageMessageSize +
                    ", messagesCached=" + messagesCached +
                    ", memoryUsage=" + memoryUsage +
                    ", memoryLimit=" + memoryLimit +
                    ", memoryPercentUsage=" + memoryPercentUsage;
        }

        String getCommonJson() {
            return "  \"statsReceivedTimeMillis\"=" + statsReceived + "\n" +
                    "  \"brokerId\"=\"" + brokerId + '"' + "\n" +
                    "  \"brokerName\"=\"" + brokerName + '"' + "\n" +
                    "  \"brokerTime\"=\"" + brokerTime + '"' + "\n" +
                    "  \"size\"=" + size + "\n" +
                    "  \"enqueueCount\"=" + enqueueCount + "\n" +
                    "  \"dequeueCount\"=" + dequeueCount + "\n" +
                    "  \"expiredCount\"=" + expiredCount + "\n" +
                    "  \"dispatchCount\"=" + dispatchCount + "\n" +
                    "  \"inflightCount\"=" + inflightCount + "\n" +
                    "  \"producerCount\"=" + producerCount + "\n" +
                    "  \"consumerCount\"=" + consumerCount + "\n" +
                    "  \"minEnqueueTime\"=" + minEnqueueTime + "\n" +
                    "  \"averageEnqueueTime\"=" + averageEnqueueTime + "\n" +
                    "  \"maxEnqueueTime\"=" + maxEnqueueTime + "\n" +
                    "  \"averageMessageSize\"=" + averageMessageSize + "\n" +
                    "  \"messagesCached\"=" + messagesCached + "\n" +

                    "  \"memoryUsage\"=" + memoryUsage + "\n" +
                    "  \"memoryLimit\"=" + memoryLimit + "\n" +
                    "  \"memoryPercentUsage\"=" + memoryPercentUsage + "\n";
        }
    }

    class BrokerStatsDto extends CommonStatsDto {
        long storeUsage;
        long storeLimit;
        int storePercentUsage;

        long tempUsage;
        long tempLimit;
        int tempPercentUsage;

        String stompSsl;
        String ssl;
        String stomp;
        String openwire;
        String vm;

        String dataDirectory;

        @Override
        public String toString() {
            return "BrokerStatsDto{" +
                    getCommonToString() +
                    ", storeUsage=" + storeUsage +
                    ", storeLimit=" + storeLimit +
                    ", storePercentUsage=" + storePercentUsage +
                    ", tempUsage=" + tempUsage +
                    ", tempLimit=" + tempLimit +
                    ", tempPercentUsage=" + tempPercentUsage +
                    ", stompSsl='" + stompSsl + '\'' +
                    ", ssl='" + ssl + '\'' +
                    ", stomp='" + stomp + '\'' +
                    ", openwire='" + openwire + '\'' +
                    ", vm='" + vm + '\'' +
                    ", dataDirectory='" + dataDirectory + '\'' +
                    '}';
        }

        public String toJson() {
            return "{\n" + getCommonJson() +
                    "  \"storeUsage\"=" + storeUsage + "\n" +
                    "  \"storeLimit\"=" + storeLimit + "\n" +
                    "  \"storePercentUsage\"=" + storePercentUsage + "\n" +
                    "  \"tempUsage\"=" + tempUsage + "\n" +
                    "  \"tempLimit\"=" + tempLimit + "\n" +
                    "  \"tempPercentUsage\"=" + tempPercentUsage + "\n" +

                    "  \"stompSsl\"=\"" + stompSsl + '"' + "\n" +
                    "  \"ssl\"=\"" + ssl + '"' + "\n" +
                    "  \"stomp\"=\"" + stomp + '"' + "\n" +
                    "  \"openwire\"=\"" + openwire + '"' + "\n" +
                    "  \"vm\"=\"" + vm + '"' + "\n" +
                    "  \"dataDirectory\"=\"" + dataDirectory + '"' + "\n" +
                    '}';
        }
    }

    class DestinationStatsDto extends CommonStatsDto {
        String destinationName;
        // Only present if there is a head message present (I created this feature in ActiveMQ!)
        Optional<Instant> firstMessageTimestamp;

        @Override
        public String toString() {
            return "DestinationStatsDto{" +
                    ", destinationName='" + destinationName + '\'' +
                    ", firstMessageTimestamp='" + firstMessageTimestamp + '\'' +
                    ", " +
                    getCommonToString() +
                    '}';
        }

        public String toJson() {
            return "{\n" +
                    "  \"destinationName\"=\"" + destinationName + "\"\n" +
                    "  \"firstMessageTimestamp\"=" +
                    (firstMessageTimestamp.map(instant -> "\"" + instant + "\"").orElse("null")) + "\n" +
                    getCommonJson() +
                    '}';
        }
    }
}
