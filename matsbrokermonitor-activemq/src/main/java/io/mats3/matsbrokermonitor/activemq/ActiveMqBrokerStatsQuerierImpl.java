package io.mats3.matsbrokermonitor.activemq;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endre Stølsvik 2021-12-20 18:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ActiveMqBrokerStatsQuerierImpl implements ActiveMqBrokerStatsQuerier, Statics {

    private static final Logger log = LoggerFactory.getLogger(ActiveMqBrokerStatsQuerierImpl.class);

    private static final String CORRELATION_ID_PREFIX_SCHEDULED = "Scheduled";
    private static final String CORRELATION_ID_PREFIX_FORCED = "Forced";
    private static final String CORRELATION_ID_PARAMETER_FULL_UPDATE = "fullUpdate";
    private static final String CORRELATION_ID_PARAMETER_NODE_ID = "nodeId";

    private String _nodeId = Long.toString(Math.abs(ThreadLocalRandom.current().nextLong()), 36)
            + Long.toString(Math.abs(ThreadLocalRandom.current().nextLong()), 36);

    private final ConnectionFactory _connectionFactory;
    private final Clock _clock;
    private final long _updateIntervalMillis;

    /**
     * 2.5 minutes query interval.
     */
    static ActiveMqBrokerStatsQuerierImpl create(ConnectionFactory connectionFactory) {
        return create(connectionFactory, DEFAULT_UPDATE_INTERVAL_MILLIS);
    }

    /**
     * Using specified query interval, should not be too small, in particular if lots of queues and topics, as each
     * queue and topic gets its own statistics reply message: Less than 30 seconds would be much too often.
     */
    static ActiveMqBrokerStatsQuerierImpl create(ConnectionFactory connectionFactory, long updateIntervalMillis) {
        return new ActiveMqBrokerStatsQuerierImpl(connectionFactory, Clock.systemUTC(), updateIntervalMillis);
    }

    private ActiveMqBrokerStatsQuerierImpl(ConnectionFactory connectionFactory, Clock clock,
            long updateIntervalMillis) {
        _connectionFactory = connectionFactory;
        _clock = clock;
        _updateIntervalMillis = updateIntervalMillis;
    }

    private enum RunStatus {
        NOT_STARTED,

        RUNNING,

        CLOSED
    }

    private String _matsDestinationPrefix = "mats.";

    private final Deque<String> _waitObject_And_ForceUpdateOutstandingCorrelationIds = new ArrayDeque<>();

    // RunStatus: NOT_STARTED -> RUNNING -> CLOSED. Not restartable.
    private volatile RunStatus _runStatus = RunStatus.NOT_STARTED;

    private volatile Thread _sendStatsRequestMessages_Thread;
    private volatile Thread _receiveDestinationsStatsReplyMessages_Thread;
    private volatile Connection _receiveDestinationsStatsReplyMessages_Connection;

    private volatile long _lastFullUpdatePropagatedMillis;

    private volatile long _lastStatsUpdateMessageReceived = System.currentTimeMillis();

    private final AtomicInteger _countOfDestinationsReceivedAfterRequest = new AtomicInteger();
    private volatile long _nanosAtStart_RequestQuery = 0;

    private volatile BrokerStatsDto _currentBrokerStatsDto;
    private final ConcurrentNavigableMap<String, DestinationStatsDto> _currentDestinationStatsDtos = new ConcurrentSkipListMap<>();

    private final CopyOnWriteArrayList<Consumer<ActiveMqBrokerStatsEvent>> _listeners = new CopyOnWriteArrayList<>();

    @Override
    public void setNodeId(String nodeId) {
        _nodeId = nodeId;
    }

    public void start() {
        if (_runStatus == RunStatus.CLOSED) {
            throw new IllegalStateException("Asked to start, but runStatus==CLOSED, so have already been closed.");
        }
        if (_runStatus == RunStatus.RUNNING) {
            log.info("Asked to start, but already running. Ignoring.");
            return;
        }
        // First set _runStatus to RUNNING, so threads will be happy
        _runStatus = RunStatus.RUNNING;
        log.info("Starting ActiveMQ Broker statistics querier.");
        String id = "(Querier@" + Integer.toHexString(System.identityHashCode(this)) + ")";
        _sendStatsRequestMessages_Thread = new Thread(this::sendStatsRequestMessages,
                "MatsBrokerMonitor.ActiveMQ: Send Statistics request messages " + id);
        _receiveDestinationsStatsReplyMessages_Thread = new Thread(this::receiveStatisticsReplyMessagesRunnable,
                "MatsBrokerMonitor.ActiveMQ: Receive&Process Statistics reply messages " + id);

        // :: First start reply consumer
        _receiveDestinationsStatsReplyMessages_Thread.start();
        // .. then starting requester (it will chill a small tad after getting connection before doing first request)
        _sendStatsRequestMessages_Thread.start();
    }

    @Override
    public void close() {
        if (_runStatus == RunStatus.CLOSED) {
            log.warn("Asked to close, but runStatus==CLOSED, so must already have been stopped. Ignoring.");
            return;
        }
        if (_runStatus == RunStatus.NOT_STARTED) {
            log.warn("Asked to close, but runStatus==NOT_STARTED, so have never been started."
                    + " Setting directly to CLOSED.");
            _runStatus = RunStatus.CLOSED;
            return;
        }

        // First set _runStatus to CLOSED, so that any loops and checks will exit.
        _runStatus = RunStatus.CLOSED;
        log.info("Asked to close. Set runStatus to CLOSED, closing Connections and interrupting threads.");
        // Notify the request sender - it'll close the Connection on its way out.
        synchronized (_waitObject_And_ForceUpdateOutstandingCorrelationIds) {
            _waitObject_And_ForceUpdateOutstandingCorrelationIds.notifyAll();
        }
        // Closing Connections for the receiver - they'll wake up from 'con.receive()'.
        closeConnectionIfNonNullIgnoreException(_receiveDestinationsStatsReplyMessages_Connection);
        // Check that all threads exit
        try {
            _sendStatsRequestMessages_Thread.join(TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN);
            _receiveDestinationsStatsReplyMessages_Thread.join(TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN);
        }
        catch (InterruptedException e) {
            /* ignore */
        }
        // .. interrupt the request sender if the above didn't work.
        interruptThread(_sendStatsRequestMessages_Thread);
        // .. interrupt the receiver too if they haven't gotten out.
        interruptThread(_receiveDestinationsStatsReplyMessages_Thread);
        // Null out Threads
        _sendStatsRequestMessages_Thread = null;
        _receiveDestinationsStatsReplyMessages_Thread = null;
    }

    @Override
    public void setMatsDestinationPrefix(String matsDestinationPrefix) {
        if (_runStatus != RunStatus.NOT_STARTED) {
            throw new IllegalStateException("Tried setting matsDestinationPrefix, but runStatus != NOT_STARTED.");
        }
        _matsDestinationPrefix = matsDestinationPrefix;
    }

    private static void closeConnectionIfNonNullIgnoreException(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        }
        catch (JMSException e) {
            log.warn("Got [" + e.getClass().getSimpleName() + "] when trying to close Connection [" + connection
                    + "], ignoring.");
        }
    }

    private void interruptThread(Thread thread) {
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public void registerListener(Consumer<ActiveMqBrokerStatsEvent> listener) {
        _listeners.add(listener);
    }

    private String constructCorrelationIdToSend(boolean forced, boolean fullUpdate, String correlationId) {
        return (forced ? CORRELATION_ID_PREFIX_FORCED : CORRELATION_ID_PREFIX_SCHEDULED)
                + ":" + CORRELATION_ID_PARAMETER_FULL_UPDATE + "." + fullUpdate
                + ":" + CORRELATION_ID_PARAMETER_NODE_ID + "." + _nodeId
                + ":" + correlationId;
    }

    private static CorrSplit splitCorrelation(String sentCorrelationid) {
        int firstColon = sentCorrelationid.indexOf(':');
        String prefix = sentCorrelationid.substring(0, firstColon);
        int secondColon = sentCorrelationid.indexOf(':', firstColon + 1);
        String full = sentCorrelationid.substring(firstColon + 1, secondColon);
        int thirdColon = sentCorrelationid.indexOf(':', secondColon + 1);
        String node = sentCorrelationid.substring(secondColon + 1, thirdColon);
        String correlationId = sentCorrelationid.substring(thirdColon + 1);

        boolean forced = CORRELATION_ID_PREFIX_FORCED.equals(prefix);
        boolean fullUpdate = full.contains("true");
        String nodeId = node.substring(CORRELATION_ID_PARAMETER_NODE_ID.length() + 1);

        return new CorrSplit(forced, fullUpdate, nodeId, correlationId);
    }

    private static class CorrSplit {
        final boolean forced;
        final boolean fullUpdate;
        final String nodeId;
        final String correlationId;

        public CorrSplit(boolean forced, boolean fullUpdate, String nodeId, String correlationId) {
            this.forced = forced;
            this.fullUpdate = fullUpdate;
            this.nodeId = nodeId;
            this.correlationId = correlationId;
        }
    }

    @Override
    public void forceUpdate(String correlationId, boolean fullUpdate) {
        synchronized (_waitObject_And_ForceUpdateOutstandingCorrelationIds) {
            _waitObject_And_ForceUpdateOutstandingCorrelationIds.add(constructCorrelationIdToSend(true, fullUpdate,
                    correlationId));
            // ?: Are there WAY too many outstanding correlation Ids?
            if (_waitObject_And_ForceUpdateOutstandingCorrelationIds
                    .size() > MAX_NUMBER_OF_OUTSTANDING_CORRELATION_IDS) {
                // -> Yes, so then we're actually screwed - there are obviously no response from the broker
                log.error("We have " + MAX_NUMBER_OF_OUTSTANDING_CORRELATION_IDS + " outstanding forced updates with"
                        + " correlationIds, which implies that the broker does not respond. We'll just ditch them all"
                        + " to prevent OOME; you need to figure out the problem: Probably the"
                        + " Statistics[Broker]Plugin is not installed on the ActiveMQ server.");
                _waitObject_And_ForceUpdateOutstandingCorrelationIds.clear();
            }
            _waitObject_And_ForceUpdateOutstandingCorrelationIds.notifyAll();
        }
    }

    private static class ActiveMqBrokerStatsEventImpl implements ActiveMqBrokerStatsEvent {
        private final String _correlationId;
        private final boolean _fullUpdate;
        private final boolean _statsEventOriginatedOnThisNode;
        private final String _originatingNodeId;
        private final double _requestReplyLatencyMillis;

        public ActiveMqBrokerStatsEventImpl(String correlationId, boolean fullUpdate,
                boolean statsEventOriginatedOnThisNode,
                String originatingNodeId, double requestReplyLatencyMillis) {
            _correlationId = correlationId;
            _fullUpdate = fullUpdate;
            _statsEventOriginatedOnThisNode = statsEventOriginatedOnThisNode;
            _originatingNodeId = originatingNodeId;
            _requestReplyLatencyMillis = requestReplyLatencyMillis;
        }

        @Override
        public Optional<String> getCorrelationId() {
            return Optional.ofNullable(_correlationId);
        }

        @Override
        public boolean isFullUpdate() {
            return _fullUpdate;
        }

        @Override
        public boolean isStatsEventOriginatedOnThisNode() {
            return _statsEventOriginatedOnThisNode;
        }

        @Override
        public Optional<String> getOriginatingNodeId() {
            return Optional.ofNullable(_originatingNodeId);
        }

        @Override
        public OptionalDouble getStatsRequestReplyLatencyMillis() {
            return _requestReplyLatencyMillis >= 0
                    ? OptionalDouble.of(_requestReplyLatencyMillis)
                    : OptionalDouble.empty();
        }
    }

    public Optional<BrokerStatsDto> getCurrentBrokerStatsDto() {
        return Optional.ofNullable(_currentBrokerStatsDto);
    }

    public ConcurrentNavigableMap<String, DestinationStatsDto> getCurrentDestinationStatsDtos() {
        return _currentDestinationStatsDtos;
    }

    // ===== IMPLEMENTATION =====

    private void sendStatsRequestMessages() {
        Connection sendRequestMessages_Connection = null;
        while (_runStatus == RunStatus.RUNNING) {
            try {
                sendRequestMessages_Connection = _connectionFactory.createConnection();
                sendRequestMessages_Connection.start();
                Session session = sendRequestMessages_Connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(null);
                // If there are no receiver (StatisticsPlugin not installed), then try to prevent build-up of msgs.
                producer.setTimeToLive(5 * 60 * 1000); // 5 minutes.
                // Just to point out that there is no need to save these messages if they cannot be handled.
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                /*
                 * So, here I'll throw in a bit of premature optimization: If the MatsDestinationPrefix is a proper
                 * "path-based" prefix with a dot as last character, like it is per default ("mats."), we'll use a
                 * specific query that only requests these. However, we'll then also need to ask specifically for the
                 * DLQs, which again are pre-prefixed with "DLQ.", and for the global DLQ just in case the user hasn't
                 * configured his message broker sanely with /individual DLQ policies/.
                 *
                 * Thus: If the MatsDestinationPrefix isn't set, or it is a crap prefix that cannot be used in the
                 * wildcard syntax of ActiveMQ (e.g. "endre:", which doesn't end in a dot), we ask one query to get
                 * every single queue and topic (well, two, since one for queues and one for topics, go figure) - but
                 * then get tons of replies which we don't care about (at least all advisory topics for every single
                 * queue and topic, and these statistics queues and topics - as well as any other queues that the user
                 * also run on the ActiveMQ instance). If it is a good prefix that can be used with the wildcard syntax,
                 * we have to ask three queries (well, four), but only get exactly the ones we care about.
                 *
                 * (The reason for this badness is my current main user of Mats which for legacy reasons employ such a
                 * bad prefix.)
                 */

                Queue requestQueuesQueue_main;
                Topic requestTopicsTopic_main;
                Queue requestQueuesQueue_individualDlq;
                Queue requestQueuesQueue_globalDlq;

                // ?: Do we have a proper "path-style" prefix, i.e. ending with a "."?
                if ((_matsDestinationPrefix != null) && _matsDestinationPrefix.endsWith(".")) {
                    // -> Yes, this is a proper "path-style" prefix, so we can reduce the query from "all" to "mats
                    // only".
                    String queryRequestDestination_specific = QUERY_REQUEST_DESTINATION_PREFIX + "."
                            + _matsDestinationPrefix + ">";
                    String queryRequestDestination_specific_DLQ = QUERY_REQUEST_DESTINATION_PREFIX + ".DLQ."
                            + _matsDestinationPrefix + ">";
                    log.info("The matsDestinationPrefix was set to [" + _matsDestinationPrefix + "], and it is a proper"
                            + " \"path-style\" prefix (i.e. ending with a dot), thus restricting the query destination"
                            + " by employing [" + queryRequestDestination_specific + "], ["
                            + queryRequestDestination_specific_DLQ + "]"
                            + " and the Global DLQ [" + Statics.ACTIVE_MQ_GLOBAL_DLQ_NAME + "]");
                    requestQueuesQueue_main = session.createQueue(queryRequestDestination_specific);
                    requestTopicsTopic_main = session.createTopic(queryRequestDestination_specific);
                    requestQueuesQueue_individualDlq = session.createQueue(queryRequestDestination_specific_DLQ);
                    requestQueuesQueue_globalDlq = session.createQueue(QUERY_REQUEST_DESTINATION_PREFIX + "."
                            + Statics.ACTIVE_MQ_GLOBAL_DLQ_NAME);
                }
                else {
                    // -> No, this is a bad prefix that cannot utilize the wildcard syntax, so have to ask for every
                    // single queue and topic.
                    String queryRequestDestination_all = QUERY_REQUEST_DESTINATION_PREFIX + ".>";

                    log.info("The matsDestinationPrefix was set to [" + _matsDestinationPrefix + "], but this isn't"
                            + " a proper \"path-style\" prefix (it is not ending with a dot), thus cannot restrict"
                            + " the query to mats-specific destinations, but must query for all destinations by"
                            + " employing [" + queryRequestDestination_all + "]");
                    requestQueuesQueue_main = session.createQueue(queryRequestDestination_all);
                    requestTopicsTopic_main = session.createTopic(queryRequestDestination_all);
                    // Since the two above gets all queues and topics, the DLQs will also be gotten.
                    requestQueuesQueue_individualDlq = null;
                    requestQueuesQueue_globalDlq = null;
                }

                // :: Create queue for the "zero-termination", sent in a separate request query
                // NOTE: We just ask for a queue which shall not be there, but request the "zero termination".
                Queue requestQueuesQueue_zeroQueuesMatchWithNullTermination = session.createQueue(
                        QUERY_REQUEST_DESTINATION_PREFIX + ".No_Match_Whatsoever_"
                                + Long.toString(Math.abs(ThreadLocalRandom.current().nextLong()), 36));

                // Request BrokerStats topic
                Topic requestBrokerTopic = session.createTopic(QUERY_REQUEST_BROKER);
                // Reply topic for all statistics messages (this thread)
                Topic replyStatisticsTopic = session.createTopic(QUERY_REPLY_STATISTICS_TOPIC);

                // Chill a small tad before sending first request, so that receivers hopefully have started.
                // Notice: It isn't particularly bad if they haven't, they'll just miss the first request/reply.
                chill((long) (CHILL_MILLIS_BEFORE_FIRST_STATS_REQUEST
                        * (1 + ThreadLocalRandom.current().nextDouble())));

                String correlationIdToSend = null;

                while (_runStatus == RunStatus.RUNNING) {
                    // :: Request stats for Broker
                    Message requestBrokerMsg = session.createMessage();
                    requestBrokerMsg.setJMSReplyTo(replyStatisticsTopic);
                    producer.send(requestBrokerTopic, requestBrokerMsg);

                    // ::: Destinations
                    // (Notice: Directing replyTo for both Queues and Topics reply to same receiver.)
                    // :: Request stats for Queues
                    Message requestQueuesMsg = session.createMessage();
                    set_includeFirstMessageTimestamp(requestQueuesMsg);
                    requestQueuesMsg.setJMSReplyTo(replyStatisticsTopic);

                    // :: Request stats for Topics
                    Message requestTopicsMsg = session.createMessage();
                    set_includeFirstMessageTimestamp(requestTopicsMsg);
                    requestTopicsMsg.setJMSReplyTo(replyStatisticsTopic);

                    // :: Send destination stats messages
                    _countOfDestinationsReceivedAfterRequest.set(0);
                    _nanosAtStart_RequestQuery = System.nanoTime();
                    producer.send(requestQueuesQueue_main, requestQueuesMsg);
                    producer.send(requestTopicsTopic_main, requestTopicsMsg);

                    // ?: Are we using a proper "path-style" mats destination prefix?
                    if (requestQueuesQueue_individualDlq != null) {
                        // -> Yes, we're using a "path-style" mats destination prefix.
                        // The main queue (and topic) query is then a specific query for only those relevant queues.
                        // Therefore, we'll also need to ask for the individual DLQs, and for the ActiveMQ Global DLQ.
                        // (Asking for the Global DLQ is just to point things out for users that haven't configured
                        // individual DLQ policy. We won't try to fix this problem fully, i.e. won't "distribute"
                        // these DLQs to the individual mats stages they really belong to, as it is pretty much
                        // infeasible if there are more than very few, and that the proper fix is very simple: Use a
                        // frikkin' individual DLQ policy.)
                        Message requestIndividualDlqMsg = session.createMessage();
                        set_includeFirstMessageTimestamp(requestIndividualDlqMsg);
                        requestIndividualDlqMsg.setJMSReplyTo(replyStatisticsTopic);

                        Message requestGlobalDlqMsg = session.createMessage();
                        set_includeFirstMessageTimestamp(requestGlobalDlqMsg);
                        requestGlobalDlqMsg.setJMSReplyTo(replyStatisticsTopic);

                        producer.send(requestQueuesQueue_individualDlq, requestIndividualDlqMsg);
                        producer.send(requestQueuesQueue_globalDlq, requestGlobalDlqMsg);
                    }

                    // :: Send the null-termination query (query w/o any destination replies, only the null-terminator)

                    // Do we currently have a specific correlationId?
                    if (correlationIdToSend == null) {
                        // -> No specific correlation Id, so make a "normal" Scheduled correlationId.
                        // Should we do a full-update, since long time since last full update?
                        boolean timeBasedFullUpdate = (System.currentTimeMillis()
                                - _lastFullUpdatePropagatedMillis) > FULL_UPDATE_INTERVAL;
                        // A random correlationId. Not really used.
                        String random = Long.toString(ThreadLocalRandom.current().nextLong(), 36);
                        correlationIdToSend = constructCorrelationIdToSend(false, timeBasedFullUpdate, random);
                    }

                    // .. construct the null-termination message
                    Message requestNullTermination = session.createMessage();
                    requestNullTermination.setJMSReplyTo(replyStatisticsTopic);
                    // .. set the special property which directs the StatisticsPlugin to "empty terminate" the replies.
                    requestNullTermination.setBooleanProperty(QUERY_REQUEST_DENOTE_END_LIST, true);
                    requestNullTermination.setJMSCorrelationID(correlationIdToSend);
                    producer.send(requestQueuesQueue_zeroQueuesMatchWithNullTermination, requestNullTermination);

                    // We've done this job, clear out correlationId
                    correlationIdToSend = null;

                    // :: Chill and loop
                    synchronized (_waitObject_And_ForceUpdateOutstandingCorrelationIds) {
                        // :: Go into wait: Either we're waking by interval, or by forced update
                        // Loop till we decide that we should do request.
                        while (_runStatus == RunStatus.RUNNING) {
                            // ?: Do we have a forced update waiting?
                            if (!_waitObject_And_ForceUpdateOutstandingCorrelationIds.isEmpty()) {
                                // -> Yes, so pop the force update correlationId before looping.
                                correlationIdToSend = _waitObject_And_ForceUpdateOutstandingCorrelationIds.remove();
                                // Break out of wait-loop
                                break;
                            }
                            // E-> No forced update, so calculate how long to wait
                            // 10% randomness, 5% to both sides
                            long randomRange = (long) Math.max(150, _updateIntervalMillis * .1d);
                            long randomness = Math.round(ThreadLocalRandom.current().nextDouble() * randomRange);
                            long currentWait = Math.max(1, _updateIntervalMillis - (randomRange / 2) + randomness);

                            // Do wait.
                            _waitObject_And_ForceUpdateOutstandingCorrelationIds.wait(currentWait);

                            // ?: Not running anymore?
                            if (_runStatus != RunStatus.RUNNING) {
                                // -> Break out of wait loop (and then all the way out, due to loop conditionals.).
                                break;
                            }
                            // ?: Forced update waiting?
                            if (!_waitObject_And_ForceUpdateOutstandingCorrelationIds.isEmpty()) {
                                // -> Loop (waitloop) to get correlationId (and then break).
                                continue;
                            }

                            // :: Check if we actually should do the request, or if the other node has already done it.

                            // ?: Is the last received stats message older than 75% of our interval?
                            if (_lastStatsUpdateMessageReceived < (System.currentTimeMillis()
                                    - (_updateIntervalMillis * 0.75d))) {
                                // -> Yes, the message is overdue enough, so let's do the request
                                log.debug("Last received StatsUpdateMessage is too old (I am probably first of the"
                                        + " nodes, or the only node); I will do the request");
                                // Break out of wait-loop
                                break;
                            }
                            else {
                                log.debug("Last received update is pretty fresh (probably gotten from another node);"
                                        + " I will NOT do the request.");
                            }
                        }
                    }
                }
            }
            catch (Throwable t) {
                // ?: Exiting?
                if (_runStatus != RunStatus.RUNNING) {
                    // -> Yes, exiting, so get out.
                    break;
                }
                log.warn("Got a [" + t.getClass().getSimpleName() + "] in the query-loop."
                        + " Attempting to close JMS Connection if gotten, then chill-waiting, then trying again.", t);
                closeConnectionIfNonNullIgnoreException(sendRequestMessages_Connection);
                chill(CHILL_MILLIS_WAIT_AFTER_THROWABLE_IN_RECEIVE_LOOPS);
            }
        }
        // To exit, we're signalled via interrupt - it is our job to close Connection (it is a local variable)
        closeConnectionIfNonNullIgnoreException(sendRequestMessages_Connection);
        log.info("Got asked to exit, and that we do!");
    }

    private void set_includeFirstMessageTimestamp(Message msg) throws JMSException {
        msg.setBooleanProperty(QUERY_REQUEST_DESTINATION_INCLUDE_FIRST_MESSAGE_TIMESTAMP, true);
    }

    // Only used within receiveStatisticsReplyMessagesRunnable-thread.
    private long _millisAtLastLogline;
    private int _numberOfSuppressedLoglines;

    private int _numberOfRequestRepliesOnThisNode;
    private long _numberOfNanosTotalBetweenRequestAndEvent;

    @SuppressWarnings("unchecked")
    private void receiveStatisticsReplyMessagesRunnable() {
        OUTERLOOP: while (_runStatus == RunStatus.RUNNING) {
            try {
                _receiveDestinationsStatsReplyMessages_Connection = _connectionFactory.createConnection();
                _receiveDestinationsStatsReplyMessages_Connection.start();
                Session session = _receiveDestinationsStatsReplyMessages_Connection.createSession(false,
                        Session.AUTO_ACKNOWLEDGE);
                Topic replyTopic = session.createTopic(QUERY_REPLY_STATISTICS_TOPIC);
                MessageConsumer consumer = session.createConsumer(replyTopic);

                long nanosAtEnd_lastMessageReceived = 0;
                while (_runStatus == RunStatus.RUNNING) {

                    // Note: The logic here is that we'll get a bunch of statistics reply messages, ended by either
                    // an explicit "empty terminator", or by timing out. This first, timed receive is for the "bunch",
                    // while the later, indefinite receive is for waiting for the next batch.

                    // Since I do not feel utterly confident about the ordering of the messages vs. the multiple
                    // requests and the last "empty terminator", we should also handle that a bunch is split into
                    // two parts: Some replies, the empty terminator, and the rest of the replies. This is not handled
                    // /good/, but we will at least receive the stats messages. However, the fired event will be too
                    // early. This will definitely be a "eventually consistent" scenario - so lets hope the ordering
                    // holds, in which case we have a very good consistency wrt. when sending the update event.

                    // The net effect here is that we will receive stats updates for everything we want, but the
                    // ordering and clean division into "sets" based on the requests being sent cannot be totally
                    // guaranteed - both due to mentioned ordering, but also the fact that there should be multiple
                    // nodes running this (so that we can get interleaving of requests), and that downstream can at
                    // any point request a forced update request, which might come right after a scheduled update req.

                    // However, since we do stack everything we get up into a persistent memory map on this side, we
                    // will just get more and more current information. As long as we eventually notify our listeners
                    // (the ActiveMqMatsBrokerMonitor), the downstream will get a good view about the broker state.
                    // (The map is scavenged based on time: If there are old destinations that haven't gotten an update
                    // for a longish time, it is assumed that the destination is gone from the broker).

                    // Go into timed receive (receiving "the rest of the bunch", but handling startup)
                    MapMessage statsMsg = (MapMessage) consumer.receive(
                            TIMEOUT_MILLIS_FOR_LAST_MESSAGE_IN_BATCH_FOR_DESTINATION_STATS);

                    // NOTE: We're using an undocumented feature of ActiveMQ's StatisticsBrokerPlugin whereby if we add
                    // a special marker to the query, the replies will be "empty terminated" by an empty MapMessage.

                    // ?: Did we either get a null, or get an "empty" MapMessage, BUT runFlag still true?
                    // (Null would be timeout for stats, empty MapMessage would be the "null termination" for stats.)
                    if (((statsMsg == null) || (statsMsg.getObject("brokerName") == null))
                            && (_runStatus == RunStatus.RUNNING)) {
                        // -> null or empty, but runStatus==RUNNING, so this was a timeout or "terminator".

                        // :: Extract information from the JMS Message's CorrelationId.
                        String correlationId = null;
                        boolean isFullUpdate = false;
                        String originatingNodeId = null;
                        boolean requestSameNode = false;
                        if (statsMsg != null) {
                            String raw = statsMsg.getJMSCorrelationID();
                            if (raw != null) {
                                CorrSplit corrSplit = splitCorrelation(raw);
                                // Only use the correlationId if it was supplied via a forceUpdate.
                                correlationId = corrSplit.forced ? corrSplit.correlationId : null;
                                isFullUpdate = corrSplit.fullUpdate;
                                originatingNodeId = corrSplit.nodeId;
                                requestSameNode = _nodeId.equals(originatingNodeId);
                            }
                        }

                        // ?: Have we gotten (bunch of) stats messages by now?
                        if (_countOfDestinationsReceivedAfterRequest.get() > 0) {
                            // -> We've gotten 1 or more stats messages.
                            // We've now (hopefully) received a full set of destination stats.
                            long nowMillis = System.currentTimeMillis();
                            long nanosBetweenQueryAndNow = System.nanoTime() - _nanosAtStart_RequestQuery;

                            // ?: Should we log this logline, or suppress it?
                            if ((nowMillis - _millisAtLastLogline) > LOGLINE_SUPPRESSION_MILLIS) {
                                // -> Log it!
                                String msg = "Statistics updates received! [Suppressed loglines since last: '"
                                        + _numberOfSuppressedLoglines + "'";
                                if (_numberOfRequestRepliesOnThisNode > 0) {
                                    long averageNanos = _numberOfNanosTotalBetweenRequestAndEvent
                                            / _numberOfRequestRepliesOnThisNode;
                                    msg += ", average time for each request-to-last-message-of-batch: '"
                                            + nanos3(averageNanos);
                                }
                                msg += "] -- We've received a batch of ["
                                        + _countOfDestinationsReceivedAfterRequest.get()
                                        + "] destination stats messages, current number of destinations ["
                                        + _currentDestinationStatsDtos.size()
                                        + "]";
                                if (requestSameNode) {
                                    long nanosBetweenQueryAndLastMessageOfBatch = nanosAtEnd_lastMessageReceived
                                            - _nanosAtStart_RequestQuery;

                                    msg += ", time taken between request sent and last message of reply batch received:"
                                            + " [" + nanos3(nanosBetweenQueryAndLastMessageOfBatch)
                                            + "] ms - this logline is [" + nanos3(nanosBetweenQueryAndNow)
                                            + "] ms since request. Notifying listeners.";
                                }
                                log.info(msg);

                                // Zero out suppression-counts
                                _numberOfNanosTotalBetweenRequestAndEvent = 0;
                                _numberOfSuppressedLoglines = 0;
                                // Note the time we logged
                                _millisAtLastLogline = nowMillis;
                            }
                            else {
                                // -> No, suppress log line. Increase "suppression-stats".
                                _numberOfSuppressedLoglines++;
                                if (requestSameNode) {
                                    _numberOfRequestRepliesOnThisNode++;
                                    _numberOfNanosTotalBetweenRequestAndEvent += nanosBetweenQueryAndNow;
                                }
                            }

                            /*
                             * :: If this is a full update, then note this. We decide whether full update on the
                             * request-sending side, and note the last full update on ActiveMq _replies received_ side,
                             * so that if the other node did the full update _request_, all nodes will notice and agree
                             * on when the last full update was propagated. (The sender will automatically tag request
                             * as full-update if > X mins since last)
                             */
                            if (isFullUpdate) {
                                _lastFullUpdatePropagatedMillis = nowMillis;
                            }

                            /*
                             * Clean out old destinations by scavenging stats which no longer is getting updates (i.e.
                             * not existing anymore). Would have been nice to force-fullupdate when this happens, but
                             * that can't be guaranteed since it may happen at different times on the different nodes,
                             * possibly not on the node that did the request (i.e. "same node"), so just don't bother.
                             * It will be propagated once the next full update kicks in.
                             */
                            long longAgo = nowMillis - SCAVENGE_OLD_STATS_SECONDS * 1000;
                            Iterator<DestinationStatsDto> currentStatsIterator = _currentDestinationStatsDtos
                                    .values().iterator();
                            while (currentStatsIterator.hasNext()) {
                                DestinationStatsDto stats = currentStatsIterator.next();
                                if (stats.statsReceived.toEpochMilli() < longAgo) {
                                    log.info("Removing destination which haven't gotten updates for ["
                                            + SCAVENGE_OLD_STATS_SECONDS
                                            + "] seconds: [" + stats.destinationName + "]");
                                    currentStatsIterator.remove();
                                }
                            }

                            // :: Notify listeners
                            ActiveMqBrokerStatsEventImpl event = new ActiveMqBrokerStatsEventImpl(correlationId,
                                    isFullUpdate, requestSameNode, originatingNodeId,
                                    requestSameNode ? nanosBetweenQueryAndNow / 1_000_000d : -1);
                            for (Consumer<ActiveMqBrokerStatsEvent> listener : _listeners) {
                                listener.accept(event);
                            }
                        }
                        else {
                            // -> No, we haven't got any messages - probably startup
                            log.debug("We haven't gotten any stats messages - wait for first one.");
                        }

                        // ----- So, either startup, or we've finished a batch of messages:
                        // Go into indefinite receive: Waiting for _next batch_ of messages triggered by _next stats
                        // request_. (Or null message, resulting from outside close due to shutdown)
                        statsMsg = (MapMessage) consumer.receive();
                    }

                    // ?: Was this a null-message, most probably denoting that we're exiting?
                    if (statsMsg == null) {
                        // -> Yes, null message received.
                        log.info("Received null message from consumer.receive(), assuming shutdown.");
                        if (_runStatus != RunStatus.RUNNING) {
                            break OUTERLOOP;
                        }
                        throw new UnexpectedNullMessageReceivedException("Null message received,"
                                + " but runFlag still true?!");
                    }

                    // :: Evaluate whether it is a BrokerStatistics or DestinationStatistics message
                    if (statsMsg.getObject("destinationName") != null) {
                        // -> Destination stats
                        // This was a destination stats message - count it
                        _countOfDestinationsReceivedAfterRequest.incrementAndGet();
                        // .. log time we received this destination stats message
                        nanosAtEnd_lastMessageReceived = System.nanoTime();

                        DestinationStatsDto destinationStatsDto = mapMessageToDestinationStatsDto(statsMsg);
                        _currentDestinationStatsDtos.put(destinationStatsDto.destinationName, destinationStatsDto);
                        // Update that we've gotten a stats-message
                        _lastStatsUpdateMessageReceived = System.currentTimeMillis();
                        if (log.isTraceEnabled()) log.trace("Got DestinationStats: " + destinationStatsDto);
                    }
                    else if (statsMsg.getObject("storeUsage") != null) {
                        // -> Broker stats
                        _currentBrokerStatsDto = mapMessageToBrokerStatsDto(statsMsg);
                        // Update that we've gotten a stats-message
                        _lastStatsUpdateMessageReceived = System.currentTimeMillis();
                        if (log.isTraceEnabled()) log.trace("Got BrokerStats: " + _currentBrokerStatsDto);
                    }
                    else {
                        log.info("Got a JMS Message that was neither destination stats nor broker stats."
                                + " This is probably an unexpected 'null termination' which happened because more than"
                                + " one instance of the querier sent stats query at the same time, thus getting two"
                                + " replies concurrently. Should not happen often since we have forced randomness in"
                                + " query interval. If it does happen often, either your interval is too low,"
                                + " you have high load or latency, very many queues - or gimme a call! \n" + statsMsg);
                    }
                }
            }
            catch (Throwable t) {
                // ?: Exiting?
                if (_runStatus != RunStatus.RUNNING) {
                    // -> Yes, exiting, so get out.
                    break;
                }
                log.warn("Got a [" + t.getClass().getSimpleName() + "] in the receive-loop."
                        + " Attempting to close JMS Connection if gotten, then chill-waiting, then trying again.", t);
                closeConnectionIfNonNullIgnoreException(_receiveDestinationsStatsReplyMessages_Connection);
                chill(CHILL_MILLIS_WAIT_AFTER_THROWABLE_IN_RECEIVE_LOOPS);
            }
        }
        // To exit, we're signalled via the JMS Connection being closed; Our job is just to null it on our way out.
        _receiveDestinationsStatsReplyMessages_Connection = null;
        log.info("Got asked to exit, and that we do!");
    }

    private BrokerStatsDto mapMessageToBrokerStatsDto(MapMessage mm) throws JMSException {
        BrokerStatsDto dto = new BrokerStatsDto();
        mapMessageToCommonStatsDto(mm, dto);

        dto.stompSsl = (String) mm.getObject("stomp+ssl");
        dto.ssl = (String) mm.getObject("ssl");
        dto.stomp = (String) mm.getObject("stomp");
        dto.openwire = (String) mm.getObject("openwire");
        dto.vm = (String) mm.getObject("vm");

        dto.dataDirectory = (String) mm.getObject("dataDirectory");

        dto.tempUsage = (long) mm.getObject("tempUsage");
        dto.tempLimit = (long) mm.getObject("tempLimit");
        dto.tempPercentUsage = (int) mm.getObject("tempPercentUsage");

        dto.storeUsage = (long) mm.getObject("storeUsage");
        dto.storePercentUsage = (int) mm.getObject("storePercentUsage");
        dto.storeLimit = (long) mm.getObject("storeLimit");

        return dto;
    }

    private DestinationStatsDto mapMessageToDestinationStatsDto(MapMessage mm) throws JMSException {
        DestinationStatsDto dto = new DestinationStatsDto();
        mapMessageToCommonStatsDto(mm, dto);

        dto.destinationName = (String) mm.getObject("destinationName");
        dto.firstMessageTimestamp = getTimestampFromMapMessage(mm, "firstMessageTimestamp");

        return dto;
    }

    private Optional<Instant> getTimestampFromMapMessage(MapMessage mm, String key) throws JMSException {
        long millis = mm.getLong(key);
        return millis != 0 ? Optional.of(Instant.ofEpochMilli(millis)) : Optional.empty();
    }

    private void mapMessageToCommonStatsDto(MapMessage mm, CommonStatsDto dto) throws JMSException {
        dto.statsReceived = Instant.now(_clock);

        dto.brokerId = (String) mm.getObject("brokerId");
        dto.brokerName = (String) mm.getObject("brokerName");
        dto.brokerTime = Instant.ofEpochMilli(mm.getJMSTimestamp());

        dto.size = (long) mm.getObject("size");
        dto.enqueueCount = (long) mm.getObject("enqueueCount");
        dto.dequeueCount = (long) mm.getObject("dequeueCount");
        dto.expiredCount = (long) mm.getObject("expiredCount");
        dto.dispatchCount = (long) mm.getObject("dispatchCount");
        dto.inflightCount = (long) mm.getObject("inflightCount");

        dto.producerCount = (long) mm.getObject("producerCount");
        dto.consumerCount = (long) mm.getObject("consumerCount");

        dto.minEnqueueTime = (double) mm.getObject("minEnqueueTime");
        dto.averageEnqueueTime = (double) mm.getObject("averageEnqueueTime");
        dto.maxEnqueueTime = (double) mm.getObject("maxEnqueueTime");

        dto.memoryUsage = (long) mm.getObject("memoryUsage");
        dto.memoryLimit = (long) mm.getObject("memoryLimit");
        dto.memoryPercentUsage = (int) mm.getObject("memoryPercentUsage");

        dto.averageMessageSize = (long) mm.getObject("averageMessageSize");

        dto.messagesCached = (long) mm.getObject("messagesCached");
    }

    private static class UnexpectedNullMessageReceivedException extends Exception {
        public UnexpectedNullMessageReceivedException(String message) {
            super(message);
        }
    }

    private static void chill(long millis) {
        log.debug("Chilling [" + millis + "] ms.");
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            log.info("Got interrupted while chill-waiting [" + millis + "].");
        }
    }
}
