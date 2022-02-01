package io.mats3.matsbrokermonitor.activemq;

import java.time.Clock;
import java.time.Instant;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
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

    private final ConnectionFactory _connectionFactory;

    private final Clock _clock;

    static ActiveMqBrokerStatsQuerierImpl create(ConnectionFactory connectionFactory) {
        return new ActiveMqBrokerStatsQuerierImpl(connectionFactory, Clock.systemUTC());
    }

    private ActiveMqBrokerStatsQuerierImpl(ConnectionFactory connectionFactory, Clock clock) {
        _connectionFactory = connectionFactory;
        _clock = clock;
    }

    private enum RunStatus {
        NOT_STARTED,

        RUNNING,

        CLOSED
    }

    private String _matsDestinationPrefix;

    private final Object _requestWaitAndPingObject = new Object();
    private int _forceUpdateRequestCount = 0;

    // RunStatus: NOT_STARTED -> RUNNING -> CLOSED. Not restartable.
    private volatile RunStatus _runStatus = RunStatus.NOT_STARTED;
    private volatile Thread _sendStatsRequestMessages_Thread;
    private volatile Thread _receiveBrokerStatsReplyMessages_Thread;
    private volatile Connection _receiveBrokerStatsReplyMessages_Connection;
    private volatile Thread _receiveDestinationsStatsReplyMessages_Thread;
    private volatile Connection _receiveDestinationsStatsReplyMessages_Connection;

    private final AtomicInteger _countOfDestinationsReceivedAfterRequest = new AtomicInteger();
    private volatile long _nanosAtStart_RequestQuery = 0;

    private volatile BrokerStatsDto _currentBrokerStatsDto;
    private final ConcurrentNavigableMap<String, DestinationStatsDto> _currentDestinationStatsDtos = new ConcurrentSkipListMap<>();

    private final CopyOnWriteArrayList<Consumer<ActiveMqBrokerStatsEvent>> _listeners = new CopyOnWriteArrayList<>();

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
        _sendStatsRequestMessages_Thread = new Thread(this::sendStatsRequestMessages,
                "MatsBrokerMonitor.ActiveMQ: Sending Statistics request messages");
        _receiveBrokerStatsReplyMessages_Thread = new Thread(this::receiveBrokerStatsReplyMessages,
                "MatsBrokerMonitor.ActiveMQ: Receive Broker Statistics reply messages");
        _receiveDestinationsStatsReplyMessages_Thread = new Thread(this::receiveDestinationStatsReplyMessages,
                "MatsBrokerMonitor.ActiveMQ: Receive Destination Statistics reply messages");

        // :: First start reply consumers
        _receiveBrokerStatsReplyMessages_Thread.start();
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
        synchronized (_requestWaitAndPingObject) {
            _requestWaitAndPingObject.notifyAll();
        }
        // Closing Connections for the receivers - they'll wake up from 'con.receive()'.
        closeConnectionIgnoreException(_receiveBrokerStatsReplyMessages_Connection);
        closeConnectionIgnoreException(_receiveDestinationsStatsReplyMessages_Connection);
        // Check that all threads exit
        try {
            _sendStatsRequestMessages_Thread.join(TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN);
            _receiveBrokerStatsReplyMessages_Thread.join(TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN);
            _receiveDestinationsStatsReplyMessages_Thread.join(TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN);
        }
        catch (InterruptedException e) {
            /* ignore */
        }
        // .. interrupt the request sender if the above didn't work.
        interruptThread(_sendStatsRequestMessages_Thread);
        // .. interrupt the receivers too if they haven't gotten out.
        interruptThread(_receiveBrokerStatsReplyMessages_Thread);
        interruptThread(_receiveDestinationsStatsReplyMessages_Thread);
        // Null out Threads
        _sendStatsRequestMessages_Thread = null;
        _receiveBrokerStatsReplyMessages_Thread = null;
        _receiveDestinationsStatsReplyMessages_Thread = null;
    }

    @Override
    public void setMatsDestinationPrefix(String matsDestinationPrefix) {
        if (_runStatus != RunStatus.NOT_STARTED) {
            throw new IllegalStateException("Tried setting matsDestinationPrefix, but runStatus != NOT_STARTED.");
        }
        _matsDestinationPrefix = matsDestinationPrefix;
    }

    private void closeConnectionIgnoreException(Connection connection) {
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

    @Override
    public void forceUpdate() {
        synchronized (_requestWaitAndPingObject) {
            _forceUpdateRequestCount++;
            _requestWaitAndPingObject.notifyAll();
        }
    }

    private static class ActiveMqBrokerStatsEventImpl implements ActiveMqBrokerStatsEvent {
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
                Queue requestQueuesQueue_individualDlq = null;
                Queue requestQueuesQueue_globalDlq = null;

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
                    // Note: Using undocumented feature to request a "null message" after last reply-message for query.
                    requestQueuesQueue_globalDlq = session.createQueue(QUERY_REQUEST_DESTINATION_PREFIX + "."
                            + Statics.ACTIVE_MQ_GLOBAL_DLQ_NAME + QUERY_REQUEST_DENOTE_END_LIST);
                }
                else {
                    String queryRequestDestination_all = QUERY_REQUEST_DESTINATION_PREFIX + ".>";

                    log.info("The matsDestinationPrefix was set to [" + _matsDestinationPrefix + "], but this isn't"
                            + " a proper \"path-style\" prefix (it is not ending with a dot), thus cannot restrict"
                            + " the query to mats-specific destinations, but must query for all destinations by"
                            + " employing [" + queryRequestDestination_all + "]");
                    requestQueuesQueue_main = session.createQueue(queryRequestDestination_all);
                    // Note: Using undocumented feature to request a "null message" after last reply-message for query.
                    requestTopicsTopic_main = session.createTopic(queryRequestDestination_all
                            + QUERY_REQUEST_DENOTE_END_LIST);
                }

                Topic requestBrokerTopic = session.createTopic(QUERY_REQUEST_BROKER);

                Topic responseBrokerTopic = session.createTopic(QUERY_REPLY_BROKER_TOPIC);
                Topic responseDestinationsTopic = session.createTopic(QUERY_REPLY_DESTINATION_TOPIC);

                // Chill a small tad before sending first request, so that receivers hopefully have started.
                // Notice: It isn't particularly bad if they haven't, they'll just miss the first request/reply.
                chill(CHILL_MILLIS_BEFORE_FIRST_STATS_REQUEST);

                while (_runStatus == RunStatus.RUNNING) {

                    // TODO: Do NOT send such request if we've already received a reply (from other node's request)
                    // within short period.

                    if (log.isDebugEnabled()) log.debug("Sending stats queries to ActiveMQ.");
                    // :: Request stats for Broker
                    Message requestBrokerMsg = session.createMessage();
                    requestBrokerMsg.setJMSReplyTo(responseBrokerTopic);
                    producer.send(requestBrokerTopic, requestBrokerMsg);

                    // ::: Destinations
                    // (Notice: Directing replyTo for both Queues and Topics reply to same receiver.)
                    // :: Request stats for Queues
                    Message requestQueuesMsg = session.createMessage();
                    set_includeFirstMessageTimestamp(requestQueuesMsg);
                    requestQueuesMsg.setJMSReplyTo(responseDestinationsTopic);

                    // :: Request stats for Topics
                    Message requestTopicsMsg = session.createMessage();
                    set_includeFirstMessageTimestamp(requestTopicsMsg);
                    requestTopicsMsg.setJMSReplyTo(responseDestinationsTopic);

                    // :: Send destination stats messages
                    _countOfDestinationsReceivedAfterRequest.set(0);
                    _nanosAtStart_RequestQuery = System.nanoTime();
                    producer.send(requestQueuesQueue_main, requestQueuesMsg);
                    producer.send(requestTopicsTopic_main, requestTopicsMsg);

                    // ?: Are we using a proper "path-style" mats destination prefix?
                    if (requestQueuesQueue_individualDlq != null) {
                        // -> Yes, we're using a "path-style" mats destination prefix.
                        // Therefore, we'll also need to ask for the individual DLQs, and for the ActiveMQ Global DLQ.
                        // (Asking for the Global DLQ is just to point things out for users that haven't configured
                        // individual DLQ policy. We won't try to fix this problem fully, i.e. won't "distribute"
                        // these DLQs to the individual mats stages they really belong to, as it is pretty much
                        // infeasible if there are more than very few, and that the proper fix is very simple: Use a
                        // frikkin' individual DLQ policy.)
                        Message requestIndividualDlqMsg = session.createMessage();
                        set_includeFirstMessageTimestamp(requestIndividualDlqMsg);
                        requestIndividualDlqMsg.setJMSReplyTo(responseDestinationsTopic);

                        Message requestGlobalDlqMsg = session.createMessage();
                        set_includeFirstMessageTimestamp(requestGlobalDlqMsg);
                        requestGlobalDlqMsg.setJMSReplyTo(responseDestinationsTopic);

                        producer.send(requestQueuesQueue_individualDlq, requestIndividualDlqMsg);
                        producer.send(requestQueuesQueue_globalDlq, requestGlobalDlqMsg);
                    }

                    // :: Chill and loop
                    synchronized (_requestWaitAndPingObject) {
                        // ?: Exiting?
                        if (_runStatus != RunStatus.RUNNING) {
                            // -> Yes, exiting, so shortcut out, avoiding going into wait.
                            break;
                        }
                        // ?: Do we have a force update already waiting?
                        if (_forceUpdateRequestCount > 0) {
                            // -> Yes, so decrement and immediately do the new request.
                            _forceUpdateRequestCount--;
                            continue;
                        }
                        // Go into wait - either interval, or force update.
                        _requestWaitAndPingObject.wait(INTERVAL_MILLIS_BETWEEN_STATS_REQUESTS);
                        // NOTE: If this was a "we are going down!"-wakeup, the loop conditional takes care of it.
                        // ?: Was this a force update wakeup?
                        if (_forceUpdateRequestCount > 0) {
                            // -> Yes, so decrement before looping.
                            _forceUpdateRequestCount--;
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
                closeConnectionIgnoreException(sendRequestMessages_Connection);
                chill(CHILL_MILLIS_WAIT_AFTER_THROWABLE_IN_RECEIVE_LOOPS);
            }
        }
        // To exit, we're signalled via interrupt - it is our job to close Connection (it is a local variable)
        closeConnectionIgnoreException(sendRequestMessages_Connection);
        log.info("Got asked to exit, and that we do!");
    }

    private void set_includeFirstMessageTimestamp(Message msg) throws JMSException {
        msg.setBooleanProperty(QUERY_REQUEST_DESTINATION_INCLUDE_HEAD_MESSAGE_BROKER_IN_TIME, true);
    }

    private void receiveBrokerStatsReplyMessages() {
        OUTERLOOP: while (_runStatus == RunStatus.RUNNING) {
            try {
                _receiveBrokerStatsReplyMessages_Connection = _connectionFactory.createConnection();
                _receiveBrokerStatsReplyMessages_Connection.start();
                Session session = _receiveBrokerStatsReplyMessages_Connection.createSession(false,
                        Session.DUPS_OK_ACKNOWLEDGE);
                Topic replyTopic = session.createTopic(QUERY_REPLY_BROKER_TOPIC);
                MessageConsumer consumer = session.createConsumer(replyTopic);

                while (_runStatus == RunStatus.RUNNING) {
                    MapMessage replyMsg = (MapMessage) consumer.receive();
                    // ?: Was this a null-message, most probably denoting that we're exiting?
                    if (replyMsg == null) {
                        // -> Yes, null message received.
                        log.info("Received null message from consumer.receive(), assuming shutdown.");
                        if (_runStatus != RunStatus.RUNNING) {
                            break OUTERLOOP;
                        }
                        throw new UnexpectedNullMessageReceivedException(
                                "Null message received, but runFlag still true?!");
                    }
                    _currentBrokerStatsDto = mapMessageToBrokerStatsDto(replyMsg);
                    log.info("Got BrokerStats: " + _currentBrokerStatsDto);
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
                closeConnectionIgnoreException(_receiveBrokerStatsReplyMessages_Connection);
                chill(CHILL_MILLIS_WAIT_AFTER_THROWABLE_IN_RECEIVE_LOOPS);
            }
        }
        // To exit, we're signalled via the JMS Connection being closed; Our job is just to null it on our way out.
        _receiveBrokerStatsReplyMessages_Connection = null;
        log.info("Got asked to exit, and that we do!");
    }

    @SuppressWarnings("unchecked")
    private void receiveDestinationStatsReplyMessages() {
        OUTERLOOP: while (_runStatus == RunStatus.RUNNING) {
            try {
                _receiveDestinationsStatsReplyMessages_Connection = _connectionFactory.createConnection();
                _receiveDestinationsStatsReplyMessages_Connection.start();
                Session session = _receiveDestinationsStatsReplyMessages_Connection.createSession(false,
                        Session.DUPS_OK_ACKNOWLEDGE);
                Topic replyTopic = session.createTopic(QUERY_REPLY_DESTINATION_TOPIC);
                MessageConsumer consumer = session.createConsumer(replyTopic);

                long nanosAtEnd_lastMessageReceived = 0;
                while (_runStatus == RunStatus.RUNNING) {
                    // Go into timed receive.
                    MapMessage replyMsg = (MapMessage) consumer.receive(
                            TIMEOUT_MILLIS_FOR_LAST_MESSAGE_IN_BATCH_FOR_DESTINATION_STATS);
                    // NOTE: We're using an undocumented feature of ActiveMQ's StatisticsBrokerPlugin whereby if we add
                    // a special marker to the query, the replies will be "empty terminated" by an empty MapMessage.
                    // ?: Did we get an "empty" MapMessage, OR null, BUT runFlag still true?
                    if (((replyMsg == null) || (replyMsg.getString("destinationName") == null))
                            && (_runStatus == RunStatus.RUNNING)) {
                        // -> empty or null, but runStatus==RUNNING, so this was a "terminator" or timeout.
                        // ?: Have we gotten (bunch of) stats messages by now?
                        if (_countOfDestinationsReceivedAfterRequest.get() > 0) {
                            // -> We've gotten 1 or more stats messages.
                            // We've now (hopefully) received a full set of destination stats.
                            long nanosBetweenQueryAndLastMessageOfBatch = nanosAtEnd_lastMessageReceived
                                    - _nanosAtStart_RequestQuery;
                            long nanosBetweenQueryAndNow = System.nanoTime() - _nanosAtStart_RequestQuery;
                            log.info("We've received a batch of [" + _countOfDestinationsReceivedAfterRequest.get()
                                    + "] destination stats messages, time taken between request sent and last message"
                                    + " of reply batch received: [" + nanos3(nanosBetweenQueryAndLastMessageOfBatch)
                                    + "] ms - this is [" + nanos3(nanosBetweenQueryAndNow)
                                    + "] ms since request. Notifying listeners.");

                            // :: Scavenge old statistics no longer getting updates (i.e. not existing anymore).
                            Iterator<DestinationStatsDto> currentStatsIterator = _currentDestinationStatsDtos.values()
                                    .iterator();
                            long longAgo = System.currentTimeMillis() - SCAVENGE_OLD_STATS_SECONDS * 1000;
                            while (currentStatsIterator.hasNext()) {
                                DestinationStatsDto next = currentStatsIterator.next();
                                if (next.statsReceived.toEpochMilli() < longAgo) {
                                    log.info("Removing destination [" + next.destinationName + "]");
                                    currentStatsIterator.remove();
                                }
                            }

                            // :: Notify listeners
                            ActiveMqBrokerStatsEventImpl event = new ActiveMqBrokerStatsEventImpl();
                            for (Consumer<ActiveMqBrokerStatsEvent> listener : _listeners) {
                                listener.accept(event);
                            }
                        }
                        else {
                            // -> No, we haven't got any messages - probably startup
                            log.debug("We haven't gotten any stats messages - wait for first one.");
                        }

                        // ----- So, either startup, or we've finished a batch of messages:
                        // Go into indefinite receive, waiting for next batch of messages triggered by next stats
                        // request.
                        replyMsg = (MapMessage) consumer.receive();
                    }
                    // ?: Was this a null-message, most probably denoting that we're exiting?
                    if (replyMsg == null) {
                        // -> Yes, null message received.
                        log.info("Received null message from consumer.receive(), assuming shutdown.");
                        if (_runStatus != RunStatus.RUNNING) {
                            break OUTERLOOP;
                        }
                        throw new UnexpectedNullMessageReceivedException("Null message received,"
                                + " but runFlag still true?!");
                    }

                    // This was a destination stats message - count it
                    _countOfDestinationsReceivedAfterRequest.incrementAndGet();
                    // .. log time we received it
                    nanosAtEnd_lastMessageReceived = System.nanoTime();

                    DestinationStatsDto dto = mapMessageToDestinationStatsDto(replyMsg);
                    // log.debug("### "+produceTextRepresentationOfMapMessage_old(replyMsg));
                    log.debug("Message DTO: " + dto);
                    _currentDestinationStatsDtos.put(dto.destinationName, dto);
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
                closeConnectionIgnoreException(_receiveDestinationsStatsReplyMessages_Connection);
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
        dto.headMessageBrokerInTime = getTimestampFromMapMessage(mm, "headMessageBrokerInTime");

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

    @SuppressWarnings("unchecked")
    private StringBuilder produceTextRepresentationOfMapMessage(MapMessage replyMsg) throws JMSException {
        StringBuilder buf = new StringBuilder();
        for (Enumeration<String> e = (Enumeration<String>) replyMsg.getMapNames(); e.hasMoreElements();) {
            String name = e.nextElement();
            buf.append(name);
            buf.append(" = ");
            Object object = replyMsg.getObject(name);
            buf.append(object.toString());
            buf.append(" (").append(object.getClass().getSimpleName()).append(") - ");
        }
        return buf;
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
