package io.mats3.matsbrokermonitor.activemq;

import java.time.Clock;
import java.time.Instant;
import java.util.Enumeration;
import java.util.Locale;
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
public class ActiveMqBrokerStatsQuerierImpl implements ActiveMqBrokerStatsQuerier {

    private static final Logger log = LoggerFactory.getLogger(ActiveMqBrokerStatsQuerierImpl.class);

    private static final int CHILL_MILLIS_BEFORE_FIRST_REQUEST = 200;
    private static final int INTERVAL_MILLIS_BETWEEN_STATS_REQUESTS = 5 * 1000;
    private static final int CHILL_MILLIS_WAIT_AFTER_RECEIVE_LOOPS_THROWABLE = 10 * 1000;
    private static final int TIMEOUT_MILLIS_FOR_LAST_MESSAGE_IN_BATCH_FOR_DESTINATIONS = 500;
    private static final int TIMEOUT_MILLIS_GRACEFUL_THREAD_SHUTDOWN = 500;

    private static final String QUERY_REQUEST_BROKER = "ActiveMQ.Statistics.Broker";
    /**
     * Note: This should be postfixed with ".{which destination}", which handles wildcards - so ".>" will return a
     * message for every destination of the same type as which the query was sent on (that is, if the query is sent on a
     * queue, you'll get answers for queues, and sent on topic gives answers for topics).
     */
    private static final String QUERY_REQUEST_DESTINATION_PREFIX = "ActiveMQ.Statistics.Destination";

    private static final String QUERY_RESPONSE_BROKER_TOPIC = "matscontrol.MatsBrokerMonitor.ActiveMQ.Broker";
    private static final String QUERY_RESPONSE_DESTINATION_TOPIC = "matscontrol.MatsBrokerMonitor.ActiveMQ.Destinations";

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

    // RunStatus: NOT_STARTED -> RUNNING -> CLOSED. Not restartable.
    private volatile RunStatus _runStatus = RunStatus.NOT_STARTED;
    private volatile Thread _sendStatsRequestMessages_Thread;
    private volatile Thread _receiveBrokerStatsReplyMessages_Thread;
    private volatile Connection _receiveBrokerStatsReplyMessages_Connection;
    private volatile Thread _receiveDestinationsStatsReplyMessages_Thread;
    private volatile Connection _receiveDestinationsStatsReplyMessages_Connection;

    private final AtomicInteger _countOfDestinationsReceivedAfterRequest = new AtomicInteger();
    private volatile long _timeRequestFiredOff = 0;

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
        // Interrupt the sender - it'll close the Connection on its way out.
        interruptThread(_sendStatsRequestMessages_Thread);
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

    private static class ActiveMqBrokerStatsEventImpl implements ActiveMqBrokerStatsEvent {
    }

    static class CommonStatsDto {
        Instant statsReceived;

        String brokerId;
        String brokerName;

        long size;
        long enqueueCount;
        long dequeueCount;
        long expiredCount;
        long dispatchCount;
        long inflightCount;

        long producerCount;
        long consumerCount;

        double minEnqueueTime;
        double averageEnqueueTime;
        double maxEnqueueTime;

        long memoryUsage;
        long memoryLimit;
        int memoryPercentUsage;

        long averageMessageSize;

        long messagesCached;
    }

    static class BrokerStatsDto extends CommonStatsDto {
        String stompSsl;
        String ssl;
        String stomp;
        String openwire;
        String vm;

        String dataDirectory;

        long storeUsage;
        long storeLimit;
        int storePercentUsage;

        long tempUsage;
        long tempLimit;
        int tempPercentUsage;

        @Override
        public String toString() {
            return "BrokerStatsDto{" +
                    "statsReceivedTimeMillis=" + statsReceived + "\n" +
                    ", brokerId='" + brokerId + '\'' + "\n" +
                    ", brokerName='" + brokerName + '\'' + "\n" +
                    ", size=" + size + "\n" +
                    ", enqueueCount=" + enqueueCount + "\n" +
                    ", dequeueCount=" + dequeueCount + "\n" +
                    ", expiredCount=" + expiredCount + "\n" +
                    ", dispatchCount=" + dispatchCount + "\n" +
                    ", inflightCount=" + inflightCount + "\n" +
                    ", producerCount=" + producerCount + "\n" +
                    ", consumerCount=" + consumerCount + "\n" +
                    ", minEnqueueTime=" + minEnqueueTime + "\n" +
                    ", averageEnqueueTime=" + averageEnqueueTime + "\n" +
                    ", maxEnqueueTime=" + maxEnqueueTime + "\n" +
                    ", memoryUsage=" + memoryUsage + "\n" +
                    ", memoryLimit=" + memoryLimit + "\n" +
                    ", memoryPercentUsage=" + memoryPercentUsage + "\n" +
                    ", averageMessageSize=" + averageMessageSize + "\n" +
                    ", messagesCached=" + messagesCached + "\n" +
                    ", stompSsl='" + stompSsl + '\'' + "\n" +
                    ", ssl='" + ssl + '\'' + "\n" +
                    ", stomp='" + stomp + '\'' + "\n" +
                    ", openwire='" + openwire + '\'' + "\n" +
                    ", vm='" + vm + '\'' + "\n" +
                    ", dataDirectory='" + dataDirectory + '\'' + "\n" +
                    ", storeUsage=" + storeUsage + "\n" +
                    ", storeLimit=" + storeLimit + "\n" +
                    ", storePercentUsage=" + storePercentUsage + "\n" +
                    ", tempUsage=" + tempUsage + "\n" +
                    ", tempLimit=" + tempLimit + "\n" +
                    ", tempPercentUsage=" + tempPercentUsage + "\n" +
                    '}';
        }
    }

    static class DestinationStatsDto extends CommonStatsDto {
        String destinationName;

        @Override
        public String toString() {
            return "DestinationStatsDto{" +
                    "statsReceivedTimeMillis=" + statsReceived + "\n" +
                    ", brokerId='" + brokerId + '\'' + "\n" +
                    ", brokerName='" + brokerName + '\'' + "\n" +
                    ", size=" + size + "\n" +
                    ", enqueueCount=" + enqueueCount + "\n" +
                    ", dequeueCount=" + dequeueCount + "\n" +
                    ", expiredCount=" + expiredCount + "\n" +
                    ", dispatchCount=" + dispatchCount + "\n" +
                    ", inflightCount=" + inflightCount + "\n" +
                    ", producerCount=" + producerCount + "\n" +
                    ", consumerCount=" + consumerCount + "\n" +
                    ", minEnqueueTime=" + minEnqueueTime + "\n" +
                    ", averageEnqueueTime=" + averageEnqueueTime + "\n" +
                    ", maxEnqueueTime=" + maxEnqueueTime + "\n" +
                    ", memoryUsage=" + memoryUsage + "\n" +
                    ", memoryLimit=" + memoryLimit + "\n" +
                    ", memoryPercentUsage=" + memoryPercentUsage + "\n" +
                    ", averageMessageSize=" + averageMessageSize + "\n" +
                    ", messagesCached=" + messagesCached + "\n" +
                    ", destinationName='" + destinationName + '\'' + "\n" +
                    '}';
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
                            + "and [" + Statics.ACTIVE_MQ_GLOBAL_DLQ_NAME + "]");
                    requestQueuesQueue_main = session.createQueue(queryRequestDestination_specific);
                    requestTopicsTopic_main = session.createTopic(queryRequestDestination_specific);
                    requestQueuesQueue_individualDlq = session.createQueue(queryRequestDestination_specific_DLQ);
                    requestQueuesQueue_globalDlq = session.createQueue(Statics.ACTIVE_MQ_GLOBAL_DLQ_NAME);
                }
                else {
                    String queryRequestDestination_all = QUERY_REQUEST_DESTINATION_PREFIX + ".>";

                    log.info("The matsDestinationPrefix was set to [" + _matsDestinationPrefix + "], but this isn't"
                            + " a proper \"path-style\" prefix (it is not ending with a dot), thus cannot restrict"
                            + " the query to mats-specific destinations, but must query for all destinations,#"
                            + " employing [" + queryRequestDestination_all + "]");
                    requestQueuesQueue_main = session.createQueue(queryRequestDestination_all);
                    requestTopicsTopic_main = session.createTopic(queryRequestDestination_all);
                }

                Topic requestBrokerTopic = session.createTopic(QUERY_REQUEST_BROKER);

                Topic responseBrokerTopic = session.createTopic(QUERY_RESPONSE_BROKER_TOPIC);
                Topic responseDestinationsTopic = session.createTopic(QUERY_RESPONSE_DESTINATION_TOPIC);

                // Chill a small tad before sending first request, so that receivers hopefully have started.
                // Notice: It isn't particularly bad if they haven't, they'll just miss the first request/reply.
                chill(CHILL_MILLIS_BEFORE_FIRST_REQUEST);

                while (_runStatus == RunStatus.RUNNING) {
                    // :: Request stats for Broker
                    Message requestBrokerMsg = session.createMessage();
                    requestBrokerMsg.setJMSReplyTo(responseBrokerTopic);
                    producer.send(requestBrokerTopic, requestBrokerMsg);

                    // ::: Destinations
                    // (Notice: Directing replyTo for both Queues and Topics reply to same receiver.)

                    // :: Request stats for Queues
                    Message requestQueuesMsg = session.createMessage();
                    // NOTE: The _producer_ has set both TTL and NON_PERSISTENT.
                    requestQueuesMsg.setJMSReplyTo(responseDestinationsTopic);

                    // :: Request stats for Topics
                    Message requestTopicsMsg = session.createMessage();
                    requestTopicsMsg.setJMSReplyTo(responseDestinationsTopic);

                    // :: Send destination stats messages
                    _countOfDestinationsReceivedAfterRequest.set(0);
                    _timeRequestFiredOff = System.currentTimeMillis();
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
                        requestIndividualDlqMsg.setJMSReplyTo(responseDestinationsTopic);
                        Message requestGlobalDlqMsg = session.createMessage();
                        requestGlobalDlqMsg.setJMSReplyTo(responseDestinationsTopic);

                        producer.send(requestQueuesQueue_individualDlq, requestIndividualDlqMsg);
                        producer.send(requestQueuesQueue_globalDlq, requestGlobalDlqMsg);
                    }

                    // :: Chill and loop
                    chill(INTERVAL_MILLIS_BETWEEN_STATS_REQUESTS);
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
                chill(CHILL_MILLIS_WAIT_AFTER_RECEIVE_LOOPS_THROWABLE);
            }
        }
        // To exit, we're signalled via interrupt - it is our job to close Connection (it is a local variable)
        closeConnectionIgnoreException(sendRequestMessages_Connection);
        log.info("Got asked to exit, and that we do!");
    }

    private void receiveBrokerStatsReplyMessages() {
        OUTERLOOP: while (_runStatus == RunStatus.RUNNING) {
            try {
                _receiveBrokerStatsReplyMessages_Connection = _connectionFactory.createConnection();
                _receiveBrokerStatsReplyMessages_Connection.start();
                Session session = _receiveBrokerStatsReplyMessages_Connection.createSession(false,
                        Session.AUTO_ACKNOWLEDGE);
                Topic replyTopic = session.createTopic(QUERY_RESPONSE_BROKER_TOPIC);
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
                chill(CHILL_MILLIS_WAIT_AFTER_RECEIVE_LOOPS_THROWABLE);
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
                        Session.AUTO_ACKNOWLEDGE);
                Topic replyTopic = session.createTopic(QUERY_RESPONSE_DESTINATION_TOPIC);
                MessageConsumer consumer = session.createConsumer(replyTopic);

                long lastMessageReceived = 0;
                while (_runStatus == RunStatus.RUNNING) {
                    // Go into timed receive.
                    MapMessage replyMsg = (MapMessage) consumer.receive(
                            TIMEOUT_MILLIS_FOR_LAST_MESSAGE_IN_BATCH_FOR_DESTINATIONS);
                    // ?: Did we get a null BUT runFlag still true?
                    if ((replyMsg == null) && (_runStatus == RunStatus.RUNNING)) {
                        // -> null, but runStatus==RUNNING, so this was a timeout.

                        // ?: Have we gotten (bunch of) stats messages by now?
                        if (_countOfDestinationsReceivedAfterRequest.get() > 0) {
                            // -> We've gotten 1 or more stats messages.
                            // We've now (hopefully) received a full set of destination stats.
                            long millisTaken = lastMessageReceived - _timeRequestFiredOff;
                            log.info("We've received a batch of [" + _countOfDestinationsReceivedAfterRequest.get()
                                    + "] destination stats messages, time taken between request sent and last message"
                                    + " of batch received: [" + millisTaken + "] ms. Notifying listeners.");
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
                        throw new UnexpectedNullMessageReceivedException(
                                "Null message received, but runFlag still true?!");
                    }

                    // This was a destination stats message - count it
                    _countOfDestinationsReceivedAfterRequest.incrementAndGet();
                    // .. log time we received it
                    lastMessageReceived = System.currentTimeMillis();

                    DestinationStatsDto dto = mapMessageToDestinationStatsDto(replyMsg);
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
                chill(CHILL_MILLIS_WAIT_AFTER_RECEIVE_LOOPS_THROWABLE);
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

        return dto;
    }

    private void mapMessageToCommonStatsDto(MapMessage mm, CommonStatsDto dto) throws JMSException {
        dto.statsReceived = Instant.now(_clock);

        dto.brokerId = (String) mm.getObject("brokerId");
        dto.brokerName = (String) mm.getObject("brokerName");

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
            Object object = replyMsg.getObject(name);
            buf.append("dto." + name + " = (" + object.getClass().getSimpleName().toLowerCase(Locale.ROOT)
                    + ") mm.getObject(\"" + name + "\");\n");
        }
        return buf;
    }

    @SuppressWarnings("unchecked")
    private StringBuilder produceTextRepresentationOfMapMessage_old(MapMessage replyMsg) throws JMSException {
        StringBuilder buf = new StringBuilder();
        for (Enumeration<String> e = (Enumeration<String>) replyMsg.getMapNames(); e.hasMoreElements();) {
            String name = e.nextElement();
            buf.append("  ").append(name);
            buf.append(" = ");
            Object object = replyMsg.getObject(name);
            buf.append(object.toString());
            buf.append(" (").append(object.getClass().getSimpleName()).append(")\n");
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
