package io.mats3.matsbrokermonitor.jms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.mats3.matsbrokermonitor.api.MatsBrokerBrowseAndActions;

/**
 * @author Endre Stølsvik 2022-01-15 23:04 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsMatsBrokerBrowseAndActions implements MatsBrokerBrowseAndActions, Statics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsBrokerBrowseAndActions.class);

    private final ConnectionFactory _connectionFactory;
    private final String _matsDestinationPrefix;
    private final String _matsTraceKey;

    public static JmsMatsBrokerBrowseAndActions create(ConnectionFactory connectionFactory) {
        return createWithDestinationPrefix(connectionFactory, "mats.", "mats:trace");
    }

    public static JmsMatsBrokerBrowseAndActions createWithDestinationPrefix(ConnectionFactory connectionFactory,
            String matsDestinationPrefix, String matsTraceKey) {
        return new JmsMatsBrokerBrowseAndActions(connectionFactory, matsDestinationPrefix, matsTraceKey);
    }

    public static JmsMatsBrokerBrowseAndActions createWithDestinationPrefix(ConnectionFactory connectionFactory,
            String matsDestinationPrefix) {
        return createWithDestinationPrefix(connectionFactory, matsDestinationPrefix, "mats:trace");
    }

    private JmsMatsBrokerBrowseAndActions(ConnectionFactory connectionFactory, String matsDestinationPrefix,
            String matsTraceKey) {
        _connectionFactory = connectionFactory;
        _matsDestinationPrefix = matsDestinationPrefix;
        _matsTraceKey = matsTraceKey;
    }

    @Override
    public void start() {
        /* nothing to do */
    }

    @Override
    public void close() {
        /* nothing to do */
    }

    @Override
    public List<String> deleteMessages(String queueId, Collection<String> messageSystemIds) {
        if (queueId == null) {
            throw new NullPointerException("queueId");
        }
        if (messageSystemIds == null) {
            throw new NullPointerException("messageSystemIds");
        }

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = createSession(false);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue queue = session.createQueue(queueId);

            // NOTICE: This is not optimal in any way, but to avoid premature optimizations and more complex code
            // before it is needed, I'll just let it be like this: Single receive and drop (delete), loop.
            // Could have batched harder using the 'messageSelector' with an OR-construct.

            ArrayList<String> deletedMessageIds = new ArrayList<>();
            boolean first = true;
            for (String messageSystemId : messageSystemIds) {
                long nanosAtStart_CreateConsumerAndReceive = System.nanoTime();
                MessageConsumer consumer = session.createConsumer(queue, "JMSMessageID = '" + messageSystemId + "'");
                Message message = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
                long nanosTaken_CreateConsumerAndReceive = System.nanoTime() - nanosAtStart_CreateConsumerAndReceive;
                try {
                    MDC.put(MDC_MATS_MESSAGE_SYSTEM_ID, messageSystemId);
                    String extraLog = first ? " - con.getSession(): " + ms(nanosTaken_CreateSession) + " ms" : "";
                    first = false;
                    if (message != null) {
                        String traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);
                        MDC.put(MDC_MATS_STAGE_ID, message.getStringProperty(JMS_MSG_PROP_TO));
                        MDC.put(MDC_TRACE_ID, traceId);
                        MDC.put(MDC_MATS_MESSAGE_ID, message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID));
                        log.info("DELETED MESSAGE: Received and dropping message from [" + queue + "]!"
                                + " MsgSysId [" + messageSystemId + "], Message TraceId: [" + traceId + "]" + extraLog
                                + " - session.createConsumer()+receive(): " + ms(nanosTaken_CreateConsumerAndReceive));
                        deletedMessageIds.add(message.getJMSMessageID());
                    }
                    else {
                        log.info("DELETE NOT DONE: Did NOT receive a message for id [" + messageSystemId
                                + "], not deleted" + extraLog
                                + " - session.createConsumer()+receive(): " + ms(nanosTaken_CreateConsumerAndReceive));
                    }
                    consumer.close();
                }
                finally {
                    MDC.remove(MDC_MATS_MESSAGE_SYSTEM_ID);
                    MDC.remove(MDC_MATS_STAGE_ID);
                    MDC.remove(MDC_TRACE_ID);
                    MDC.remove(MDC_MATS_MESSAGE_ID);
                }
            }
            return deletedMessageIds;
        }
        catch (JMSException e) {
            // Bad, so ditch connection.
            closeAndNullOutSharedConnection();
            throw new BrokerIOException("Problems talking to broker."
                    + " If you retry the operation, a new attempt at making a JMS Connection will be performed.", e);
        }
        finally {
            closeIfNonNullIgnoreException(session);
        }
    }

    @Override
    public Map<String, String> reissueMessages(String deadLetterQueueId, Collection<String> messageSystemIds) {
        if (deadLetterQueueId == null) {
            throw new NullPointerException("deadLetterQueueId");
        }
        if (messageSystemIds == null) {
            throw new NullPointerException("messageSystemIds");
        }

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = createSession(true);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue dlq = session.createQueue(deadLetterQueueId);
            MessageProducer genericProducer = session.createProducer(null);

            // NOTICE: This is not optimal in any way, but to avoid premature optimizations and more complex code
            // before it is needed, I'll just let it be like this: Single receive-then-send (move), commit tx, loop.
            // Could have batched harder, both within transaction, and via the 'messageSelector' using an OR-construct.

            Map<String, String> reissuedMessageIds = new LinkedHashMap<>(messageSystemIds.size());
            boolean first = true;
            for (String messageSystemId : messageSystemIds) {
                long nanosAtStart_CreateConsumerAndReceive = System.nanoTime();
                MessageConsumer consumer = session.createConsumer(dlq, "JMSMessageID = '" + messageSystemId + "'");
                Message message = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
                long nanosTaken_CreateConsumerAndReceive = System.nanoTime() - nanosAtStart_CreateConsumerAndReceive;

                try {
                    String extraLog = first ? " - con.getSession(): " + ms(nanosTaken_CreateSession) + " ms" : "";
                    first = false;
                    MDC.put(MDC_MATS_MESSAGE_SYSTEM_ID, messageSystemId);
                    if (message != null) {
                        String originalTo = message.getStringProperty(JMS_MSG_PROP_TO);
                        String traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);

                        MDC.put(MDC_MATS_STAGE_ID, originalTo);
                        MDC.put(MDC_TRACE_ID, traceId);
                        MDC.put(MDC_MATS_MESSAGE_ID, message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID));
                        // ?: Do we know which endpoint/queue it originally should go to?
                        if (originalTo != null) {
                            // -> Yes, we know which queue it originally came from!
                            String toQueueName = _matsDestinationPrefix + originalTo;
                            Queue toQueue = session.createQueue(toQueueName);
                            // Send it ..
                            long nanosAtStart_Send = System.nanoTime();
                            genericProducer.send(toQueue, message);
                            long nanosTaken_Send = System.nanoTime() - nanosAtStart_Send;
                            // .. afterwards it has the new JMS Message ID.
                            String newMsgSysId = message.getJMSMessageID();
                            log.info("REISSUE MESSAGE: Received and reissued message from dlq [" + dlq
                                    + "] to [" + toQueue + "]! Original MsgSysId: [" + messageSystemId
                                    + "], New MsgSysId: [" + newMsgSysId + "], Message TraceId: [" + traceId + "]"
                                    + extraLog + " - session.createConsumer()+receive(): "
                                    + ms(nanosTaken_CreateConsumerAndReceive)
                                    + " - producer.send(): " + ms(nanosTaken_Send));

                            reissuedMessageIds.put(messageSystemId, newMsgSysId);
                        }
                        else {
                            // -> No, we don't know which queue it originally came from!
                            String matsDeadLetterQueue = _matsDestinationPrefix + MATS_DEAD_LETTER_ENDPOINT_ID;
                            Queue matsDlq = session.createQueue(matsDeadLetterQueue);
                            // Send it ..
                            long nanosAtStart_Send = System.nanoTime();
                            genericProducer.send(matsDlq, message);
                            long nanosTaken_Send = System.nanoTime() - nanosAtStart_Send;
                            // .. afterwards it has the new JMS Message ID.
                            String newMsgSysId = message.getJMSMessageID();
                            log.error("REISSUE FAILED: Found message without JmsMats JMS StringProperty ["
                                    + JMS_MSG_PROP_TO + "], so don't know where it was originally destined. Sending it"
                                    + " to a synthetic Mats \"DLQ\" endpoint [" + matsDlq + "] to handle it,"
                                    + " and get it away from the DLQ. You may inspect it there, and either delete it,"
                                    + " or if an actual Mats message, code up a consumer for the endpoint."
                                    + " Original MsgSysId: [" + messageSystemId + "], New MsgSysId: [" + newMsgSysId
                                    + "]" + extraLog + " - session.createConsumer()+receive(): "
                                    + ms(nanosTaken_CreateConsumerAndReceive)
                                    + " - producer.send(): " + ms(nanosTaken_Send));
                        }
                    }
                    else {
                        log.info("REISSUE NOT DONE: Did NOT receive a message for id [" + messageSystemId
                                + "], not reissued!" + extraLog + " - session.createConsumer()+receive(): "
                                + ms(nanosTaken_CreateConsumerAndReceive));
                    }
                    consumer.close();
                }
                finally {
                    MDC.remove(MDC_MATS_MESSAGE_SYSTEM_ID);
                    MDC.remove(MDC_MATS_STAGE_ID);
                    MDC.remove(MDC_TRACE_ID);
                    MDC.remove(MDC_MATS_MESSAGE_ID);
                }
                session.commit();
            }
            genericProducer.close();
            return reissuedMessageIds;
        }
        catch (JMSException e) {
            // Bad, so ditch connection.
            closeAndNullOutSharedConnection();
            throw new BrokerIOException("Problems talking to broker."
                    + " If you retry the operation, a new attempt at making a JMS Connection will be performed.", e);
        }
        finally {
            closeIfNonNullIgnoreException(session);
        }
    }

    @Override
    public int deleteAllMessages(String destinationId) {
        // TODO: Implement!
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public int reissueAllMessages(String deadLetterQueueId) {
        // TODO: Implement!
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public MatsBrokerMessageIterable browseQueue(String queueId)
            throws BrokerIOException {
        return browse_internal(queueId, null);
    }

    @Override
    public Optional<MatsBrokerMessageRepresentation> examineMessage(String queueId, String messageSystemId)
            throws BrokerIOException {
        if (messageSystemId == null) {
            throw new NullPointerException("messageSystemId");
        }
        try (MatsBrokerMessageIterableImpl iterable = browse_internal(queueId,
                "JMSMessageID = '" + messageSystemId + "'")) {
            Iterator<MatsBrokerMessageRepresentation> iter = iterable.iterator();
            if (!iter.hasNext()) {
                return Optional.empty();
            }
            return Optional.of(iter.next());
        }
    }

    private MatsBrokerMessageIterableImpl browse_internal(String queueId, String jmsMessageSelector) {
        if (queueId == null) {
            throw new NullPointerException("queueId");
        }
        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = createSession(false);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue queue = session.createQueue(queueId);
            long nanosAtStart_CreateBrowser = System.nanoTime();
            QueueBrowser browser = session.createBrowser(queue, jmsMessageSelector);
            @SuppressWarnings("unchecked")
            Enumeration<Message> messageEnumeration = (Enumeration<Message>) browser.getEnumeration();
            long nanosTaken_CreateBrowser = System.nanoTime() - nanosAtStart_CreateBrowser;

            log.info("BROWSE [" + queueId + "] for [" + (jmsMessageSelector == null ? "all messages in queue"
                    : jmsMessageSelector) + "] - con.getSession(): " + ms(nanosTaken_CreateSession)
                    + " ms - session.createBrowser()+getEnumeration(): " + ms(nanosTaken_CreateBrowser) + " ms.");

            // The Iterator will close the connection upon close.
            return new MatsBrokerMessageIterableImpl(session, _matsTraceKey, messageEnumeration);
        }
        catch (JMSException e) {
            // Bad, so ditch connection.
            closeAndNullOutSharedConnection();
            throw new BrokerIOException("Problems talking to broker."
                    + " If you retry the operation, a new attempt at making a JMS Connection will be performed.", e);
        }
        // NOTICE: NOT closing Session, as the Iterator needs it till it is closed!
    }

    private Session createSession(boolean transacted) throws JMSException {
        try {
            return transacted
                    ? getCachedConnection().createSession(true, Session.SESSION_TRANSACTED)
                    : getCachedConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        catch (JMSException e) {
            // :: If the createSession() call failed, let's retry once more to get a new Connection.
            closeAndNullOutSharedConnection();
            // Retry getting Connection and then createSession. If this fails, give up.
            return transacted
                    ? getCachedConnection().createSession(true, Session.SESSION_TRANSACTED)
                    : getCachedConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
    }

    private void closeAndNullOutSharedConnection() {
        Connection existing;
        synchronized (this) {
            existing = _jmsConnection;
            // Null out connection, so that we'll get a new one.
            _jmsConnection = null;
        }
        // Close existing if present
        closeIfNonNullIgnoreException(existing);
    }

    // GuardedBy(this): the three following
    private Connection _jmsConnection;
    private boolean _connectionBeingCreated;
    private Throwable _creatingException;

    private final Object _waitObject = new Object();

    private Connection getCachedConnection() {
        synchronized (_waitObject) {
            synchronized (this) {
                // ?: Do we have a shared Connection?
                if (_jmsConnection != null) {
                    // -> Yes, so return it.
                    return _jmsConnection;
                }
                else {
                    // -> No connection, so someone needs to make it.
                    // ?: Are someone else already creating a new Connection?
                    if (!_connectionBeingCreated) {
                        // -> No, we're not already creating a new Connection, so go ahead and do it
                        log.info("No connection available, no one else is creating it, so we need to make it -"
                                + " firing off a create-Connection-Thread.");
                        // We are now in the process of attempting to create a new Connection
                        _connectionBeingCreated = true;
                        // There is currently no creating exception
                        _creatingException = null;
                        new Thread(() -> {
                            try {
                                log.info("Creating JMS Connection.");
                                Connection connection = _connectionFactory.createConnection();
                                connection.start();
                                synchronized (JmsMatsBrokerBrowseAndActions.this) {
                                    _jmsConnection = connection;
                                    _connectionBeingCreated = false;
                                }
                                log.info("JMS Connection created successfully.");
                            }
                            catch (Throwable e) {
                                synchronized (JmsMatsBrokerBrowseAndActions.this) {
                                    _creatingException = e;
                                    _connectionBeingCreated = false;
                                }
                                log.warn("Couldn't create JMS Connection.", e);
                            }
                            finally {
                                // Just to entirely sure that it is not possible to leave this thread with the system
                                // in a bad state that we'll never get out of.
                                synchronized (JmsMatsBrokerBrowseAndActions.this) {
                                    _connectionBeingCreated = false;
                                }
                                // Now wake all waiter (including the thread that fired off this thread!).
                                synchronized (_waitObject) {
                                    _waitObject.notifyAll();
                                }
                            }
                        }, "JmsMatsBrokerBrowseAndActions: Creating JMS Connection").start();
                    }
                    else {
                        // -> Someone else has fired off the create-Connection-Thread.
                        log.info("No connection available, but someone else have fired off a"
                                + " create-Connection-Thread.");
                    }
                }
            }

            // ----- Now either we fired off a create-new-Connection thread, or someone else has already done it.

            // We already hold the lock for _waitObject, so thead cannot notify us yet.
            try {
                // Go into wait on resolution: Wither created Exception, or Exception for why it didn't work.
                _waitObject.wait(10_000);
            }
            catch (InterruptedException e) {
                throw new BrokerIOException("We got interrupted while waiting for connection to be made.", e);
            }

            synchronized (this) {
                // ?: Did the create-new-Connection thread get a Connection?
                if (_jmsConnection != null) {
                    // -> Yes, connection now present, so return it.
                    return _jmsConnection;
                }
                // ?: Did the create-new-Connection thread get an Exception?
                else if (_creatingException != null) {
                    // -> Yes, so throw an Exception out
                    throw new BrokerIOException("Couldn't create JMS Connection.", _creatingException);
                }
                else {
                    // -> Neither Connection nor Exception - probably timeout.
                    /*
                     * NOTE: There is a tiny chance of a state-race here, in that another thread might already have
                     * gotten problems with the new Connection, nulled it out, and started a new connection creation. I
                     * choose to ignore this, as the resulting failure isn't devastating.
                     */
                    throw new BrokerIOException("Didn't manage to create JMS Connection within timeout.");
                }
            }
        }
    }

    private static BrokerIOException closeExceptionally(Connection connection, JMSException e) {
        JMSException suppressed = null;
        if (connection != null) {
            try {
                connection.close();
            }
            catch (JMSException ex) {
                suppressed = ex;
            }
        }
        BrokerIOException brokerIOException = new BrokerIOException("Problems talking with broker.", e);
        if (suppressed != null) {
            brokerIOException.addSuppressed(suppressed);
        }
        return brokerIOException;
    }

    private static void closeIfNonNullIgnoreException(Connection connection) {
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

    private static void closeIfNonNullIgnoreException(Session session) {
        if (session == null) {
            return;
        }
        try {
            session.close();
        }
        catch (JMSException e) {
            log.warn("Got [" + e.getClass().getSimpleName() + "] when trying to close Session [" + session
                    + "], ignoring.");
        }
    }

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
    private static double ms(long nanosTaken) {
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

    private static class MatsBrokerMessageIterableImpl implements MatsBrokerMessageIterable {
        private static final Logger log = LoggerFactory.getLogger(MatsBrokerMessageIterableImpl.class);
        private final Session _session;
        private final String _matsTraceKey;
        private final Enumeration<Message> _messageEnumeration;
        private final long _nanosAtStart_Iterate = System.nanoTime();

        public MatsBrokerMessageIterableImpl(Session session, String matsTraceKey,
                Enumeration<Message> messageEnumeration) {
            _session = session;
            _matsTraceKey = matsTraceKey;
            _messageEnumeration = messageEnumeration;
        }

        @Override
        public void close() {
            try {
                _session.close();
            }
            catch (JMSException e) {
                log.warn("Couldn't close JMS Connection after browsing. Ignoring.", e);
            }
            long nanosTaken_Iterate = System.nanoTime() - _nanosAtStart_Iterate;
            log.info(".. BROWSE-iterator.close(), iteration took " + ms(nanosTaken_Iterate) + " ms.");
        }

        @Override
        public Iterator<MatsBrokerMessageRepresentation> iterator() {
            return new Iterator<MatsBrokerMessageRepresentation>() {
                @Override
                public boolean hasNext() {
                    return _messageEnumeration.hasMoreElements();
                }

                @Override
                public MatsBrokerMessageRepresentation next() {
                    return JmsMatsBrokerMessageRepresentationImpl.create(_messageEnumeration.nextElement(),
                            _matsTraceKey);
                }
            };
        }
    }

    private static class JmsMatsBrokerMessageRepresentationImpl implements MatsBrokerMessageRepresentation {
        private final Message _jmsMessage;

        private final String _messageSystemId;
        private final String _matsMessageId;
        private final long _timestamp;
        private final String _traceId;
        private final String _messageType;
        private final String _fromStageId;
        private final String _initializingApp;
        private final String _initiatorId;
        private final String _toStageId;
        private final boolean _persistent;
        private final boolean _interactive;
        private final long _expirationTimestamp;
        private final byte[] _matsTraceBytes; // nullable
        private final String _matsTraceMeta; // nullable

        private static MatsBrokerMessageRepresentation create(
                Message message, String matsTraceKey) throws BrokerIOException {
            try {
                return new JmsMatsBrokerMessageRepresentationImpl(message, matsTraceKey);
            }
            catch (JMSException e) {
                throw new BrokerIOException("Couldn't fetch data from JMS Message", e);
            }

        }

        private JmsMatsBrokerMessageRepresentationImpl(Message message, String matsTraceKey) throws JMSException {
            _jmsMessage = message;

            _messageSystemId = message.getJMSMessageID();
            _matsMessageId = message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID);
            _timestamp = message.getJMSTimestamp();
            _traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);
            _messageType = message.getStringProperty(JMS_MSG_PROP_MESSAGE_TYPE);
            _fromStageId = message.getStringProperty(JMS_MSG_PROP_FROM);
            _initializingApp = message.getStringProperty(JMS_MSG_PROP_INITIALIZING_APP);
            _initiatorId = message.getStringProperty(JMS_MSG_PROP_INITIATOR_ID);
            // Relevant for Global DLQ, where the original id is now effectively lost
            _toStageId = message.getStringProperty(JMS_MSG_PROP_TO);
            _persistent = message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT;
            _interactive = message.getJMSPriority() > 4; // Mats-JMS uses 9 for "interactive"
            _expirationTimestamp = message.getJMSExpiration();

            // Handle MatsTrace
            byte[] matsTraceBytes = null;
            String matsTraceMeta = null;
            if (message instanceof MapMessage) {
                MapMessage mm = (MapMessage) message;
                matsTraceBytes = mm.getBytes(matsTraceKey);
                matsTraceMeta = mm.getString(matsTraceKey + ":meta");
            }

            // These are nullable
            _matsTraceBytes = matsTraceBytes;
            _matsTraceMeta = matsTraceMeta;
        }

        @Override
        public String getMessageSystemId() {
            return _messageSystemId;
        }

        @Override
        public String getMatsMessageId() {
            return _matsMessageId;
        }

        @Override
        public long getTimestamp() {
            return _timestamp;
        }

        @Override
        public String getTraceId() {
            return _traceId;
        }

        @Override
        public String getMessageType() {
            return _messageType;
        }

        @Override
        public String getFromStageId() {
            return _fromStageId;
        }

        @Override
        public String getInitializingApp() {
            return _initializingApp;
        }

        @Override
        public String getInitiatorId() {
            return _initiatorId;
        }

        @Override
        public String getToStageId() {
            return _toStageId;
        }

        @Override
        public boolean isPersistent() {
            return _persistent;
        }

        @Override
        public boolean isInteractive() {
            return _interactive;
        }

        @Override
        public long getExpirationTimestamp() {
            return _expirationTimestamp;
        }

        @Override
        public Optional<byte[]> getMatsTraceBytes() {
            return Optional.ofNullable(_matsTraceBytes);
        }

        @Override
        public Optional<String> getMatsTraceMeta() {
            return Optional.ofNullable(_matsTraceMeta);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{JMS_Message = " + _jmsMessage + "}";
        }
    }
}
