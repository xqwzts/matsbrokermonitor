package io.mats3.matsbrokermonitor.jms;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

    private final JmsConnectionHolder _jmsConnectionHolder;

    private final String _matsDestinationPrefix;
    private final String _matsTraceKey;

    public static JmsMatsBrokerBrowseAndActions create(ConnectionFactory connectionFactory) {
        return createWithDestinationPrefix(connectionFactory, "mats.", "mats:trace");
    }

    public static JmsMatsBrokerBrowseAndActions createWithDestinationPrefix(ConnectionFactory connectionFactory,
            String matsDestinationPrefix) {
        return createWithDestinationPrefix(connectionFactory, matsDestinationPrefix, "mats:trace");
    }

    public static JmsMatsBrokerBrowseAndActions createWithDestinationPrefix(ConnectionFactory connectionFactory,
            String matsDestinationPrefix, String matsTraceKey) {
        return new JmsMatsBrokerBrowseAndActions(connectionFactory, matsDestinationPrefix, matsTraceKey);
    }

    private JmsMatsBrokerBrowseAndActions(ConnectionFactory connectionFactory, String matsDestinationPrefix,
            String matsTraceKey) {
        _jmsConnectionHolder = new JmsConnectionHolder(connectionFactory);
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
    public Map<String, MatsBrokerMessageMetadata> deleteMessages(String queueId, Collection<String> messageSystemIds) {
        if (queueId == null) {
            throw new NullPointerException("queueId");
        }
        if (messageSystemIds == null) {
            throw new NullPointerException("messageSystemIds");
        }

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = _jmsConnectionHolder.createSession(false);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue queue = session.createQueue(queueId);

            // NOTICE: This is not optimal in any way, but to avoid premature optimizations and more complex code
            // before it is needed, I'll just let it be like this: Single receive and drop (delete), loop.
            // Could have batched harder using the 'messageSelector' with an OR-construct.

            Map<String, MatsBrokerMessageMetadata> deletedMessages = new LinkedHashMap<>(messageSystemIds.size());
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
                        String matsMessageId = message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID);
                        String toStageId = message.getStringProperty(JMS_MSG_PROP_TO);
                        MDC.put(MDC_MATS_STAGE_ID, toStageId);
                        MDC.put(MDC_TRACE_ID, traceId);
                        MDC.put(MDC_MATS_MESSAGE_ID, matsMessageId);
                        log.info("DELETED MESSAGE: Received and dropping message from [" + queue + "]!"
                                + " MsgSysId [" + messageSystemId + "], Message TraceId: [" + traceId + "]" + extraLog
                                + " - session.createConsumer()+receive(): " + ms(nanosTaken_CreateConsumerAndReceive));
                        deletedMessages.put(message.getJMSMessageID(), new MatsBrokerMessageMetadata(messageSystemId,
                                null, matsMessageId, traceId, toStageId));
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
            return deletedMessages;
        }
        catch (JMSException e) {
            // Bad, so ditch connection.
            _jmsConnectionHolder.closeAndNullOutSharedConnection();
            throw new BrokerIOException("Problems talking to broker."
                    + " If you retry the operation, a new attempt at making a JMS Connection will be performed.", e);
        }
        finally {
            JmsConnectionHolder.closeIfNonNullIgnoreException(session);
        }
    }

    @Override
    public Map<String, MatsBrokerMessageMetadata> deleteAllMessages(String queueId, int maxNumberOfMessages)
            throws BrokerIOException {
        // TODO: Implement!
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public Map<String, MatsBrokerMessageMetadata> reissueMessages(String deadLetterQueueId,
            Collection<String> messageSystemIds) {
        if (deadLetterQueueId == null) {
            throw new NullPointerException("deadLetterQueueId");
        }
        if (messageSystemIds == null) {
            throw new NullPointerException("messageSystemIds");
        }

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = _jmsConnectionHolder.createSession(true);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue dlq = session.createQueue(deadLetterQueueId);
            MessageProducer genericProducer = session.createProducer(null);

            // NOTICE: This is not optimal in any way, but to avoid premature optimizations and more complex code
            // before it is needed, I'll just let it be like this: Single receive-then-send (move), commit tx, loop.
            // Could have batched harder, both within transaction, and via the 'messageSelector' using an OR-construct.

            Map<String, MatsBrokerMessageMetadata> reissuedMessageIds = new LinkedHashMap<>(messageSystemIds.size());
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
                        String traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);
                        String matsMessageId = message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID);
                        String toStageId = message.getStringProperty(JMS_MSG_PROP_TO);
                        MDC.put(MDC_MATS_STAGE_ID, toStageId);
                        MDC.put(MDC_TRACE_ID, traceId);
                        MDC.put(MDC_MATS_MESSAGE_ID, matsMessageId);
                        // ?: Do we know which endpoint/queue it originally should go to?
                        if (toStageId != null) {
                            // -> Yes, we know which queue it originally came from!
                            String toQueueName = _matsDestinationPrefix + toStageId;
                            Queue toQueue = session.createQueue(toQueueName);
                            // Send it ..
                            long nanosAtStart_Send = System.nanoTime();
                            genericProducer.send(toQueue, message);
                            long nanosTaken_Send = System.nanoTime() - nanosAtStart_Send;
                            // .. afterwards it has the new JMS Message ID.
                            String reissuedMsgSysId = message.getJMSMessageID();
                            MDC.put(MDC_MATS_REISSUED_MESSAGE_SYSTEM_ID, reissuedMsgSysId);

                            log.info("REISSUE MESSAGE: Received and reissued message from dlq [" + dlq
                                    + "] to [" + toQueue + "]! Original MsgSysId: [" + messageSystemId
                                    + "], New MsgSysId: [" + reissuedMsgSysId + "], Message TraceId: [" + traceId + "]"
                                    + extraLog + " - session.createConsumer()+receive(): "
                                    + ms(nanosTaken_CreateConsumerAndReceive)
                                    + " - producer.send(): " + ms(nanosTaken_Send));

                            reissuedMessageIds.put(messageSystemId, new MatsBrokerMessageMetadata(messageSystemId,
                                    reissuedMsgSysId, matsMessageId, traceId, toStageId));
                        }
                        else {
                            // -> No, we don't know which queue it originally came from!
                            String matsFailedReissueQueueName = _matsDestinationPrefix
                                    + MatsBrokerBrowseAndActions.MATS_QUEUE_ID_FOR_FAILED_REISSUE;
                            Queue matsFailedReissueQueue = session.createQueue(matsFailedReissueQueueName);
                            // Send it ..
                            long nanosAtStart_Send = System.nanoTime();
                            genericProducer.send(matsFailedReissueQueue, message);
                            long nanosTaken_Send = System.nanoTime() - nanosAtStart_Send;
                            // .. afterwards it has the new JMS Message ID.
                            String newMsgSysId = message.getJMSMessageID();
                            log.error("REISSUE FAILED: Found message without JmsMats JMS StringProperty ["
                                    + JMS_MSG_PROP_TO + "], so don't know where it was originally destined. Sending it"
                                    + " to a synthetic Mats \"DLQ\" endpoint [" + matsFailedReissueQueue
                                    + "] to handle it, and get it away from the DLQ. You may inspect it there, and"
                                    + " either delete it, or if an actual Mats message, code up a consumer for the"
                                    + " endpoint. Original MsgSysId: [" + messageSystemId + "], New MsgSysId: ["
                                    + newMsgSysId + "]" + extraLog + " - session.createConsumer()+receive(): "
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
                    MDC.remove(MDC_MATS_REISSUED_MESSAGE_SYSTEM_ID);
                }
                session.commit();
            }
            genericProducer.close();
            return reissuedMessageIds;
        }
        catch (JMSException e) {
            // Bad, so ditch connection.
            _jmsConnectionHolder.closeAndNullOutSharedConnection();
            throw new BrokerIOException("Problems talking to broker."
                    + " If you retry the operation, a new attempt at making a JMS Connection will be performed.", e);
        }
        finally {
            JmsConnectionHolder.closeIfNonNullIgnoreException(session);
        }
    }

    @Override
    public Map<String, MatsBrokerMessageMetadata> reissueAllMessages(String deadLetterQueueId, int maxNumberOfMessages,
            String randomCookie) throws BrokerIOException {
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
            session = _jmsConnectionHolder.createSession(false);
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
            _jmsConnectionHolder.closeAndNullOutSharedConnection();
            throw new BrokerIOException("Problems talking to broker."
                    + " If you retry the operation, a new attempt at making a JMS Connection will be performed.", e);
        }
        // NOTICE: NOT closing Session, as the Iterator needs it till it is closed!
    }

    private static class JmsConnectionHolder {

        private final ConnectionFactory _connectionFactory;

        // GuardedBy(this): the three following
        private Connection _jmsConnection;
        private boolean _connectionBeingCreated;
        private Throwable _creatingException;

        // Wait-object while separate thread creates the Connection (in case multiple threads need it at the same time).
        private final Object _waitWhileConnectionBeingCreatedObject = new Object();

        public JmsConnectionHolder(ConnectionFactory connectionFactory) {
            _connectionFactory = connectionFactory;
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
                // Retry getting Connection. If this fails, let exception percolate out.
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
            if (existing == null) {
                return;
            }
            try {
                existing.close();
            }
            catch (JMSException e) {
                log.warn("Got [" + e.getClass().getSimpleName() + "] when trying to close Connection [" + existing
                        + "], ignoring.");
            }
        }

        private Connection getCachedConnection() {
            synchronized (_waitWhileConnectionBeingCreatedObject) {
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
                                    synchronized (JmsConnectionHolder.this) {
                                        _jmsConnection = connection;
                                        _connectionBeingCreated = false;
                                    }
                                    log.info("JMS Connection created successfully.");
                                }
                                catch (Throwable e) {
                                    synchronized (JmsConnectionHolder.this) {
                                        _creatingException = e;
                                        _connectionBeingCreated = false;
                                    }
                                    log.warn("Couldn't create JMS Connection.", e);
                                }
                                finally {
                                    // Just to entirely sure that it is not possible to leave this thread with the
                                    // system
                                    // in a bad state that we'll never get out of.
                                    synchronized (JmsConnectionHolder.this) {
                                        _connectionBeingCreated = false;
                                    }
                                    // Now wake all waiter (including the thread that fired off this thread!).
                                    synchronized (_waitWhileConnectionBeingCreatedObject) {
                                        _waitWhileConnectionBeingCreatedObject.notifyAll();
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
                } // Exiting sync on 'this'.

                // ----- Now either we fired off a create-new-Connection thread, or someone else has already done it.

                // We already hold the lock for _waitObject, so thead cannot notify us yet.
                try {
                    // Go into wait on resolution: Whether created Exception, or Exception for why it didn't work.
                    _waitWhileConnectionBeingCreatedObject.wait(30_000);
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
                         * gotten problems with the new Connection, nulled it out, and started a new connection
                         * creation. I choose to ignore this, as the resulting failure isn't devastating.
                         */
                        throw new BrokerIOException("Didn't manage to create JMS Connection within timeout.");
                    }
                }
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
    }

    private static class MatsBrokerMessageIterableImpl implements MatsBrokerMessageIterable, Statics {
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
            return new Iterator<>() {
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
