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

    public static JmsMatsBrokerBrowseAndActions create(ConnectionFactory connectionFactory,
            String matsDestinationPrefix, String matsTraceKey) {
        return new JmsMatsBrokerBrowseAndActions(connectionFactory, matsDestinationPrefix, matsTraceKey);
    }

    public static JmsMatsBrokerBrowseAndActions create(ConnectionFactory connectionFactory,
            String matsDestinationPrefix) {
        return create(connectionFactory, matsDestinationPrefix, "mats:trace");
    }

    public static JmsMatsBrokerBrowseAndActions create(ConnectionFactory connectionFactory) {
        return create(connectionFactory, "mats.", "mats:trace");
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

        Connection connection = null;
        try {
            connection = _connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueId);

            ArrayList<String> deletedMessageIds = new ArrayList<>();
            for (String messageSystemId : messageSystemIds) {
                MessageConsumer consumer = session.createConsumer(queue, "JMSMessageID = '" + messageSystemId + "'");
                Message message = consumer.receive(150);
                if (message != null) {
                    log.info("DELETED MESSAGE: Received and dropping message for id [" + messageSystemId
                            + "]! Messages Mats' TraceId:[" + message.getStringProperty(JMS_MSG_PROP_TRACE_ID) + "]");
                    deletedMessageIds.add(message.getJMSMessageID());
                }
                else {
                    log.info("DELETE NOT DONE: Did NOT receive a message for id [" + messageSystemId
                            + "], not deleted.");
                }
                consumer.close();
            }
            session.close();
            connection.close();
            return deletedMessageIds;
        }
        catch (JMSException e) {
            throw closeExceptionally(connection, e);
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

        Connection connection = null;
        try {
            connection = _connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue dlq = session.createQueue(deadLetterQueueId);
            MessageProducer genericProducer = session.createProducer(null);

            // NOTICE: This is not optimal in any way, but to avoid premature optimizations and more complex code
            // before it is needed, I'll just let it be like this: Single move, commit tx.
            // Could have batched harder, both within transaction, and via the 'messageSelector', using an OR-construct.

            Map<String, String> reissuedMessageIds = new LinkedHashMap<>(messageSystemIds.size());
            for (String messageSystemId : messageSystemIds) {
                MessageConsumer consumer = session.createConsumer(dlq, "JMSMessageID = '" + messageSystemId + "'");
                Message message = consumer.receive(150);
                try {
                    MDC.put(MDC_MATS_MESSAGE_SYSTEM_ID, messageSystemId);
                    if (message != null) {
                        String originalTo = message.getStringProperty(JMS_MSG_PROP_TO);
                        String traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);

                        MDC.put(MDC_MATS_STAGE_ID, originalTo);
                        MDC.put(MDC_TRACE_ID, traceId);
                        MDC.put(MDC_MATS_MESSAGE_ID, message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID));
                        if (originalTo != null) {
                            String toQueueName = _matsDestinationPrefix + originalTo;
                            Queue toQueue = session.createQueue(toQueueName);
                            log.info("REISSUE MESSAGE: Received and reissued message from dlq [" + dlq
                                    + "] to :[" + toQueue + "]!");

                            genericProducer.send(toQueue, message);
                            reissuedMessageIds.put(messageSystemId, message.getJMSMessageID());
                        }
                        else {
                            String matsDeadLetterQueue = _matsDestinationPrefix + MATS_DEAD_LETTER_ENDPOINT_ID;
                            Queue matsDlq = session.createQueue(matsDeadLetterQueue);
                            log.error("Found message without JmsMats JMS StringProperty [" + JMS_MSG_PROP_TO
                                    + "], so don't know where it was originally destined. Sending it to a"
                                    + " synthetic Mats \"DLQ\" endpoint [" + matsDlq + "] to handle it,"
                                    + " and get it away from the DLQ. You may inspect it there, and either delete it,"
                                    + " or if an actual Mats message, code up a consumer for the endpoint.");
                            genericProducer.send(matsDlq, message);
                        }
                    }
                    else {
                        log.info("REISSUE NOT DONE: Did NOT receive a message for id [" + messageSystemId
                                + "], not reissued.");
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
            session.close();
            connection.close();
            return reissuedMessageIds;
        }
        catch (JMSException e) {
            throw closeExceptionally(connection, e);
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
        Connection connection = null;
        try {
            connection = _connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueId);
            QueueBrowser browser = session.createBrowser(queue, jmsMessageSelector);
            @SuppressWarnings("unchecked")
            Enumeration<Message> messageEnumeration = (Enumeration<Message>) browser.getEnumeration();
            // The Iterator will close the connection upon close.
            return new MatsBrokerMessageIterableImpl(connection, _matsTraceKey, messageEnumeration);
        }
        catch (JMSException e) {
            throw closeExceptionally(connection, e);
        }
    }

    private BrokerIOException closeExceptionally(Connection connection, JMSException e) {
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

    private static class MatsBrokerMessageIterableImpl implements MatsBrokerMessageIterable {
        private static final Logger log = LoggerFactory.getLogger(MatsBrokerMessageIterableImpl.class);
        private final Connection _connection;
        private final String _matsTraceKey;
        private final Enumeration<Message> _messageEnumeration;

        public MatsBrokerMessageIterableImpl(Connection connection, String matsTraceKey,
                Enumeration<Message> messageEnumeration) {
            _connection = connection;
            _matsTraceKey = matsTraceKey;
            _messageEnumeration = messageEnumeration;
        }

        @Override
        public void close() {
            try {
                _connection.close();
            }
            catch (JMSException e) {
                log.warn("Couldn't close JMS Connection after browsing. Ignoring.", e);
            }
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
