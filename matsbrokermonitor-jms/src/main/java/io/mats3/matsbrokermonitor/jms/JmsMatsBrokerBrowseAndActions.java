package io.mats3.matsbrokermonitor.jms;

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
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
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;

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
    public Map<String, MatsBrokerMessageMetadata> deleteAllMessages(String queueId, int limitMessages)
            throws BrokerIOException {
        if (queueId == null) {
            throw new NullPointerException("queueId");
        }
        if (limitMessages <= 0) {
            throw new IllegalArgumentException("maxNumberOfMessages must be positive! [" + limitMessages + "]");
        }

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = _jmsConnectionHolder.createSession(false);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue queue = session.createQueue(queueId);

            long nanosAtStart_CreateConsumerAndReceive = System.nanoTime();
            MessageConsumer consumer = session.createConsumer(queue);
            long nanosTaken_CreateConsumerAndReceive = System.nanoTime() - nanosAtStart_CreateConsumerAndReceive;

            log.info("DELETING up to [" + limitMessages + "] messages from [" + queue + "]"
                    + " - con.getSession(): " + ms(nanosTaken_CreateSession) + " ms"
                    + " - session.createConsumer(): " + ms(nanosTaken_CreateConsumerAndReceive) + " ms");

            Map<String, MatsBrokerMessageMetadata> deletedMessages = new LinkedHashMap<>(limitMessages);
            int deletedMessagesCount = 0;
            while (true) {
                Message message = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
                try {
                    if (message == null) {
                        log.info("DELETE STOPPED - no more messages after [" + deletedMessagesCount + "] messages, "
                                + " requested [" + limitMessages + "] messages to be deleted.");
                        break;
                    }
                    String messageSystemId = message.getJMSMessageID();
                    String traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);
                    String matsMessageId = message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID);
                    String toStageId = message.getStringProperty(JMS_MSG_PROP_TO);
                    MDC.put(MDC_MATS_MESSAGE_SYSTEM_ID, messageSystemId);
                    MDC.put(MDC_MATS_STAGE_ID, toStageId);
                    MDC.put(MDC_TRACE_ID, traceId);
                    MDC.put(MDC_MATS_MESSAGE_ID, matsMessageId);
                    log.info("DELETED MESSAGE: Received and dropping message from [" + queue + "]!"
                            + " MsgSysId [" + messageSystemId + "], Message TraceId: [" + traceId + "]");
                    deletedMessages.put(messageSystemId, new MatsBrokerMessageMetadata(messageSystemId,
                            null, matsMessageId, traceId, toStageId));

                    deletedMessagesCount++;
                    if (deletedMessagesCount >= limitMessages) {
                        log.info("DELETE FINISHED after requested [" + limitMessages + "] messages to be"
                                + " deleted.");
                        break;
                    }
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
    public Map<String, MatsBrokerMessageMetadata> reissueMessages(String deadLetterQueueId,
            Collection<String> messageSystemIds, String reissuingUsername) {
        return retainingOperationOnMessages(RetainingMessageOperation.REISSUE, deadLetterQueueId, messageSystemIds,
                reissuingUsername, null, true);
    }

    @Override
    public Map<String, MatsBrokerMessageMetadata> reissueAllMessages(String deadLetterQueueId, int limitMessages,
            String username) throws BrokerIOException {
        return retainingOperationOnAllMessages(RetainingMessageOperation.REISSUE, deadLetterQueueId, limitMessages,
                username, null, true);
    }

    @Override
    public Map<String, MatsBrokerMessageMetadata> muteMessages(String deadLetterQueueId,
            Collection<String> messageSystemIds, String mutingUsername, String muteComment) throws BrokerIOException {
        return retainingOperationOnMessages(RetainingMessageOperation.MUTE, deadLetterQueueId, messageSystemIds,
                mutingUsername, muteComment, false);
    }

    @Override
    public Map<String, MatsBrokerMessageMetadata> muteAllMessages(String deadLetterQueueId, int limitMessages,
            String username, String muteComment) throws BrokerIOException {
        return retainingOperationOnAllMessages(RetainingMessageOperation.MUTE, deadLetterQueueId, limitMessages,
                username, muteComment, false);
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

    // ----- Private implementations

    private enum RetainingMessageOperation {
        REISSUE("reissue", "reissuing", "reissued"), MUTE("mute", "muting", "muted");

        private final String _infinitive;
        private final String _present;
        private final String _past;

        RetainingMessageOperation(String infinitive, String present, String past) {
            _infinitive = infinitive;
            _present = present;
            _past = past;
        }
    }

    private Map<String, MatsBrokerMessageMetadata> retainingOperationOnMessages(RetainingMessageOperation operation,
            String deadLetterQueueId, Collection<String> messageSystemIds, String username, String comment,
            boolean deleteDlqProps) {
        if (deadLetterQueueId == null) {
            throw new NullPointerException("deadLetterQueueId");
        }
        if (messageSystemIds == null) {
            throw new NullPointerException("messageSystemIds");
        }

        long nanosAtStart_Total = System.nanoTime();

        String randomCookie = random();

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = _jmsConnectionHolder.createSession(true);
            Queue dlq = session.createQueue(deadLetterQueueId);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            long nanosAtStart_CreateConsumer = System.nanoTime();
            MessageProducer genericProducer = session.createProducer(null);
            long nanosTaken_CreateConsumer = System.nanoTime() - nanosAtStart_CreateConsumer;

            log.info(operation._present.toUpperCase() + " selected [" + messageSystemIds.size() + "] messages from ["
                    + dlq + "] - con.getSession(): " + ms(nanosTaken_CreateSession) + " ms"
                    + " - session.createConsumer(): " + ms(nanosTaken_CreateConsumer) + " ms");

            Map<String, MatsBrokerMessageMetadata> operationMessageIds = new LinkedHashMap<>(messageSystemIds.size());
            int operationMessageCount = 0;
            for (String messageSystemId : messageSystemIds) {
                long nanosAtStart_CreateConsumerAndReceive = System.nanoTime();
                MessageConsumer consumer = session.createConsumer(dlq, "JMSMessageID = '" + messageSystemId + "'");
                Message message = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
                long nanosTaken_CreateConsumerAndReceive = System.nanoTime() - nanosAtStart_CreateConsumerAndReceive;

                if (message != null) {
                    retainingOperationOnSingleMessage(operation, message, randomCookie, username, comment, session,
                            genericProducer, dlq, nanosTaken_CreateConsumerAndReceive, operationMessageIds,
                            deleteDlqProps);
                }
                else {
                    log.info(operation._infinitive.toUpperCase() + " NOT DONE: Did NOT receive a message for id ["
                            + messageSystemId + "], not " + operation._past + "! - message receive: " + ms(
                                    nanosTaken_CreateConsumerAndReceive) + " ms");
                }
                // Close the JMSMessageID-specific consumer.
                consumer.close();

                operationMessageCount++;

                // Commit for each 50th action'ed message
                if ((operationMessageCount % 50) == 0) {
                    session.commit();
                }
            }
            // Final commit
            session.commit();
            genericProducer.close();

            long nanosTaken_Total = System.nanoTime() - nanosAtStart_Total;
            log.info(operation._present.toUpperCase() + " FINISHED after requested [" + messageSystemIds.size()
                    + "] messages, of which [" + operationMessageIds.size() + "] were handled. Total time: "
                    + ms(nanosTaken_Total) + " ms.");

            return operationMessageIds;
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

    public Map<String, MatsBrokerMessageMetadata> retainingOperationOnAllMessages(RetainingMessageOperation operation,
            String deadLetterQueueId, int limitMessages, String username, String comment, boolean deleteDlqProps)
            throws BrokerIOException {
        if (deadLetterQueueId == null) {
            throw new NullPointerException("deadLetterQueueId");
        }
        if (limitMessages <= 0) {
            throw new IllegalArgumentException("limitMessages must be positive! [" + limitMessages + "]");
        }

        String randomCookie = random();

        long nanosAtStart_Total = System.nanoTime();

        Session session = null;
        try {
            long nanosAtStart_CreateSession = System.nanoTime();
            session = _jmsConnectionHolder.createSession(true);
            long nanosTaken_CreateSession = System.nanoTime() - nanosAtStart_CreateSession;

            Queue dlq = session.createQueue(deadLetterQueueId);
            MessageProducer genericProducer = session.createProducer(null);

            long nanosAtStart_CreateConsumer = System.nanoTime();
            MessageConsumer consumer = session.createConsumer(dlq);
            long nanosTaken_CreateConsumer = System.nanoTime() - nanosAtStart_CreateConsumer;

            log.info(operation._present.toUpperCase() + " up to [" + limitMessages + "] messages from [" + dlq + "]"
                    + " - con.getSession(): " + ms(nanosTaken_CreateSession) + " ms"
                    + " - session.createConsumer(): " + ms(nanosTaken_CreateConsumer) + " ms");

            Map<String, MatsBrokerMessageMetadata> reissuedMessageIds = new LinkedHashMap<>(limitMessages);
            int reissuedMessageCount = 0;
            while (true) {
                long nanosAtStart_Receive = System.nanoTime();
                Message message = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
                long nanosTaken_Receive = System.nanoTime() - nanosAtStart_Receive;

                // ?: Did we get a message?
                if (message == null) {
                    // -> No, no message - queue became empty before we got the requested number of messages.
                    long nanosTaken_Total = System.nanoTime() - nanosAtStart_Total;
                    log.info(operation._present.toUpperCase() + " STOPPED - no more messages after ["
                            + reissuedMessageCount + "] messages,  requested [" + limitMessages
                            + "] messages. Total time: " + ms(nanosTaken_Total) + " ms.");
                    break;
                }

                // ?: Have we reissued the message already in this reissue operation?
                if (randomCookie.equals(message.getStringProperty(JMS_MSG_PROP_LAST_OPERATION_COOKIE))) {
                    // -> Yes, we've already been through this message - must be "looping" where reissued messages
                    // again end up on the DLQ.
                    long nanosTaken_Total = System.nanoTime() - nanosAtStart_Total;
                    log.info(operation._present.toUpperCase() + " STOPPED: Message [" + message.getJMSMessageID()
                            + "] has already been reissued by this reissue operation, seems like we are"
                            + " \"looping\". This happened after [" + reissuedMessageCount + "] messages, requested ["
                            + limitMessages + "] messages. Total time: " + ms(nanosTaken_Total) + " ms.");
                    break;
                }

                // ----- We got a message, and it is not already handled in this round of operations.

                // :: Reissue!
                retainingOperationOnSingleMessage(operation, message, randomCookie, username, comment, session,
                        genericProducer, dlq, nanosTaken_Receive, reissuedMessageIds, deleteDlqProps);

                reissuedMessageCount++;
                if (reissuedMessageCount >= limitMessages) {
                    long nanosTaken_Total = System.nanoTime() - nanosAtStart_Total;
                    log.info(operation._present.toUpperCase() + " FINISHED after requested [" + limitMessages
                            + "] messages. Total time: " + ms(nanosTaken_Total) + " ms.");
                    break;
                }
                // Commit for each 50th handled message
                if ((reissuedMessageCount % 50) == 0) {
                    session.commit();
                }
            }
            // Final commit
            session.commit();
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

    private void retainingOperationOnSingleMessage(RetainingMessageOperation operation, Message message,
            String randomCookie, String username, String comment, Session session, MessageProducer genericProducer,
            Queue fromQueue, long nanosTaken_Receive, Map<String, MatsBrokerMessageMetadata> handledMessagesMap,
            boolean deleteDlqProps)
            throws JMSException {
        try {
            String messageSystemId = message.getJMSMessageID();
            String matsMessageId = message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID);
            String traceId = message.getStringProperty(JMS_MSG_PROP_TRACE_ID);
            String toStageId = message.getStringProperty(JMS_MSG_PROP_TO);
            MDC.put(MDC_MATS_MESSAGE_SYSTEM_ID, messageSystemId);
            MDC.put(MDC_MATS_MESSAGE_ID, matsMessageId);
            MDC.put(MDC_TRACE_ID, traceId);
            MDC.put(MDC_MATS_STAGE_ID, toStageId);

            // :: Modify the message to be handled
            // JMS have this strange thing where a received message is has read-only properties.
            // This can be "fixed" by message.clearProperties() - but with the obvious drawback that
            // all properties are cleared. So we need to copy off the existing properties, and put them
            // back on the message after 'clearProperties'.

            // Copy off the existing properties ..
            Map<String, Object> existingProperties = new HashMap<>();
            @SuppressWarnings("unchecked")
            Enumeration<String> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement();
                existingProperties.put(propertyName, message.getObjectProperty(propertyName));
            }
            // .. and then clear the properties (to make them editable)
            message.clearProperties();
            // .. and then put them back on (now editable!)
            for (Map.Entry<String, Object> entry : existingProperties.entrySet()) {
                // .. but - if requested - remove the DLQ-specific properties *except* the DLQ_COUNT, which we want to
                // keep so as to be able to see how many times it has been DLQed.
                if (deleteDlqProps && entry.getKey().startsWith("mats_dlq_")
                        && !entry.getKey().equals(JMS_MSG_PROP_DLQ_DLQ_COUNT)) {
                    continue;
                }
                message.setObjectProperty(entry.getKey(), entry.getValue());
            }

            // Before sending it: Tag the message with the operation cookie and username.
            message.setStringProperty(JMS_MSG_PROP_LAST_OPERATION_COOKIE, randomCookie);
            message.setStringProperty(JMS_MSG_PROP_LAST_OPERATION_USERNAME, username);
            message.setStringProperty(JMS_MSG_PROP_LAST_OPERATION_COMMENT, comment);

            // ?: Do we know which endpoint/queue it originally should go to?
            if (toStageId != null) {
                // -> Yes, we know which queue it originally came from!
                Destination toDestination;

                // :: Handle the different operation
                // s
                // ?: Is it a REISSUE operation?
                if (operation == RetainingMessageOperation.REISSUE) {
                    // Originally on Queue or Topic?
                    String messageType = message.getStringProperty(JMS_MSG_PROP_MESSAGE_TYPE);
                    // TODO: Delete this in 2025 ASAP.
                    if (messageType == null) {
                        log.error("Message lacks JmsMats JMS StringProperty [" + JMS_MSG_PROP_MESSAGE_TYPE
                                + "], so don't know if it was originally destined for a Queue or a Topic."
                                + " Message System ID: [" + messageSystemId + "]");
                        // Default to "" to ensure Queue.
                        messageType = "";
                    }

                    boolean isTopic = "PUBLISH".equals(messageType) || "REPLY_SUBSCRIPTION".equals(messageType);
                    MDC.put(MDC_MATS_DESTINATION_TYPE, isTopic ? "Topic" : "Queue");

                    String toDestinationName = _matsDestinationPrefix + toStageId;
                    toDestination = isTopic
                            ? session.createTopic(toDestinationName)
                            : session.createQueue(toDestinationName);

                }
                // ?: Is it a MUTE operation?
                else if (operation == RetainingMessageOperation.MUTE) {
                    // Mute: Send to the Mats "DLQ" endpoint for Muted messages
                    String toDestinationName = "DLQ."
                            + _matsDestinationPrefix
                            + StageDestinationType.DEAD_LETTER_QUEUE_MUTED.getMidfix()
                            + toStageId;
                    toDestination = session.createQueue(toDestinationName);
                }
                else {
                    throw new AssertionError("Unknown RetainAction: " + operation);
                }

                // Send it ..
                long nanosAtStart_Send = System.nanoTime();
                genericProducer.send(toDestination, message);
                long nanosTaken_Send = System.nanoTime() - nanosAtStart_Send;

                // .. afterwards it has the new JMS Message ID.
                String resultingMsgSysId = message.getJMSMessageID();
                MDC.put(MDC_MATS_RESULTING_MESSAGE_SYSTEM_ID, resultingMsgSysId);

                String actionPastTitlecase = operation._past.substring(0, 1).toUpperCase() + operation._past.substring(
                        1);
                log.info(operation._infinitive.toUpperCase() + " MESSAGE: " + actionPastTitlecase + " message to:["
                        + toDestination + "] from dlq:[" + fromQueue + "]! Original MsgSysId: [" + messageSystemId
                        + "], Resulting MsgSysId: [" + resultingMsgSysId + "], Message TraceId: [" + traceId + "]"
                        + " - message receive: " + ms(nanosTaken_Receive) + " - producer.send(): " + ms(
                                nanosTaken_Send));

                handledMessagesMap.put(messageSystemId, new MatsBrokerMessageMetadata(messageSystemId,
                        resultingMsgSysId, matsMessageId, traceId, toStageId));
            }
            else {
                // -> No, we don't know which queue it originally came from!
                // Send it to a synthetic Mats "DLQ" endpoint for failed operations - where it just has to be deleted
                // after inspection.
                Queue matsFailedReissueQueue = session.createQueue(
                        MatsBrokerBrowseAndActions.QUEUE_ID_FOR_FAILED_OPERATIONS);

                // Add a reason to it.
                message.setStringProperty(JMS_MSG_PROP_OPERATION_FAILED_REASON, "Message lacks '"
                        + JMS_MSG_PROP_TO + "' property on the message,  so don't know where it was originally destined"
                        + " - sending it to '" + QUEUE_ID_FOR_FAILED_OPERATIONS + "' for inspection and deletion!");

                // Send it ..
                long nanosAtStart_Send = System.nanoTime();
                genericProducer.send(matsFailedReissueQueue, message);
                long nanosTaken_Send = System.nanoTime() - nanosAtStart_Send;

                // .. afterwards it has the new JMS Message ID.
                String newMsgSysId = message.getJMSMessageID();
                log.error(operation._infinitive.toUpperCase() + " FAILED: Found message without JmsMats JMS"
                        + " StringProperty [" + JMS_MSG_PROP_TO + "], so don't know where it was originally destined."
                        + " Sending it to a synthetic Mats \"DLQ\" endpoint [" + matsFailedReissueQueue
                        + "] to handle it, and get it away from the DLQ. You may inspect it there."
                        + " Original MsgSysId: [" + messageSystemId + "], New MsgSysId: ["
                        + newMsgSysId + "] - message receive: " + ms(nanosTaken_Receive)
                        + " - producer.send(): " + ms(nanosTaken_Send));
            }
        }
        finally {
            MDC.remove(MDC_MATS_MESSAGE_SYSTEM_ID);
            MDC.remove(MDC_MATS_STAGE_ID);
            MDC.remove(MDC_TRACE_ID);
            MDC.remove(MDC_MATS_MESSAGE_ID);
            MDC.remove(MDC_MATS_DESTINATION_TYPE);
            MDC.remove(MDC_MATS_RESULTING_MESSAGE_SYSTEM_ID);
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
        private final long _timestamp;

        private final String _traceId;
        private final String _matsMessageId;
        private final String _messageType;
        private final String _dispatchType;
        private final String _fromStageId;
        private final String _initiatingApp;
        private final String _initiatorId;
        private final String _toStageId;

        private final boolean _isNoAudit;
        private final boolean _nonPersistent;
        private final boolean _interactive;
        private final long _expirationTimestamp;

        // :: 'Mats Managed DLQ Divert' props
        private final String _dlq_ExceptionStacktrace; // nullable
        private final Boolean _dlq_MessageRefused; // nullable
        private final Integer _dlq_DeliveryCount; // Integer
        private final Integer _dlq_DlqCount; // nullable
        private final String _dlq_StageOrigin; // nullable
        private final String _dlq_AppVersionAndNode; // nullable
        private final String _dlq_LastOperationUsername; // nullable

        // :: MatsTrace
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
            _matsMessageId = message.getStringProperty(JMS_MSG_PROP_MATS_MESSAGE_ID);
            _dispatchType = message.getStringProperty(JMS_MSG_PROP_DISPATCH_TYPE);
            _messageType = message.getStringProperty(JMS_MSG_PROP_MESSAGE_TYPE);
            _fromStageId = message.getStringProperty(JMS_MSG_PROP_FROM);
            _initiatingApp = message.getStringProperty(JMS_MSG_PROP_INITIATING_APP);
            _initiatorId = message.getStringProperty(JMS_MSG_PROP_INITIATOR_ID);
            _toStageId = message.getStringProperty(JMS_MSG_PROP_TO);
            // TODO: Simplify by deleting 'JMS_MSG_PROP_AUDIT' ASAP, latest 2025
            if (message.propertyExists(JMS_MSG_PROP_AUDIT)) {
                _isNoAudit = !message.getBooleanProperty(JMS_MSG_PROP_AUDIT);
            }
            else if (message.propertyExists(JMS_MSG_PROP_NO_AUDIT)) {
                _isNoAudit = message.getBooleanProperty(JMS_MSG_PROP_NO_AUDIT);
            }
            else {
                _isNoAudit = false;
            }
            _nonPersistent = message.propertyExists(JMS_MSG_PROP_NON_PERSISTENT)
                    && message.getBooleanProperty(JMS_MSG_PROP_NON_PERSISTENT);
            _interactive = message.propertyExists(JMS_MSG_PROP_INTERACTIVE)
                    && message.getBooleanProperty(JMS_MSG_PROP_INTERACTIVE);
            _expirationTimestamp = message.getJMSExpiration() != 0
                    ? message.getJMSExpiration()
                    : message.propertyExists(JMS_MSG_PROP_EXPIRES)
                            ? message.getLongProperty(JMS_MSG_PROP_EXPIRES)
                            : 0;

            // :: If DLQed message
            _dlq_ExceptionStacktrace = message.getStringProperty(JMS_MSG_PROP_DLQ_EXCEPTION);
            _dlq_MessageRefused = message.propertyExists(JMS_MSG_PROP_DLQ_REFUSED)
                    ? message.getBooleanProperty(JMS_MSG_PROP_DLQ_REFUSED)
                    : null;
            _dlq_DeliveryCount = message.propertyExists(JMS_MSG_PROP_DLQ_DELIVERY_COUNT)
                    ? message.getIntProperty(JMS_MSG_PROP_DLQ_DELIVERY_COUNT)
                    : null;
            _dlq_DlqCount = message.propertyExists(JMS_MSG_PROP_DLQ_DLQ_COUNT)
                    ? message.getIntProperty(JMS_MSG_PROP_DLQ_DLQ_COUNT)
                    : null;
            _dlq_StageOrigin = message.getStringProperty(JMS_MSG_PROP_DLQ_STAGE_ORIGIN);
            _dlq_AppVersionAndNode = message.getStringProperty(JMS_MSG_PROP_DLQ_APP_VERSION_AND_HOST);
            _dlq_LastOperationUsername = message.getStringProperty(JMS_MSG_PROP_LAST_OPERATION_USERNAME);

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
        public String getDispatchType() {
            return _dispatchType;
        }

        @Override
        public String getFromStageId() {
            return _fromStageId;
        }

        @Override
        public String getInitiatingApp() {
            return _initiatingApp;
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
        public boolean isNoAudit() {
            return _isNoAudit;
        }

        @Override
        public boolean isNonPersistent() {
            return _nonPersistent;
        }

        @Override
        public boolean isInteractive() {
            return _interactive;
        }

        @Override
        public Optional<String> getDlqExceptionStacktrace() {
            return Optional.ofNullable(_dlq_ExceptionStacktrace);
        }

        @Override
        public Optional<Boolean> isDlqMessageRefused() {
            return Optional.ofNullable(_dlq_MessageRefused);
        }

        @Override
        public Optional<Integer> getDlqDeliveryCount() {
            return Optional.ofNullable(_dlq_DeliveryCount);
        }

        @Override
        public Optional<Integer> getDlqCount() {
            return Optional.ofNullable(_dlq_DlqCount);
        }

        @Override
        public Optional<String> getDlqAppVersionAndHost() {
            return Optional.ofNullable(_dlq_AppVersionAndNode);
        }

        @Override
        public Optional<String> getDlqStageOrigin() {
            return Optional.ofNullable(_dlq_StageOrigin);
        }

        @Override
        public Optional<String> getDlqLastOperationUsername() {
            return Optional.ofNullable(_dlq_LastOperationUsername);
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
