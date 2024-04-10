package io.mats3.matsbrokermonitor.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;

/**
 * API for browsing queues and performing actions on messages of a Mats Broker. This is the API that the Mats Broker
 * Monitor uses to browse and perform actions on the broker.
 * <p/>
 * Note: This is the "read queues and actions on messages" API, which complement the "monitor the broker" API which is
 * {@link MatsBrokerMonitor}. The reason for this separation of the API is that the pieces defined in this piece can be
 * done with ordinary JMS operations, while the operations in the monitor API are not part of a standard JMS API and
 * must be implemented specifically for each broker.
 *
 * @see MatsBrokerMonitor
 *
 * @author Endre Stølsvik 2022-01-15 00:08 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerBrowseAndActions extends Closeable {

    /**
     * Synthetic DLQ for failed reissues where we could not determine original queue. We have very little recourse when
     * this happens, and we don't want to delete the message either. Letting it lay on the original DLQ is not a good
     * solution either - you want them out of the way. Therefore, we put them on another DLQ instead. However, the only
     * solution MatsBrokerMonitor provide after this move is to delete the message after inspection.
     * <p/>
     * Value is <code>"DLQ.MatsBrokerMonitor.FailedReissues"</code>.
     */
    String QUEUE_ID_FOR_FAILED_REISSUE = "DLQ.MatsBrokerMonitor.FailedReissues";

    void start();

    void close();

    /**
     * <b>NOTICE!! It is imperative that the returned iterable is closed!</b>. Loop through it ASAP (remember a max
     * number), and then close it in a finally-block - prefer <i>try-with-resources</i>.
     * <p/>
     * <b>NOTICE!! You should not hold on to the returned {@link MatsBrokerMessageRepresentation}s coming from the
     * iterable, as the contained message may be large. For example, if you put these into a list, the JVM will not be
     * able to GC the messages until the list is GCed, and you might consume a lot of memory.</b> This means that you
     * should iterate over the messaages and output the resulting information (e.g. HTML) in a stream fashion, and then
     * close the iterable.
     *
     * @param queueId
     *            the full name of the queue, including mats prefix.
     * @return a {@link MatsBrokerMessageIterable}, containing either all (unbounded), or a max number of messages (for
     *         ActiveMQ, it is 400) - note that it is absolutely essential that this object is closed after use! You
     *         should have a max number of messages that is read, as it can potentially be many and unbounded (so if
     *         there's a million messages on the destination, you might get them all if you don't have a max. Not on
     *         ActiveMQ, though - this broker doesn't give more than 400 even if there are more).
     */
    MatsBrokerMessageIterable browseQueue(String queueId) throws BrokerIOException;

    /**
     * Fetches the specified message for introspection, but does not consume it, i.e. "browses" a single message. The
     * requested message might not be present, in which case {@link Optional#empty()} is returned.
     *
     * @param queueId
     *            the full name of the queue, including mats prefix.
     * @param messageSystemId
     *            the broker's id for this message, for JMS it is the message.getJMSMessageID().
     * @return the specified message, if present.
     */
    Optional<MatsBrokerMessageRepresentation> examineMessage(String queueId, String messageSystemId)
            throws BrokerIOException;

    /**
     * Deletes the specified message from the specified queue.
     *
     * @param queueId
     *            the full name of the queue, including mats prefix.
     * @param messageSystemIds
     *            the broker's id for the messages to be deleted, for JMS it is the message.getJMSMessageID().
     * @return a Map of the messageSystemIds of the messages deleted, to an instance of
     *         {@link MatsBrokerMessageMetadata} which contains the metadata of the deleted message.
     * @throws BrokerIOException
     *             if problems talking with the broker.
     */
    Map<String, MatsBrokerMessageMetadata> deleteMessages(String queueId, Collection<String> messageSystemIds)
            throws BrokerIOException;

    /**
     * Deletes all message on the specified queue, up to the specified max number of messages which should be the number
     * of messages currently on the queue.
     *
     * @param queueId
     *            the full name of the queue, including mats prefix.
     * @param limitMessages
     *            the max number of messages to delete - will typically be the number of messages we got from the last
     *            update from the {@link MatsBrokerMonitor} via
     *            {@link MatsBrokerDestination#getNumberOfQueuedMessages()}.
     * @return a Map of the messageSystemIds of the messages deleted, to an instance of
     *         {@link MatsBrokerMessageMetadata} which contains the metadata of the deleted message.
     * @throws BrokerIOException
     *             if problems talking with the broker.
     */
    Map<String, MatsBrokerMessageMetadata> deleteAllMessages(String queueId, int limitMessages)
            throws BrokerIOException;

    /**
     * Reissues the specified message Ids on the specified Dead Letter Queue. Note that there is no check that the
     * queueId is actually a DLQ - it is up to the caller to ensure this. The messages reissued will be put on the same
     * queue as they were originally on - which is gotten from a property on the message which is set by the Mats
     * implementation - if this is missing, the message will be put on a new synthetic DLQ named
     * <code>{@link #QUEUE_ID_FOR_FAILED_REISSUE}"</code>, and the message will be logged.
     *
     * @param deadLetterQueueId
     *            the full name of the queue, including mats prefix.
     * @param messageSystemIds
     *            the broker's id for the messages to be reissued, for JMS it is the message.getJMSMessageID().
     * @param reissuingUsername
     *            the username of the user reissuing the messages, which will be put on the message as a property.
     * @return a Map of the messageSystemIds of the messages reissued, to an instance of
     *         {@link MatsBrokerMessageMetadata} which contains the metadata of the reissued message, including the new
     *         messageSystemId of the reissued message, if available.
     * @throws BrokerIOException
     *             if problems talking with the broker.
     */
    Map<String, MatsBrokerMessageMetadata> reissueMessages(String deadLetterQueueId,
            Collection<String> messageSystemIds, String reissuingUsername) throws BrokerIOException;

    /**
     * Reissues all message on the specified Dead Letter Queue, up to the specified max number of messages which should
     * be the number of messages currently on the queue. Note that there is no check that the queueId is actually a DLQ
     * - it is up to the caller to ensure this. The messages reissued will be put on the same queue as they were
     * originally on - which is gotten from a property on the message which is set by the Mats implementation - if this
     * is missing, the message will be put on a new synthetic DLQ named
     * <code>"{@link #QUEUE_ID_FOR_FAILED_REISSUE}"</code>, and the message will be logged.
     * <p/>
     * The reissuing employs a "cookie" to ensure that if the messages are again DLQed while we are reissuing them, we
     * will not reissue the same messages again: This is a random string which is put on the message when it is
     * reissued, and which is checked when we get the message from the DLQ. If the same cookie is present, we know that
     * we have already reissued this message and we're effectively "looping" (reissued messages are again DLQing), and
     * we stop the reissuing process.
     *
     * @param deadLetterQueueId
     *            the full name of the queue, including mats prefix.
     * @param limitMessages
     *            the max number of messages to reissue - will typically be the number of messages we got from the last
     *            update from the {@link MatsBrokerMonitor} via
     *            {@link MatsBrokerDestination#getNumberOfQueuedMessages()}.
     * @param reissuingUsername
     *            the username of the user reissuing the messages, which will be put on the message as a property.
     * @return a Map of the messageSystemIds of the messages reissued, to an instance of
     *         {@link MatsBrokerMessageMetadata} which contains the metadata of the reissued message, including the new
     *         messageSystemId of the reissued message, if available.
     * @throws BrokerIOException
     *             if problems talking with the broker.
     */
    Map<String, MatsBrokerMessageMetadata> reissueAllMessages(String deadLetterQueueId, int limitMessages,
            String reissuingUsername) throws BrokerIOException;

    interface MatsBrokerMessageIterable extends Iterable<MatsBrokerMessageRepresentation>, AutoCloseable {
        /**
         * Close overridden to not throw.
         */
        void close();
    }

    /**
     * The "metadata" of a message, i.e. the information that is available without deserializing the MatsTrace. This is
     * a concrete class, and its field names are such that it can be used as a DTO for serializing to JSON.
     * <p/>
     * <i>Note: As a user of the API, you should employ this as if an API interface. The class is final, you should not
     * create instance of it, and it may change - probably to get more fields.</i>
     */
    final class MatsBrokerMessageMetadata {
        public String messageSystemId;
        public String reissuedMessageSystemId;
        public String matsMessageId;
        public String traceId;
        public String toStageId;

        public MatsBrokerMessageMetadata(String messageSystemId, String reissuedMessageSystemId, String matsMessageId,
                String traceId, String toStageId) {
            this.messageSystemId = messageSystemId;
            this.reissuedMessageSystemId = reissuedMessageSystemId;
            this.matsMessageId = matsMessageId;
            this.traceId = traceId;
            this.toStageId = toStageId;
        }

        String getMessageSystemId() {
            return messageSystemId;
        }

        /**
         * @return if a reissue, returns the new messageSystemId of the reissued message, if available.
         */
        Optional<String> getReissuedMessageSystemId() {
            return Optional.ofNullable(reissuedMessageSystemId);
        }

        String getMatsMessageId() {
            return matsMessageId;
        }

        String getTraceId() {
            return traceId;
        }

        /**
         * @return the (original) To-Stage Id - even if this message is now DLQed, even if to a Global DLQ where
         *         otherwise the original queue name is lost.
         */
        String getToStageId() {
            return toStageId;
        }
    }

    /**
     * The full message representation, with all of metadata, including the serialized MatsTrace.
     */
    interface MatsBrokerMessageRepresentation {
        /**
         * @return the broker's id of this message, for JMS it is message.getJMSMessageID().
         */
        String getMessageSystemId();

        String getMatsMessageId();

        long getTimestamp();

        String getTraceId();

        String getMessageType();

        String getFromStageId();

        String getInitiatingApp();

        String getInitiatorId();

        /**
         * @return the (original) To-Stage Id - even if this message is now DLQed, even if to a Global DLQ where
         *         otherwise the original queue name is lost.
         */
        String getToStageId();

        boolean isPersistent();

        boolean isInteractive();

        // DLQ

        /**
         * For messages residing on a DLQ: Returns the exception stacktrace that caused the DLQ if available. Depending
         * on how the message was DLQed, this might not be available: E.g. for <i>Mats Managed Dlq Divert</i>, if the
         * message was DLQed on the receive side, the exception stacktrace is not available, while if the message was
         * DLQed while still in the processing side, the exception stacktrace is available. If the message was DLQed by
         * the message broker, the exception stacktrace is not available.
         *
         * @return the exception stacktrace that caused the DLQ if available. Depending on how the message was DLQed,
         *         this might not be available.
         */
        Optional<String> getDlqExceptionStacktrace();

        /**
         * For messages residing on a DLQ: Returns whether the message was DLQed "on purpose", i.e. refused by the
         * consumer by throwing <code>MatsRefuseMessageException</code> (<code>true</code>), or if it was DLQed due to
         * an excessive number of reissues where the processing stage failed (raised some RuntimeException)
         * (<code>false</code>). If the message was DLQed by the message broker, or the DLQing happened on the reception
         * side, this will be <code>empty</code>.
         *
         * @return whether the message was DLQed "on purpose", i.e. refused by the consumer by throwing
         *         <code>MatsRefuseMessageException</code> (<code>true</code>), or <code>empty</code> if not DLQed (by
         *         Mats3).
         */
        Optional<Boolean> isDlqMessageRefused();

        /**
         * For messages residing on a DLQ: For <i>Mats Managed Dlq Divert</i>, this is the number of times the message
         * was attempted delivered to the stage, but failed (raised some Exception). If the message was DLQed by the
         * message broker, this will be <code>empty</code>.
         *
         * @return the number of times the message was attempted delivered to the stage, but failed (raised some
         *         Exception), or <code>empty</code> if not DLQed (by Mats3).
         */
        Optional<Integer> getDlqDeliveryCount();

        /**
         * For messages residing on a DLQ: For <i>Mats Managed Dlq Divert</i>, this is the number of times this message
         * has been DLQed (a message can be DLQed, then manually reissued - and then it DLQs again). If the message was
         * DLQed by the message broker, this will be <code>empty</code> (If this message was previously DLQed by Mats,
         * and then subsequently DLQed by the broker, this count will not reflect the new DLQ - the count will just
         * stick around from the previous DLQ. This should never happen, though).
         *
         * @return the number of times this message has been DLQed, or <code>empty</code> if not DLQed (by Mats3).
         */
        Optional<Integer> getDlqCount();

        /**
         * For messages residing on a DLQ: For <i>Mats Managed Dlq Divert</i>, this is the name of the application,
         * version and hostname, separated by ";" and "@", that DLQed this message. If the message was DLQed by the
         * message broker, this will be <code>empty</code>
         *
         * @return the name of the application, version and host, separated by ";" and "@", that DLQed this message or
         *         <code>empty</code> if not DLQed (by Mats3).
         */
        Optional<String> getDlqAppVersionAndHost();

        /**
         * For messages residing on a DLQ: For <i>Mats Managed Dlq Divert</i>, this is the "Origin" ("debug info") of
         * the Stage that DLQed this message - i.e. <i>where</i> in the Application source code the stage was defined.
         * If the message was DLQed by the message broker, this will be <code>empty</code>.
         * <p/>
         * Note that the StageId that processed the message can be found by {@link #getToStageId()}.
         *
         * @return the "stage origin" of the Mats Stage that processed this message and then DLQed it, if available.
         */
        Optional<String> getDlqStageOrigin();

        /**
         * For messages residing on a DLQ: The username of the user that reissued this message from the DLQ via
         * MatsBrokerMonitor (assuming that the message has already been tried reissued!).
         *
         * @return the username of the user that reissued this message from the DLQ via MatsBrokerMonitor, if available.
         */
        Optional<String> getDlqLastReissuedUsername();

        /**
         * @return the timestamp (millis-from-epoch) when the message will expire, or <code>0</code> if never.
         */
        long getExpirationTimestamp();

        /**
         * @return the serialized-to-bytes MatsTrace, if present.
         */
        Optional<byte[]> getMatsTraceBytes();

        /**
         * @return If the {@link #getMatsTraceBytes()} is present, this returns the "meta" information of it, needed to
         *         perform deserialization.
         */
        Optional<String> getMatsTraceMeta();
    }

    /**
     * Thrown if problems talking with the broker, e.g. for JMS, if <code>JMSException</code> is raised.
     */
    class BrokerIOException extends RuntimeException {
        public BrokerIOException(String message) {
            super(message);
        }

        public BrokerIOException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
