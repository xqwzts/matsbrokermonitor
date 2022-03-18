package io.mats3.matsbrokermonitor.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Endre Stølsvik 2022-01-15 00:08 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerBrowseAndActions extends Closeable {

    void start();

    void close();

    /**
     * <b>NOTICE!! It is imperative that the returned iterable is closed!</b>. Loop through it ASAP (remember a max
     * number), and then close it in a finally-block - prefer <i>try-with-resources</i>.
     *
     * @param queueId
     *            the name of the destination, with mats prefix.
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
     *            where the message lives
     * @param messageSystemId
     *            the broker's id for this message, for JMS it is the message.getJMSMessageID().
     * @return the specified message, if present.
     */
    Optional<MatsBrokerMessageRepresentation> examineMessage(String queueId, String messageSystemId)
            throws BrokerIOException;

    List<String> deleteMessages(String queueId, Collection<String> messageSystemIds) throws BrokerIOException;

    int deleteAllMessages(String queueId) throws BrokerIOException;

    Map<String, String> reissueMessages(String deadLetterQueueId, Collection<String> messageSystemIds)
            throws BrokerIOException;

    int reissueAllMessages(String deadLetterQueueId) throws BrokerIOException;

    interface MatsBrokerMessageIterable extends Iterable<MatsBrokerMessageRepresentation>, AutoCloseable {
        /**
         * Close overridden to not throw.
         */
        void close();
    }

    interface MatsBrokerMessageRepresentation {
        /**
         * @return the broker's id of this message, for JMS it is message.getJMSMessageID().
         */
        String getMessageSystemId();

        long getTimestamp();

        String getTraceId();

        String getMessageType();

        String getFromStageId();

        String getInitializingApp();

        String getInitiatorId();

        /**
         * @return the (original) To-Stage Id - even if this message is now DLQed, even if to a Global DLQ where
         *         otherwise the original queue name is lost.
         */
        String getToStageId();

        boolean isPersistent();

        boolean isInteractive();

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
        public BrokerIOException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
