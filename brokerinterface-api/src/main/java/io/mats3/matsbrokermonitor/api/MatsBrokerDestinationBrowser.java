package io.mats3.matsbrokermonitor.api;

import java.util.List;
import java.util.Optional;

/**
 * NOTE!! Has special handling for MatsFactories employing MatsTrace as envelope.
 *
 * @author Endre Stølsvik 2022-01-15 00:08 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerDestinationBrowser {

    /**
     * <b>NOTICE!! It is imperative that the returned iterable is closed!</b>
     *
     * @param destinationType
     *            queue or topic
     * @param destinationId
     *            the name of the destination, with mats prefix.
     * @return a {@link MatsBrokerMessageIterable}, containing either all (unbounded), or a max number of messages (for
     *         ActiveMQ, it is 400) - note that it is absolutely essential that this object is closed after use! You
     *         should have a max number of messages that is read, as it can potentially be many and unbounded (so if
     *         there's a million messages on the destination, you might get them all if you don't max out. Not on
     *         ActiveMQ, though - this broker doesn't give more than 400 even if there are more).
     */
    MatsBrokerMessageIterable browseDestination(DestinationType destinationType, String destinationId);

    List<String> deleteMessages(DestinationType destinationType, String destinationId, List<String> messageSystemIds);

    int purgeDestination(DestinationType destinationType, String destinationId);

    List<String> moveMessages(DestinationType sourceDestinationType, String sourceDestinationId,
            DestinationType targetDestinationType, String targetDestinationId, List<String> messageSystemIds);

    int moveAllMessages(DestinationType sourceDestinationType, String sourceDestinationId,
            DestinationType targetDestinationType, String targetDestinationId);

    interface MatsBrokerMessageIterable extends Iterable<MatsBrokerMessageRepresentation>, AutoCloseable {
        /* no extra methods */
    }

    interface MatsBrokerMessageRepresentation {
        /**
         * @return the broker's id of this message, for JMS it is message.getJMSMessageID().
         */
        String getMessageSystemId();

        String getTraceId();

        String getMessageType();

        String getFromStageId();

        boolean isPersistent();

        boolean isInteractive();

        /**
         * @return the serialized-to-bytes MatsTrace, if present.
         */
        Optional<byte[]> getMatsTraceBytes();

        /**
         * @return If the {@link #getMatsTraceBytes()} is present, this returns the "meta" information of it, needed to
         *         perform deserialization.
         */
        Optional<String> getMeta();
    }

}
