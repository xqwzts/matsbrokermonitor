package io.mats3.matsbrokermonitor.spi;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Consumer;

/**
 * @author Endre Stølsvik 2021-12-16 23:10 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerInterface extends Closeable {

    void start();

    void close();

    void registerListener(Consumer<DestinationUpdateEvent> listener);

    interface DestinationUpdateEvent {
        Map<String, MatsBrokerDestination> getNewOrUpdatedDestinations();

        /**
         * @return the set of destinations (queues or topics) that have disappeared - notice that this might happen
         * with existing Mats endpoints if the broker removes the queue or topic e.g. due to inactivity or boot. Such
         * a situation should just be interpreted as that stageId not having any messages in queue.
         */
        Set<String> getRemovedDestinations();
    }

    Map<String, MatsBrokerDestination> getMatsDestinations();

    interface MatsBrokerDestination {
        String matsStageId();

        boolean isQueue();

        boolean isDlq();

        int getNumberOfMessages();
    }
}
