package io.mats3.matsbrokermonitor.spi;

import java.time.Instant;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * @author Endre Stølsvik 2021-12-16 23:10 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerInterface {

    void start();

    void stop();

    SortedSet<BrokerDestination> getQueues();

    SortedSet<BrokerDestination> getTopics();


    interface BrokerDestination {
        String matsStageId();

        int getNumberOfMessages();

        Iterator<BrokerMessage> getMessages(int limit);

        boolean isDlq();
    }

    interface BrokerMessage {
        BrokerDestination getBrokerDestination();

        Instant getEnqueueTime();
    }
}
