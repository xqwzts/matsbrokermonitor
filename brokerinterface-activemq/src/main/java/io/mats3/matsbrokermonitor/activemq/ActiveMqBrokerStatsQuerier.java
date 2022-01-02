package io.mats3.matsbrokermonitor.activemq;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerierImpl.BrokerStatsDto;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerierImpl.DestinationStatsDto;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Consumer;

/**
 * @author Endre Stølsvik 2021-12-28 02:31 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface ActiveMqBrokerStatsQuerier extends Closeable {
    void start();

    void close();

    void setMatsDestinationPrefix(String matsDestinationPrefix);

    void registerListener(Consumer<ActiveMqBrokerStatsEvent> listener);

    interface ActiveMqBrokerStatsEvent {
    }

    void forceUpdate();

    Optional<BrokerStatsDto> getCurrentBrokerStatsDto();

    ConcurrentNavigableMap<String, DestinationStatsDto> getCurrentDestinationStatsDtos();
}
