package io.mats3.matsbrokermonitor.activemq;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.ActiveMqBrokerStatsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerierImpl.BrokerStatsDto;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerierImpl.DestinationStatsDto;
import io.mats3.matsbrokermonitor.spi.MatsBrokerInterface;

/**
 * @author Endre Stølsvik 2021-12-27 14:40 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ActiveMqMatsBrokerInterface implements MatsBrokerInterface {
    private static final Logger log = LoggerFactory.getLogger(ActiveMqMatsBrokerInterface.class);

    private final ActiveMqBrokerStatsQuerier _querier;

    static ActiveMqMatsBrokerInterface create(ActiveMqBrokerStatsQuerierImpl querier) {
        return new ActiveMqMatsBrokerInterface(querier);
    }

    ActiveMqMatsBrokerInterface(ActiveMqBrokerStatsQuerier querier) {
        _querier = querier;
        _querier.registerListener(this::eventFromQuerier);
    }

    private final CopyOnWriteArrayList<Consumer<DestinationUpdateEvent>> _listeners = new CopyOnWriteArrayList<>();

    @Override
    public void start() {
        _querier.start();
    }

    @Override
    public void registerListener(Consumer<DestinationUpdateEvent> listener) {
        _listeners.add(listener);
    }

    @Override
    public Map<String, MatsBrokerDestination> getMatsDestinations() {
        return null;
    }

    @Override
    public void close() {
        _querier.close();
    }

    // ===== IMPLEMENTATION

    private void eventFromQuerier(ActiveMqBrokerStatsEvent event) {
        log.info("Got event [" + event + "].");
        Optional<BrokerStatsDto> brokerStatsDto = _querier.getCurrentBrokerStatsDto();
        log.info("Broker Stats:\n" + brokerStatsDto);

        ConcurrentNavigableMap<String, DestinationStatsDto> destinationStatsDtos = _querier
                .getCurrentDestinationStatsDtos();
        log.info("Destination Stats:\n" + destinationStatsDtos);
    }



}
