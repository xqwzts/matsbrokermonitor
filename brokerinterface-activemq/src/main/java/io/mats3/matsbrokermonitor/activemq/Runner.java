package io.mats3.matsbrokermonitor.activemq;

import io.mats3.matsbrokermonitor.activemq.RepeatedlyQueryActiveMqForStatistics.ActiveMqBrokerStatsEvent;
import io.mats3.matsbrokermonitor.activemq.RepeatedlyQueryActiveMqForStatistics.BrokerStatsDto;
import io.mats3.matsbrokermonitor.activemq.RepeatedlyQueryActiveMqForStatistics.DestinationStatsDto;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Consumer;

/**
 * @author Endre Stølsvik 2021-12-25 15:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Runner {
    private static Logger log = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) throws InterruptedException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        RepeatedlyQueryActiveMqForStatistics repeatQuery = RepeatedlyQueryActiveMqForStatistics.create(connectionFactory);
        Consumer<ActiveMqBrokerStatsEvent> listener = (event) -> {
            Optional<BrokerStatsDto> brokerStatsDto = repeatQuery.getCurrentBrokerStatsDto();
            log.info("Broker Stats:\n"+brokerStatsDto);

            ConcurrentNavigableMap<String, DestinationStatsDto> destinationStatsDtos = repeatQuery.getCurrentDestinationStatsDtos();
            log.info("Destination Stats:\n"+destinationStatsDtos);
        };
        repeatQuery.registerListener(listener);
        repeatQuery.start();

        Thread.sleep(7 * 1000);
        repeatQuery.stop();
    }
}
