package io.mats3.matsbrokermonitor.api.aggregator;

import java.util.OptionalLong;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;
import io.mats3.matsbrokermonitor.api.aggregator.MatsFabricAggregatedRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.aggregator.MatsFabricAggregatedRepresentation.MatsEndpointGroupBrokerRepresentation;

/**
 * <b>Not really interesting, check out {@link MatsFabricAggregatedRepresentation} instead!</b>
 * <p/>
 * Common interface for the MatsFabric aggregates: Endpoint (with its Stages), EndpointGroup, and MatsFabric. The three
 * interfaces {@link MatsEndpointBrokerRepresentation}, {@link MatsEndpointGroupBrokerRepresentation} and
 * {@link MatsFabricAggregatedRepresentation} extends this interface.
 * <p/>
 * <i>Note: It should really have been an inner interface of {@link MatsFabricAggregatedRepresentation}, but that leads
 * to cyclic dependencies, so it is in its own file.</i>
 */
public interface MatsFabricAggregates {
    // :: INCOMING

    long getTotalNumberOfQueuedMessages(StageDestinationType StageDestinationType);

    OptionalLong getOldestStageHeadMessageAgeMillis(StageDestinationType StageDestinationType);

    long getMaxStageNumberOfMessages(StageDestinationType StageDestinationType);
}
