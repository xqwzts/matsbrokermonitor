package io.mats3.matsbrokermonitor.api;

import java.util.OptionalLong;

import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointGroupBrokerRepresentation;

/**
 * Aggregate numbers, common for {@link MatsFabricBrokerRepresentation}, {@link MatsEndpointBrokerRepresentation} and
 * {@link MatsEndpointGroupBrokerRepresentation}.
 * <p/>
 * Note: This interface makes no sense alone, it should ideally just be an inner class of
 * {@link MatsFabricBrokerRepresentation}, but that can't compile due to "cyclic inheritance".
 *
 * @author Endre Stølsvik 2022-03-23 23:12 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsFabricAggregates {

    // :: INCOMING

    long getTotalNumberOfIncomingMessages();

    OptionalLong getOldestIncomingMessageAgeMillis();

    long getMaxStageNumberOfIncomingMessages();

    // :: DLQ

    long getTotalNumberOfDlqMessages();

    OptionalLong getOldestDlqMessageAgeMillis();

    long getMaxStageNumberOfDlqMessages();
}
