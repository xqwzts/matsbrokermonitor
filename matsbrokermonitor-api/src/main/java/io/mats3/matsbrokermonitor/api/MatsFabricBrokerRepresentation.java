package io.mats3.matsbrokermonitor.api;

import java.util.Collection;
import java.util.Optional;
import java.util.SortedMap;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;

/**
 * Consumes the info from {@link MatsBrokerMonitor}, and stacks it up in a Mats-relevant representation.
 *
 * @author Endre Stølsvik 2022-01-07 00:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsFabricBrokerRepresentation {

    /**
     * @param matsDestinations
     *            the output from {@link MatsBrokerMonitor#getMatsDestinations()}.values().
     * @return a Mats-relevant representation.
     */
    static MatsFabricBrokerRepresentation stack(Collection<MatsBrokerDestination> matsDestinations) {
        return _Impl.stack_interal(matsDestinations);
    }

    /**
     * @return the global DLQ, if there is such a thing in the connected broker. There should really not be, as the
     *         broker should be configured to use an "individual DLQ policy" whereby each queue gets its own DLQ.
     */
    Optional<MatsBrokerDestination> getGlobalDlq();

    /**
     * @return Map[String:EndpointGroupName, MatsEndpointGroupBrokerRepresentation]
     */
    SortedMap<String, MatsEndpointGroupBrokerRepresentation> getMatsEndpointGroupBrokerRepresentations();

    /**
     * @return Map[String:EndpointId, MatsEndpointBrokerRepresentation]
     */
    SortedMap<String, MatsEndpointBrokerRepresentation> getMatsEndpointBrokerRepresentations();

    /**
     * Representation of a Mats "EndpointGroup", as defined by the first part of the endpoint name, i.e.
     * <code>"EndpointGroup.[SubServiceName.]methodName"</code>.
     */
    interface MatsEndpointGroupBrokerRepresentation {
        String getEndpointGroup();

        SortedMap<String, MatsEndpointBrokerRepresentation> getMatsEndpointBrokerRepresentations();
    }

    /**
     * Representation of a Mats Endpoint (which contains all stages) as seen from the "Mats Fabric", i.e. as seen from
     * the Broker.
     */
    interface MatsEndpointBrokerRepresentation {
        String getEndpointId();

        SortedMap<Integer, MatsStageBrokerRepresentation> getStages();
    }

    /**
     * Representation of a Mats Stage, including the Endpoint's initial stage ("stage 0"), with possibly its DLQ, as
     * seen from the "Mats Fabric", i.e. as seen from the Broker.
     */
    interface MatsStageBrokerRepresentation {
        /**
         * @return the index of the stage, where 0 is initial (endpoint entry).
         */
        int getStageIndex();

        /**
         * @return the stageId of the stage - where the 0th (initial) is identical to the EndpointId, while the
         *         subsequent have a postfix ".stageX".
         */
        String getStageId();

        /**
         * @return the {@link MatsBrokerDestination} from which the stage is consuming. Because it is technically
         *         possible that the incoming destination isn't present while the DLQ is, it is Optional - but in
         *         practice, this should rarely happen.
         */
        Optional<MatsBrokerDestination> getIncomingDestination();

        /**
         * @return the DLQ Destination for this stage, if it is present.
         */
        Optional<MatsBrokerDestination> getDlqDestination();
    }
}
