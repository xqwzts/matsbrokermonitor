package io.mats3.matsbrokermonitor.api;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.impl.MatsFabricArranger;

/**
 * Consumes the info from {@link MatsBrokerMonitor} (all its {@link MatsBrokerDestination MatsBrokerDestination}
 * instances), and stacks it up in a Mats fabric-relevant representation: {@link MatsEndpointGroupBrokerRepresentation
 * Endpoint Groups}, consisting of {@link MatsEndpointBrokerRepresentation Endpoints}, consisting of
 * {@link MatsStageBrokerRepresentation Stages}.
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
        return MatsFabricArranger.stack_interal(matsDestinations);
    }

    /**
     * @return the max of {@link MatsEndpointBrokerRepresentation#getStageOldestHeadMessageAgeMillis()
     *         endpoint.getStageOldestHeadMessageAgeMillis()} for {@link #getEndpoints() all endpoints} of the Mats
     *         fabric, if no Endpoint has a message, {@link OptionalLong#empty()} is returned.
     */
    default OptionalLong getStageOldestHeadMessageAgeMillis() {
        return getEndpoints().values().stream()
                .map(MatsEndpointBrokerRepresentation::getStageOldestHeadMessageAgeMillis)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .max(Comparator.naturalOrder())
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }

    /**
     * @return the max of {@link MatsEndpointBrokerRepresentation#getStageMaxNumberOfDeadLetterMessages()
     *         endpoint.getStageMaxNumberOfDeadLetteredMessages()} for all Endpoints in the Mats fabric, including the
     *         {@link #getDefaultGlobalDlq()} if present. Zero is a good number, any other means that there exists a DLQ
     *         with messages which probably should be looked into.
     */
    default long getQueueMaxNumberOfDeadLetterMessages() {
        Stream<Long> stagesMax = getEndpoints().values().stream()
                .map(MatsEndpointBrokerRepresentation::getStageMaxNumberOfDeadLetterMessages);

        Stream<Long> globalDlqNum = getDefaultGlobalDlq().map(MatsBrokerDestination::getNumberOfQueuedMessages)
                .map(Stream::of).orElseGet(Stream::empty); // Java 8 missing optional.stream()

        return Stream.concat(stagesMax, globalDlqNum)
                .max(Comparator.naturalOrder())
                .orElse(0L);
    }

    /**
     * @return the default global DLQ, if there is such a thing in the connected broker. There should really not be, as
     *         the broker should be configured to use an "individual DLQ policy" whereby each queue gets its own DLQ.
     */
    Optional<MatsBrokerDestination> getDefaultGlobalDlq();

    /**
     * All Endpoints, no grouping.
     *
     * @return Map[String:EndpointId, MatsEndpointBrokerRepresentation]
     */
    Map<String, MatsEndpointBrokerRepresentation> getEndpoints();

    /**
     * All Endpoints, grouped by "Endpoint Group", where a "group" is all Endpoints sharing the first part of the
     * EndpointId.
     *
     * @return Map[String:EndpointGroupName, MatsEndpointGroupBrokerRepresentation]
     */
    Map<String, MatsEndpointGroupBrokerRepresentation> getEndpointGroups();

    /**
     * Representation of a Mats Endpoint (which contains all stages) as seen from the "Mats Fabric", i.e. as seen from
     * the Broker.
     */
    interface MatsEndpointBrokerRepresentation {
        String getEndpointId();

        Map<Integer, MatsStageBrokerRepresentation> getStages();

        /**
         * @return the max of {@link MatsStageBrokerRepresentation#getHeadMessageAgeMillis()
         *         stage.getHeadMessageAgeMillis()} for {@link #getStages() all Stages} of the Endpoint, if no Stages
         *         has a message, {@link OptionalLong#empty()} is returned.
         */
        default OptionalLong getStageOldestHeadMessageAgeMillis() {
            return getStages().values().stream()
                    .map(MatsStageBrokerRepresentation::getHeadMessageAgeMillis)
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        /**
         * @return the max of {@link MatsStageBrokerRepresentation#getNumberOfDeadLetterMessages()
         *         stage.getNumberOfDeadLetterMessages()} for {@link #getStages() all stages} of the Endpoint.
         */
        default long getStageMaxNumberOfDeadLetterMessages() {
            return getStages().values().stream()
                    .map(MatsStageBrokerRepresentation::getNumberOfDeadLetterMessages)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }
    }

    /**
     * Representation of a Mats "EndpointGroup", a collection of Endpoint, grouped by the first part of the endpoint
     * name, i.e. <code>"EndpointGroup.[SubServiceName.]methodName"</code>. This should ideally have a 1:1 relation with
     * the actual (micro) services in the system.
     */
    interface MatsEndpointGroupBrokerRepresentation {
        String getEndpointGroup();

        Map<String, MatsEndpointBrokerRepresentation> getEndpoints();

        /**
         * @return the max of {@link MatsEndpointBrokerRepresentation#getStageOldestHeadMessageAgeMillis()
         *         endpoint.getStageOldestHeadMessageAgeMillis()} for {@link #getEndpoints() all endpoints} of the
         *         EndpointGroup, if no Endpoints has a message, {@link OptionalLong#empty()} is returned.
         */
        default OptionalLong getStageOldestHeadMessageAgeMillis() {
            return getEndpoints().values().stream()
                    .map(MatsEndpointBrokerRepresentation::getStageOldestHeadMessageAgeMillis)
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        /**
         * @return the max of {@link MatsEndpointBrokerRepresentation#getStageMaxNumberOfDeadLetterMessages()
         *         endpoint.getStageMaxNumberOfDeadLetteredMessages()} for {@link #getEndpoints() all Endpoints} of the
         *         EndpointGroup.
         */
        default long getStageMaxNumberOfDeadLetterMessages() {
            return getEndpoints().values().stream()
                    .map(MatsEndpointBrokerRepresentation::getStageMaxNumberOfDeadLetterMessages)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }
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
         * @return {@link MatsBrokerDestination#getHeadMessageAgeMillis()} if {@link #getIncomingDestination()} is
         *         present.
         */
        default OptionalLong getHeadMessageAgeMillis() {
            return getIncomingDestination()
                    .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                    .orElse(OptionalLong.empty());
        }

        /**
         * @return the DLQ Destination for this stage, if it is present.
         */
        Optional<MatsBrokerDestination> getDlqDestination();

        /**
         * @return {@link MatsBrokerDestination#getNumberOfQueuedMessages()} for the DLQ of this stage, or zero if
         *         {@link #getDlqDestination()} is not present.
         */
        default long getNumberOfDeadLetterMessages() {
            return getDlqDestination()
                    .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .orElse(0L);
        }
    }
}
