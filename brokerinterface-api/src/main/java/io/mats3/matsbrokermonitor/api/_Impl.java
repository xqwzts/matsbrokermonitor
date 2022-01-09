package io.mats3.matsbrokermonitor.api;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsStageBrokerRepresentation;

/**
 * @author Endre Stølsvik 2022-01-09 00:23 - http://stolsvik.com/, endre@stolsvik.com
 */
public final class _Impl {

    private _Impl() {
        /* hide constructor */
    }

    private static final Pattern STAGE_PATTERN = Pattern.compile("(.*)\\.stage(\\d+)");

    static MatsFabricBrokerRepresentation stack_interal(Collection<MatsBrokerDestination> matsBrokerDestinations) {
        MatsBrokerDestination globalDlq = null;
        Map<String, MatsEndpointBrokerRepresentationImpl> result = new HashMap<>();
        for (MatsBrokerDestination matsBrokerDestination : matsBrokerDestinations) {
            // ?: Is this the Global DLQ?
            if (matsBrokerDestination.isGlobalDlq()) {
                // -> Yes, Global DLQ: save, and continue.
                globalDlq = matsBrokerDestination;
                continue;
            }
            // ?: Is there a Mats StageId (also EndpointId)?
            if (!matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> No, no StageId, so ditch this, and continue.
                continue;
            }
            // :: Find which Endpoint this queue/topic relates to,
            String stageId = matsBrokerDestination.getMatsStageId().get();
            String endpointId;
            int stageIndex;
            Matcher stageMatcher = STAGE_PATTERN.matcher(stageId);
            if (stageMatcher.matches()) {
                // -> Stage
                endpointId = stageMatcher.group(1);
                stageIndex = Integer.parseInt(stageMatcher.group(2));
            }
            else {
                // -> Missing "stageXX", so this is the "stage0" for the Endpoint, where StageId == EndpointId
                endpointId = stageId;
                stageIndex = 0;
            }

            // Endpoint: Create the MatsEndpointBrokerRepresentation if not already present.
            MatsEndpointBrokerRepresentationImpl matsEndpointBrokerRepresentation = result
                    .computeIfAbsent(endpointId, MatsEndpointBrokerRepresentationImpl::new);

            // Stage: Create the MatsStageBrokerRepresentation if not already present.
            MatsStageBrokerRepresentationImpl matsStageBrokerRepresentation = matsEndpointBrokerRepresentation._stages
                    .computeIfAbsent(stageIndex, stIdx -> new MatsStageBrokerRepresentationImpl(stIdx, stageId));

            // Add the MatsBrokerDestination to the Stage, either it is the incoming destination or the DLQ.
            if (matsBrokerDestination.isDlq()) {
                matsStageBrokerRepresentation._dlqDestination = matsBrokerDestination;
            }
            else {
                matsStageBrokerRepresentation._incomingDestination = matsBrokerDestination;
            }

        }
        return new MatsFabricBrokerRepresentationImpl(Collections.unmodifiableMap(result), globalDlq);
    }

    static class MatsFabricBrokerRepresentationImpl implements MatsFabricBrokerRepresentation {

        private final Map<String, MatsEndpointBrokerRepresentation> _matsEndpointBrokerRepresentations;
        private final MatsBrokerDestination _globalDlq;

        public MatsFabricBrokerRepresentationImpl(
                Map<String, MatsEndpointBrokerRepresentation> matsEndpointBrokerRepresentations,
                MatsBrokerDestination globalDlq) {
            _matsEndpointBrokerRepresentations = matsEndpointBrokerRepresentations;
            _globalDlq = globalDlq;
        }

        @Override
        public Map<String, MatsEndpointBrokerRepresentation> getMatsEndpointBrokerRepresentations() {
            return _matsEndpointBrokerRepresentations;
        }

        @Override
        public Optional<MatsBrokerDestination> getGlobalDlq() {
            return Optional.ofNullable(_globalDlq);
        }
    }

    static class MatsEndpointBrokerRepresentationImpl implements MatsEndpointBrokerRepresentation {

        private final String _endpointId;
        private final Map<Integer, MatsStageBrokerRepresentationImpl> _stages = new HashMap<>();

        public MatsEndpointBrokerRepresentationImpl(String endpointId) {
            _endpointId = endpointId;
        }

        @Override
        public String getEndpointId() {
            return _endpointId;
        }

        @Override
        public Map<Integer, MatsStageBrokerRepresentation> getStages() {
            return Collections.unmodifiableMap(_stages);
        }
    }

    static class MatsStageBrokerRepresentationImpl implements MatsStageBrokerRepresentation {
        private final int _stageIndex;
        private final String _stageId;

        private MatsBrokerDestination _incomingDestination;
        private MatsBrokerDestination _dlqDestination;

        public MatsStageBrokerRepresentationImpl(int stageIndex, String stageId) {
            _stageIndex = stageIndex;
            _stageId = stageId;
        }

        @Override
        public int getStageIndex() {
            return _stageIndex;
        }

        @Override
        public String getStageId() {
            return _stageId;
        }

        @Override
        public Optional<MatsBrokerDestination> getIncomingDestination() {
            return Optional.ofNullable(_incomingDestination);
        }

        @Override
        public Optional<MatsBrokerDestination> getDlqDestination() {
            return Optional.ofNullable(_dlqDestination);
        }
    }
}
