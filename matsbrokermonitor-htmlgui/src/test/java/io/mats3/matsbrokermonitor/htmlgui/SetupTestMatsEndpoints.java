package io.mats3.matsbrokermonitor.htmlgui;

import java.util.MissingFormatArgumentException;
import java.util.Objects;

import org.junit.Assert;

import io.mats3.MatsEndpoint;
import io.mats3.MatsEndpoint.MatsRefuseMessageException;
import io.mats3.MatsEndpoint.ProcessContext;
import io.mats3.MatsFactory;
import io.mats3.matsbrokermonitor.htmlgui.SetupTestMatsEndpoints.ThisIsARandomInnerClass.ThisIsYetAnotherLevelOfInnerClass.MatsThisIsNotReallyAMatsException;

/**
 * @author Endre Stølsvik 2021-12-31 01:50 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SetupTestMatsEndpoints {

    public static int BASE_CONCURRENCY = 5;

    static void setupMatsTestEndpoints(String servicePrefix, MatsFactory matsFactory) {
        setupMainMultiStagedService(servicePrefix, matsFactory);
        setupMidMultiStagedService(servicePrefix, matsFactory);
        setupLeafService(servicePrefix, matsFactory);

        setupTerminator(servicePrefix, matsFactory);
        setupSubscriptionTerminator(servicePrefix, matsFactory);
    }

    static final String SERVICE_MAIN = ".mainService";
    static final String SERVICE_MID = ".private.midMethod";
    static final String SERVICE_LEAF = ".private.leafMethod";
    static final String TERMINATOR = ".terminator";
    static final String SUBSCRIPTION_TERMINATOR = ".subscriptionTerminator";

    // If present as TraceProperty with Boolean.TRUE, the randomThrow(context) won't throw!
    static final String DONT_THROW = "DONT_THROW";

    // If present as TraceProperty with Boolean.TRUE, the mid-service will throw!
    static final String THROW = "THROW!";

    public static void setupLeafService(String servicePrefix, MatsFactory matsFactory) {
        MatsEndpoint<DataTO, Void> single = matsFactory.single(servicePrefix + SERVICE_LEAF, DataTO.class, DataTO.class,
                (context, dto) -> {

                    randomThrow(context);

                    // Use the 'multiplier' in the request to formulate the reply.. I.e. multiply the number..!
                    return new DataTO(dto.number * dto.multiplier, dto.string + ":FromLeafService");
                });
        single.getEndpointConfig().setConcurrency(BASE_CONCURRENCY * 6);
    }

    public static void setupMidMultiStagedService(String servicePrefix, MatsFactory matsFactory) {
        MatsEndpoint<DataTO, StateTO> ep = matsFactory.staged(servicePrefix + SERVICE_MID, DataTO.class,
                StateTO.class);
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            // Store the multiplier in state, so that we can use it when replying in the next (last) stage.
            sto.number1 = dto.multiplier;
            // Add an important number to state..!
            sto.number2 = Math.PI;
            context.request(servicePrefix + SERVICE_LEAF, new DataTO(dto.number, dto.string + ":LeafCall", 2));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            // Only assert number2, as number1 is differing between calls (it is the multiplier for MidService).
            Assert.assertEquals(Math.PI, sto.number2, 0d);
            // Change the important number in state..!
            sto.number2 = Math.E;

            randomThrow(context);

            context.next(new DataTO(dto.number, dto.string + ":NextCall"));
        });
        ep.lastStage(DataTO.class, (context, sto, dto) -> {
            // Only assert number2, as number1 is differing between calls (it is the multiplier for MidService).
            Assert.assertEquals(Math.E, sto.number2, 0d);

            // Check if we are directed to throw!
            if (context.getTraceProperty(THROW, Boolean.class) == Boolean.TRUE) {
                var e1 = new MissingFormatArgumentException(
                        "Just to have a nested exception! Here's quite some long text blah blah testing 123..!");
                var e2 = new IllegalArgumentException("Just to have a nested exception!", e1);
                var toBeThrown = new MatsThisIsNotReallyAMatsException("Throwing as directed by TraceProperty!"
                        + " THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: "
                        + "THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE: THIS IS A REALLY LONG MESSAGE\n"
                        + "This is a second line of the REALLY LONG MESSAGE!\n"
                        + "\n"
                        + "This is a third line of the REALLY LONG MESSAGE! (after a blank!)"
                        + " {curlies}[brackets]$<lessgreater>\"quotes\"%!;* <- special chars!"
                        + " (there's two newlines after here)\n\n",
                        e2);

                var nested2OnSuppressedL2b = new IllegalArgumentException("Nested 2 on Suppressed L2b\n"
                        + "This is a second line on Nested 2 on Suppressed L2b");
                var nested1OnSuppressedL2b = new IllegalArgumentException("Nested 1 on Suppressed L2b",
                        nested2OnSuppressedL2b);
                var suppressedL2bException = new RuntimeException("Suppressed L2b Exception", nested1OnSuppressedL2b);

                var nested2OnSupppressedL1 = new IllegalArgumentException("Nested 2 on Suppressed L1\n"
                        + "This is a second line on Nested 2 on Suppressed L1\n"
                        + "\n"
                        + "This is a third line on Nested 2 on Suppressed L1 (after a blank line)\n"
                        + " {curlies}[brackets]$<lessgreater>\"quotes\"%!;* <- special chars!"
                        + " (there's two newlines after here)\n\n");
                nested2OnSupppressedL1.addSuppressed(suppressedL2bException);
                var nested1OnSupppressedL1 = new IllegalArgumentException("Nested 1 on Suppressed L1",
                        nested2OnSupppressedL1);
                var suppressedL1Exception = new RuntimeException("Suppressed L1 Exception", nested1OnSupppressedL1);

                var nestedOnSuppressedL2 = new IllegalArgumentException("Nested on Suppressed L2");
                var suppressedOnSuppressedL2 = new RuntimeException("Suppressed L2 on Suppressed",
                        nestedOnSuppressedL2);
                suppressedL1Exception.addSuppressed(suppressedOnSuppressedL2);

                toBeThrown.addSuppressed(suppressedL1Exception);
                throw toBeThrown;
            }

            // Use the 'multiplier' in the request to formulate the reply.. I.e. multiply the number..!
            return new DataTO(dto.number * sto.number1, dto.string + ":FromMidService");
        });

        ep.getEndpointConfig().setConcurrency(BASE_CONCURRENCY * 4);
    }

    static class ThisIsARandomInnerClass {
        static class ThisIsYetAnotherLevelOfInnerClass {
            static class MatsThisIsNotReallyAMatsException extends RuntimeException {
                public MatsThisIsNotReallyAMatsException(String message, Exception nested) {
                    super(message, nested);
                }
            }
        }
    }

    public static void setupMainMultiStagedService(String servicePrefix, MatsFactory matsFactory) {
        MatsEndpoint<DataTO, StateTO> ep = matsFactory.staged(servicePrefix + SERVICE_MAIN, DataTO.class,
                StateTO.class);
        ep.stage(DataTO.class, (context, sto, dto) -> {
            // We don't assert initial state, as that might be set or not, based on whether initialState is sent along.

            sto.number1 = Integer.MAX_VALUE;
            sto.number2 = Math.E;
            context.request(servicePrefix + SERVICE_MID, new DataTO(dto.number, dto.string + ":MidCall1", 3));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.E), sto);
            sto.number1 = 1;
            sto.number2 = 2;

            randomThrow(context);

            context.next(dto);
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(1, 2), sto);
            sto.number1 = Integer.MIN_VALUE;
            sto.number2 = Math.E * 2;
            sto.text = "43289432890532870jklr2jf980gfj234980gj4290gj49g4j290g4j20g423"
                    + "gj904gj42930gj3490gj4390gj4309gj4239058i34290fgj49032ut609342jtg09234ut9034256tu34"
                    + "gj34290g432j90g43j09g43j90g432j09g34u5609t43u59043utg9043jg9043j90g g4390gj4390gj943"
                    + "0gj3490gj4390gj4390gj439g34j90g43kjg940jkg9043j90g43u9543 jit9043i943ui904353490"
                    + "jg9034jg4309jg4390jg0934jg4039gj4309gj4390";
            context.request(servicePrefix + SERVICE_MID, new DataTO(dto.number, dto.string + ":MidCall2", 7));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(Integer.MIN_VALUE, Math.E * 2), sto);
            sto.number1 = Integer.MIN_VALUE / 2;
            sto.number2 = Math.E / 2;
            context.request(servicePrefix + SERVICE_LEAF, new DataTO(dto.number, dto.string + ":LeafCall1", 4));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(Integer.MIN_VALUE / 2, Math.E / 2), sto);
            sto.number1 = Integer.MIN_VALUE / 4;
            sto.number2 = Math.E / 4;
            context.request(servicePrefix + SERVICE_LEAF, new DataTO(dto.number, dto.string + ":LeafCall2", 6));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(Integer.MIN_VALUE / 4, Math.E / 4), sto);
            sto.number1 = Integer.MAX_VALUE / 2;
            sto.number2 = Math.PI / 2;
            context.request(servicePrefix + SERVICE_MID, new DataTO(dto.number, dto.string + ":MidCall3", 8));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE / 2, Math.PI / 2), sto);
            sto.number1 = Integer.MAX_VALUE / 4;
            sto.number2 = Math.PI / 4;
            context.request(servicePrefix + SERVICE_MID, new DataTO(dto.number, dto.string + ":MidCall4", 9));
        });
        ep.lastStage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE / 4, Math.PI / 4), sto);

            randomThrow(context);

            return new DataTO(dto.number * 5, dto.string + ":FromMasterService");
        });
    }

    public static void setupTerminator(String servicePrefix, MatsFactory matsFactory) {
        matsFactory.terminator(servicePrefix + TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    randomThrow(context);
                });
    }

    public static void setupSubscriptionTerminator(String servicePrefix, MatsFactory matsFactory) {
        matsFactory.subscriptionTerminator(servicePrefix + SUBSCRIPTION_TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    // Check if we are directed to throw!
                    if (context.getTraceProperty(THROW, Boolean.class) == Boolean.TRUE) {
                        throw new RuntimeException("Throwing as directed by TraceProperty!");
                    }
                });
    }

    private static void randomThrow(ProcessContext<?> context) throws MatsRefuseMessageException {
        if (context.getTraceProperty("DONT_THROW", Boolean.class) == Boolean.TRUE) {
            return;
        }
        if (Math.random() < 0.01) {
            throw new MatsRefuseMessageException("Random DLQ! This is throwing randomly as directed!"
                    + (Math.random() > 0.5
                            ? " This is MULTILINE!\nThis is a second line!\nThis is a third line {curlies}[brackets]$<lessgreater>\"quotes\"%!;*"
                                    + " <- those are some special characters! (there's two newlines after here)\n\n"
                            : ""));
        }
    }

    public static class DataTO {
        public double number;
        public String string;

        // This is used for the "Test_ComplexLargeMultiStage" to tell the service what it should multiply 'number'
        // with..!
        public int multiplier;

        public DataTO() {
            // For Jackson JSON-lib which needs default constructor.
        }

        public DataTO(double number, String string) {
            this.number = number;
            this.string = string;
        }

        public DataTO(double number, String string, int multiplier) {
            this.number = number;
            this.string = string;
            this.multiplier = multiplier;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            // NOTICE: Not Class-equals, but "instanceof", since we accept the "SubDataTO" too.
            if (!(o instanceof DataTO)) return false;
            DataTO dataTO = (DataTO) o;
            return Double.compare(dataTO.number, number) == 0 &&
                    multiplier == dataTO.multiplier &&
                    Objects.equals(string, dataTO.string);
        }

        @Override
        public int hashCode() {
            return Objects.hash(number, string, multiplier);
        }

        @Override
        public String toString() {
            return "DataTO [number=" + number
                    + ", string=" + string
                    + (multiplier != 0 ? ", multiplier=" + multiplier : "")
                    + "]";
        }
    }

    public static class StateTO {
        public int number1;
        public double number2;

        public String text;

        public StateTO() {
            // For Jackson JSON-lib which needs default constructor.
        }

        public StateTO(int number1, double number2) {
            this.number1 = number1;
            this.number2 = number2;
        }

        @Override
        public int hashCode() {
            return (number1 * 3539) + (int) Double.doubleToLongBits(number2 * 99713.80309);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof StateTO)) {
                throw new AssertionError(StateTO.class.getSimpleName() + " was attempted equalled to [" + obj + "].");
            }
            StateTO other = (StateTO) obj;
            return (this.number1 == other.number1) && (this.number2 == other.number2);
        }

        @Override
        public String toString() {
            return "StateTO [number1=" + number1 + ", number2=" + number2 + "]";
        }
    }
}
