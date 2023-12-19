package com.github.dfauth.util;

import com.github.dfauth.ta.functional.Lists;
import com.github.dfauth.ta.util.ArrayRingBuffer;
import com.github.dfauth.ta.util.RingBuffer;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.github.dfauth.ta.functional.Function2.rightCurry;
import static com.github.dfauth.ta.functional.Lists.mapList;
import static java.util.function.Function.identity;
import static junit.framework.TestCase.assertEquals;

public class ChangeCollectorTest {

    public static final List<Integer> INPUT = List.of(1,2,4,8,16);
    public static final Function<List<Integer>,BigDecimal> SMA = l -> BigDecimal.valueOf(l.stream().mapToDouble(Integer::doubleValue).sum() / l.size());
    public static final BiFunction<Integer,Integer,Integer> subtraction = (i1,i2) -> i1-i2;
    public static final Function<Integer, Function<Integer,Integer>> curriedSubtraction = rightCurry(subtraction);

    @Test
    public void testIt() {
        List<Function<Integer, Integer>> result = mapList(INPUT, curriedSubtraction);
        BiFunction<Integer, Function<Integer, Integer>, Integer> f1 = (i, _f) -> _f.apply(i);
        List<Integer> a = INPUT.subList(1, result.size());
        List<Function<Integer, Integer>> b = result.subList(0, result.size() - 1);
        List<Integer> result1 = Lists.zip(a,b,f1);
        assertEquals(List.of(1,2,4,8), result1);
        BigDecimal sma = result1.stream().collect(ringBufferCollector(new Integer[3],SMA));
        assertEquals(4.66667, sma.doubleValue(), 0.001d);
    }

    @Test
    public void testItAgain() {
//        BigDecimal sma = INPUT.stream().map(i -> Map.entry(i,curriedSubtraction.apply(i))).reduce(
        BigDecimal sma = INPUT.stream().map(Annotated.annotateWith(curriedSubtraction)).reduce(
                new Thingy<Integer,Integer>(),
                Thingy::add,
                oops()
        ).map(l -> l.stream().collect(ringBufferCollector(new Integer[3],SMA)));
        assertEquals(4.66667, sma.doubleValue(), 0.001d);
    }

    public static <U> BinaryOperator<U> oops() {
        return (t1,t2) -> {
            throw new IllegalStateException("Oops. Parallel operations not supported");
        };
    }

    public static <T,R> Collector<T, RingBuffer<T>,R> ringBufferCollector(T[] buffer, Function<List<T>,R> finisher) {
        return mutableCollector(
                new ArrayRingBuffer<>(buffer),
                (_rb,t) -> {
                    _rb.write(t);
                    return _rb;
                },
                _rb -> {
                    List<T> l = _rb.stream().collect(Collectors.toList());
                    return finisher.apply(l);
                }
        );
    }

    public static <T,A> Collector<T, AtomicReference<A>,A> collector(A initial, BiFunction<A,T,A> accumulator) {
        return collector(initial, accumulator, identity());
    }

    public static <T,A,R> Collector<T, AtomicReference<A>,R> collector(A initial, BiFunction<A,T,A> accumulator, Function<A,R> finisher) {
        return new Collector<>() {
            @Override
            public Supplier<AtomicReference<A>> supplier() {
                return () -> new AtomicReference<>(initial);
            }

            @Override
            public BiConsumer<AtomicReference<A>, T> accumulator() {
                return (a,t) -> a.set(accumulator.apply(a.get(), t));
            }

            @Override
            public BinaryOperator<AtomicReference<A>> combiner() {
                return (a1,a2) -> {
                    throw new IllegalStateException("Parallel operations not supported");
                };
            }

            @Override
            public Function<AtomicReference<A>, R> finisher() {
                return a -> finisher.apply(a.get());
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of();
            }
        };
    }

    public static <T,A,R> Collector<T, A,R> mutableCollector(A initial, BiFunction<A,T,A> accumulator, Function<A,R> finisher) {
        return new Collector<>() {
            @Override
            public Supplier<A> supplier() {
                return () -> initial;
            }

            @Override
            public BiConsumer<A, T> accumulator() {
                return accumulator::apply;
            }

            @Override
            public BinaryOperator<A> combiner() {
                return (a1,a2) -> {
                    throw new IllegalStateException("Parallel operations not supported");
                };
            }

            @Override
            public Function<A, R> finisher() {
                return finisher;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of();
            }
        };
    }

    public static class Thingy<T,A> {

        private AtomicReference<Function<T,A>> input = new AtomicReference<>(null);
        private List<A> output = new ArrayList<>();

        public Thingy<T,A> add(Annotated<T,Function<T,A>> e) {
            Optional.ofNullable(input.get()).map(f -> f.apply(e.payload())).ifPresent(v -> output.add(v));
            input.set(e.annotation());
            return this;
        }

        public <R> R map(Function<List<A>,R> f) {
            return f.apply(output);
        }
    }

    interface Annotated<T,R> {
        T payload();
        R annotation();

        static <T,R> Function<T,Annotated<T,R>> annotateWith(Function<T,R> f) {
            return t -> annotate(t,f);
        }

        static <T,R> Annotated<T,R> annotate(T t, Function<T,R> f) {
            return new Annotated<>() {
                @Override
                public T payload() {
                    return t;
                }

                @Override
                public R annotation() {
                    return f.apply(t);
                }
            };
        }
    }
}
