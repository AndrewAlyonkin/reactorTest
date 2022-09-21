package com.afalenkin.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

/**
 * @author Alenkin Andrew
 * oxqq@ya.ru
 *
 * Здесь рассматриваются Cold Observable - холодные, это означает что до тех пор, пока кто-нибудь не подпишется
 * на паблишер - ничего происходить не будет.
 */
@Slf4j
class FluxTest {

    @Test
    void fluxSubscriberTest() {
        Flux<String> flux = Flux.just("first", "second", "third", "fourth", "fifth")
                .log();

        StepVerifier.create(flux)
                .expectNext("first", "second", "third", "fourth", "fifth")
                .verifyComplete();
    }

    @Test
    void fluxSubscriberIntTest() {
        Flux<Integer> flux = Flux.range(0, 6)
                .log();

        flux.subscribe(i -> log.info("Number {}", i));

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberListTest() {
        List<Integer> list = List.of(0, 1, 2, 3, 4, 5);
//        Flux<List<Integer>> flux = Flux.just(list).log(); сгенерирует поток листов

        Flux<Integer> flux = Flux.fromIterable(list)
                .log();

        flux.subscribe(i -> log.info("Number {}", i));

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberIntErrorTest() {
        Flux<Integer> flux = Flux.range(0, 6)
                .log()
                .map(i -> {
                    if (i == 4) throw new RuntimeException();
                    return i;
                });

        flux.subscribe(
                i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("Не выполнится, так как завершение flux не достигнуто"));

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void fluxSubscriberIntErrorNotReceivedTest() {
        Flux<Integer> flux = Flux.range(0, 6)
                .log()
                .map(i -> {
                    if (i == 4) throw new RuntimeException();
                    return i;
                });

        flux.subscribe(
                i -> log.info("Number {}", i),
                e -> log.error("Не выполнится, т.к. до ошибки не дойдет, запрошено только 3 элемента"),
                () -> log.info("Не выполнится, так как завершение flux не достигнуто"),
                subscription -> subscription.request(3));

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3)
                .expectError(RuntimeException.class)
                .verify();
    }

    /**
     * Можно запрашивать у паблишера элементы пачками по несколько штук.
     * В этом примере паблишер будет отдавать числа по 2 штуки, после каждой пары выполнять log() до тех пор пока
     * в нем не кончатся элементы.
     * В данном случае BackPressure = 2, т.е. подписчик может обрабатывать по 2 элемента за раз.
     */
    @Test
    void fluxSubscriberBackPressureTest() {
        Flux<Integer> flux = Flux.range(0, 10)
                .log();

        flux.subscribe(new Subscriber<>() {
            private int count = 0;
            private final int requestCount = 2;
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                subscription.request(requestCount);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .verifyComplete();
    }

    /**
     * Можно сделать то же самое но более кратко и красиво с помощью BaseSubscriber
     */
    @Test
    void fluxSubscriberBackPressureWithBaseSubscriberTest() {
        Flux<Integer> flux = Flux.range(0, 10)
                .log();

        flux.subscribe(new BaseSubscriber<>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberIntervalTest() throws InterruptedException {
        /*
         * Метод interval для создания flux - блокирующий, поэтому действия с событиями, которые он генерирует,
         * будут происходить асинхронно в отдельном фоновом потоке.
         */
        Flux<Long> flux = Flux.interval(Duration.ofMillis(100))
                .log();

        flux.subscribe(i -> log.info("Number {}", i));

        /*
         * Чтобы посмотреть на работу interval можно ненадолго усыпить текущий поток.
         *
         * Если запустить такой флакс на сервере - он будет выполняться бесконечно с заданным интервалом
         */
        Thread.sleep(1000);
    }

    @Test
    void fluxSubscriberIntervalBoundedTest() throws InterruptedException {
        /*
         * Метод interval для создания flux - блокирующий, поэтому действия с событиями, которые он генерирует,
         * будут происходить асинхронно в отдельном фоновом потоке.
         */
        Flux<Long> flux = Flux.interval(Duration.ofMillis(100))
                // можно ограничить выполнение флакс с интервалом заданнным количеством элементов.
                // После того, как будет сгенерировано 3 элемента - флакс завершится
                .take(3)
                .log();

        flux.subscribe(
                i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("Flux completed"));

        Thread.sleep(1000);
    }

    @Test
    void fluxSubscriberIntervalVirtualTimeTest() {
        // Позволяет подделать виртуальное время и проверить что флакс ежедневно будет генерировать числа по возрастающей.
        // При этом флакс нужно создавать внутри withVirtualTime - чтобы подставлять фальшивое время.
        StepVerifier
                .withVirtualTime(() -> Flux.interval(Duration.ofDays(1)).log())
                .expectSubscription()
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L)
                .expectNext(1L)
                .thenCancel()
                .verify();

        // ИЛИ

        StepVerifier
                .withVirtualTime(() -> Flux.interval(Duration.ofDays(1)).log())
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(23)) // проверить что события не будут генерироваться раньше времени
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    @Test
    void fluxSubscriberBackPressureWithLimitRateTest() {
        // Не работает, т.к. limitRate находится над log()
//        Flux<Integer> flux = Flux.range(0, 10)
//                .limitRate(3)
//                .log();
//
//        flux.subscribe(i -> log.info("Number {}", i));
//
//        StepVerifier.create(flux)
//                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
//                .verifyComplete();

        Flux<Integer> flux = Flux.range(0, 10)
                .log()
                .limitRate(3);

        flux.subscribe(i -> log.info("Number {}", i));

        StepVerifier.create(flux)
                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .verifyComplete();
    }
}
