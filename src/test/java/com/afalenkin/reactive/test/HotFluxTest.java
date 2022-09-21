package com.afalenkin.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * @author Alenkin Andrew
 * oxqq@ya.ru
 */
@Slf4j
class HotFluxTest {

    @Test
    void connectableFluxTest() throws InterruptedException {
        // Флакс, который может испускать события без подписчика. Subscriber могут подписываться
        // на него и получать элементы, которые испускаются в текущий момент.
        // delayElements - позволяет задать временной интервал между испусканием элементов. При этом интервал
        // не распространяется на пустые события и ошибки
        ConnectableFlux<Integer> connectable = Flux.range(0, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        // При вызове метода connect флакс начнет испускать элементы, при этом существование подписчика не требуется.
        connectable.connect();

        // Усыпим текущий поток на 400 мс
        log.info("Current thread sleeps for 400ms");
        Thread.sleep(400);

        // За это время флакс должен испустить 4 элемента и мы не должны увидеть их при подписке,
        // мы увидим только последующие.
        connectable.subscribe(i -> log.info("{} from subscriber 1", i));
// --------------------------------------------------
        // Усыпим текущий поток на 200 мс
        log.info("Current thread sleeps for 400ms");
        Thread.sleep(200);

        // За это время флакс должен испустить 4 элемента и мы не должны увидеть их при подписке,
        // мы увидим только последующие.
        connectable.subscribe(i -> log.info("{} from subscriber 2", i));
        Thread.sleep(400);

        StepVerifier.create(connectable)
                .then(connectable::connect)
                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .expectComplete()
                .verify();
    }

    @Test
    void connectableFluxStepVerifierTest() throws InterruptedException {
        ConnectableFlux<Integer> connectable = Flux.range(0, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectable.connect();

        StepVerifier.create(connectable)
                .then(connectable::connect)
                .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                .expectComplete()
                .verify();
    }

    @Test
    void connectableFluxStepVerifierBoundedTest() throws InterruptedException {
        ConnectableFlux<Integer> connectable = Flux.range(0, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectable.connect();

        StepVerifier.create(connectable)
                .then(connectable::connect)
                // Пропустить первые несколько шагов по условию. В данном случае он проигнорирует первые 4 элемента.
                .thenConsumeWhile(i -> i <= 4)
                .expectNext(5, 6, 7, 8, 9)
                .expectComplete()
                .verify();
    }

    @Test
    void connectableFluxAutoConnectTest() throws InterruptedException {
        // autoConnect - позволяет запускать испускание элементов при достижении определенного количества подписчиков.
        // В данном случае элементы начнут испускаться только при достижении 2 subscriber.
        Flux<Integer> autoFlux = Flux.range(0, 5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(3);

        // Если здесь не создать подписчиков 2 и 3 - этот участок кода будет выполняться бесконечно,
        // т.к. флакс запустится только при наличии 3 подписчиков
        StepVerifier
                .create(autoFlux)  // подписчик 1
                .then(autoFlux::subscribe) // подписчик 2
                .then(autoFlux::subscribe) // подписчик 3
                .expectNext(0, 1, 2, 3, 4)
                .expectComplete()
                .verify();
    }


}
