package com.afalenkin.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * ReactiveStreams Test
 * <p>1. Asynchronous</p>
 * <p>2. Non-Blocking</p>
 * <p>3. Backpressure</p>
 * <p>
 * Содержит в себе интерфейсы :
 * <ol>Publisher - производит события. Данный паблишер холодный - до тех пор, пока на него кто-то не подпишется - он
 * не начнет производить события</ol>
 * <ol>Subscriber - подписывается на Publisher. </ol>
 * <ol>Subscription - этот объект возникает когда Subscriber подписываеся на Publisher. Может использоваться
 * для управления BackPressure(запрашивать количество элементов, которое он может обработать в единицу времени)</ol>
 * <ol></ol>
 */
@Slf4j
class MonoTest {

    /**
     * Mono - имплементация интерфейса Publisher,
     * поток из одного элемента или void-события (не более одного элемента: 0..1).
     */
    @Test
    void monoSubscriberTest() {
        String testName = "Andrew";

//        Метод just создает поток, который испускает предоставленные элементы, а затем завершается.
//        Ничего не передается, пока кто-нибудь на это не подпишется.
        Mono<String> mono = Mono.just(testName).log();

//        Чтобы подписаться на него, мы вызываем метод subscribe.
        mono.subscribe();

        StepVerifier.create(mono)
                .expectNext(testName)
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumerTest() {
        String testName = "Andrew";

        Mono<String> mono = Mono.just(testName).log();

        mono.subscribe(string -> log.info("{}. Выполнится для каждого элемента после активации подписки.", string));

        StepVerifier.create(mono)
                .expectNext(testName)
                .verifyComplete();
    }

    @Test
    void monoSubscriberErrorTest() {
        String testName = "Andrew";

        Mono<String> mono = Mono.just(testName)
                .map(s -> {
                    throw new RuntimeException();
                });

        // В случае возникновения исключений можно выполнить какое то действие
        mono.subscribe(
                string -> log.info("{}. Выполнится для каждого элемента после активации подписки.", string),
                string -> log.error("Выполнится при возникновении исключений"));

        // Или залогировать/вывести стектрейс
        mono.subscribe(string -> log.info("{}. Выполнится для каждого элемента после активации подписки.", string),
                Throwable::printStackTrace);

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void monoSubscriberCompleteTest() {
        String testName = "Andrew";

        Mono<String> mono = Mono.just(testName)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("{}. Выполнится для каждого элемента после активации подписки.", s),
                s -> log.error("Выполнится в случае ошибки"),
                () -> log.info("Выполнится при завершении обработки элемента"));

        StepVerifier.create(mono)
                .expectNext(testName.toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoSubscriberSubscriptionTest() {
        String testName = "Andrew";

        Mono<String> mono = Mono.just(testName)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("{}. Выполнится для каждого элемента после активации подписки.", s),
                s -> log.error("Выполнится в случае ошибки"),
                () -> log.info("Выполнится при завершении обработки элемента"),
                Subscription::cancel); // Отменить подписку и очистить ресурсы

        mono.subscribe(
                s -> log.info("{}. Выполнится для каждого элемента после активации подписки.", s),
                s -> log.error("Выполнится в случае ошибки"),
                () -> log.info("Выполнится при завершении обработки элемента"),
                subscription -> {
                    log.info("Управление подпиской");
                    subscription.request(5); // Ограничить подписку 5 элементами
                });

        StepVerifier.create(mono)
                .expectNext(testName.toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoDoOnTest() {
        String testName = "Andrew";

        Mono<String> mono = Mono.just(testName)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Добавить поведение(сайд-эффект) когда кто то подпишется."))
                .doOnRequest(longNumber -> log.info("Добавить поведение которое выполнится при получении request"))
                // doOnNext может не выполниться если не было произведено событий
                .doOnNext(s -> log.info("Добавить поведение, которое выполнится после успешного испускания события {}", s))
                .doOnSuccess(s -> log.info("Добавить поведение, которое выполнится после успешного завершения mono. {}", s));

        mono.subscribe(
                s -> log.info("{}. Выполнится для каждого элемента после активации подписки.", s),
                s -> log.error("Выполнится в случае ошибки"),
                () -> log.info("Выполнится при завершении обработки элемента"),
                subscription -> subscription.request(5)); // Отменить подписку и очистить ресурсы
    }

    @Test
    void monoDoOnNextTest() {
        String testName = "Andrew";

        Mono<Object> mono = Mono.just(testName)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Добавить поведение(сайд-эффект) когда кто то подпишется."))
                .doOnRequest(longNumber -> log.info("Добавить поведение которое выполнится при получении request"))
                // doOnNext может не выполниться если не было произведено событий
                .doOnNext(s -> log.info("Добавить поведение, которое выполнится после успешного испускания события {}", s))
                .flatMap(s -> Mono.empty())
/* не выполнится*/.doOnNext(s -> log.info("НЕ ВЫПОЛНИТСЯ Добавить поведение, которое выполнится после успешного испускания события {}", s))
                .doOnSuccess(s -> log.info("Добавить поведение, которое выполнится после успешного завершения mono. {}", s));

        mono.subscribe(
                s -> log.info("{}. Выполнится для каждого элемента после активации подписки.", s),
                s -> log.error("Выполнится в случае ошибки"),
                () -> log.info("Выполнится при завершении обработки элемента"),
                subscription -> subscription.request(5)); // Отменить подписку и очистить ресурсы
    }

    @Test
    void monoDoOnErrorTest() {
        Mono<Object> error = Mono.error(IllegalArgumentException::new)
                .doOnError(e -> MonoTest.log.error("Выполнится при возникновении исключения", e))
                .doOnNext(s -> MonoTest.log.info("Не отработает"))
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void monoDoOnErrorResumeTest() {
        Mono<Object> error = Mono.error(IllegalArgumentException::new)
                .doOnError(e -> MonoTest.log.error("Выполнится при возникновении исключения", e))
                .onErrorResume(s -> {
                    log.info("Если возникло исключение - использовать запасной Publisher");
                    return Mono.just("Fallback");
                })
                .doOnNext(s -> log.info(s.toString()))
                .log();

        StepVerifier.create(error)
                .expectNext("Fallback")
                .verifyComplete();
    }

    @Test
    void monoDoOnErrorAfterResumeTest() {
        Mono<Object> error = Mono.error(IllegalArgumentException::new)
                .onErrorResume(s -> {
                    log.info("Если возникло исключение - использовать запасной Publisher");
                    return Mono.just("Fallback");
                })
                .doOnError(e -> MonoTest.log.error("Не выполнится, так как onErrorResume отработает раньше", e))
                .doOnNext(s -> log.info(s.toString()))
                .log();

        StepVerifier.create(error)
                .expectNext("Fallback")
                .verifyComplete();
    }

    @Test
    void monoDoOnErrorReturnTest() {
        Mono<Object> error = Mono.error(IllegalArgumentException::new)
                .onErrorReturn("Если возникло исключение - вернуть альтернативное значение")
                .doOnError(e -> MonoTest.log.error("Не выполнится, так как onErrorReturn отработает раньше", e))
                .doOnNext(s -> log.info(s.toString()))
                .log();

        StepVerifier.create(error)
                .expectNext("Если возникло исключение - вернуть альтернативное значение")
                .verifyComplete();
    }
}
