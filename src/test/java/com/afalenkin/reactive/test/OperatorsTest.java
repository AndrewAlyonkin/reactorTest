package com.afalenkin.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author Alenkin Andrew
 * oxqq@ya.ru
 * <p>
 * Операции с элементами флакс и моно при подписке на них всегда выполняются в текущем потоке (за исключением
 * блокирующих операций). Чтобы они выполнялись в отдельных потоках - нужно указывать это явно.
 */
@Slf4j
class OperatorsTest {

    /**
     * subscribeOn задает планировщик, в котором выполняются все операции флакс не зависимо от расположения subscribeOn.
     */
    @Test
    void subscribeOnTest() {

        Flux<Integer> flux = Flux.range(0, 5)
                .map(i -> {
                    log.info("1 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                // Не важно в каком месте расположен subscribeOn - он влияет на все операторы паблишера.
                // Он позволяет задать планировщик, в котором будут выполняться операции map
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("2 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(0, 1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * Имеет большое значение где расположен publishOn - он задает планировщик, в котором будут выполняться
     * последующие после него блоки
     */
    @Test
    void publishOnTest() {

        // В данном случае операции первого блока map - выполнятся в текущем потоке, а второго - в отдельном.
        Flux<Integer> flux = Flux.range(0, 5)
                .map(i -> {
                    log.info("1 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                // Имеет большое значение где расположен publishOn - он задает планировщик только для последующих шагов
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        // Для каждого подписчика будут создаваться новые планировщики
        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(0, 1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * Если в цепочке флакс с помощью subscribeOn задается несколько различных планировщиков - для всей цепочки
     * будет использоваться только первый по цепочке вызовов планировщик
     */
    @Test
    void multipleSubscribeOnTest() {
        Flux<Integer> flux = Flux.range(0, 5)
                // Будет использоваться первый в цепочке вызовов subscribeOn
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("1 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(0, 1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * Если в цепочке используется несколько publishOn - он будет распространяться на все последующие
     * блоки в цепочке до тех пор, пока не встретится следующий publishOn - он задаст новый планировщик для последующих
     * блоков
     */
    @Test
    void multiplePublishOnTest() {

        Flux<Integer> flux = Flux.range(0, 5)
                // Следующие операции выполнятся в single планировщике
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("1 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                // Следующие операции выполнятся в boundedElastic планировщике
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        // Для каждого подписчика будут создаваться новые планировщики
        flux.subscribe();
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(0, 1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * Если subscribeOn расположен по цепочке вызовов после publishOn - то subscribeOn будет проигнорирован.
     * Все операции выполнятся в планировщике, который задается в publishOn.
     */
    @Test
    void publishOnAndSubscribeOnTest() {

        Flux<Integer> flux = Flux.range(0, 5)
                // Следующие операции выполнятся в single планировщике
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("1 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                // Следующие операции выполнятся в boundedElastic планировщике
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        // Для каждого подписчика будут создаваться новые планировщики
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(0, 1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * Если subscribeOn расположен в цепочке перед publishOn - то все расположенные после subscribeOn операции
     * выполнятся в планировщике, который в нем задается. Но как только встретится publishOn - он изменит планировщик для
     * выполнения дальнейших операций и все последующие subscribeOn будут игнорироваться
     */
    @Test
    void subscribeOnAndPublishOnTest() {

        Flux<Integer> flux = Flux.range(0, 5)
                // Следующие операции выполнятся в single планировщике
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("1 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                // Следующие операции выполнятся в boundedElastic планировщике
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("2 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("3 MAP block for {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        // Для каждого подписчика будут создаваться новые планировщики
        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(0, 1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void subscribeOnIOTest() throws InterruptedException {
        Mono<List<String>> mono = Mono.fromCallable(() -> Files.readAllLines(Path.of("src/test/resources/file.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        // Будет выполняться асинхронно в отдельном потоке
        mono.subscribe(s -> log.info("{}", s));
        Thread.sleep(50);

        StepVerifier.create(mono)
                .expectSubscription()
                .thenConsumeWhile(list -> {
                    assertFalse(list.isEmpty());
                    log.info("List size {}", list.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    void switchIfEmptyTest() {
        Flux<Object> empty = Flux.empty();

        // Можно предусмотреть случай, когда флакс окажется пустым.
        // С помощью switchIfEmpty можно предоставить альтернативный паблишер, если основной оказался пустым
        Flux<Object> notEmpty = empty
                .switchIfEmpty(Flux.just("notEmpty"))
                .log();

        StepVerifier.create(notEmpty)
                .expectSubscription()
                .expectNext("notEmpty")
                .expectComplete()
                .verify();
    }

    @Test
    void deferTest() throws InterruptedException {
        Mono<Long> mono = Mono.just(System.currentTimeMillis());

        // Несмотря на разрыв в 100 миллисекунд между подписчиками - во всех них будет выводиться
        // идентичное значение, потому что моно каждый раз отдает объект, созданный в момент инстанциирования.
        mono.subscribe(l -> log.info("From mono time is {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("From mono time is {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("From mono time is {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("From mono time is {}", l));

        //=============================================================
        // defer позволяет моно создавать новое событие каждый раз, когда кто-либо на него подпишется
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        // Время везде будет разное, так как каждый раз будет создаваться новый объект
        defer.subscribe(l -> log.info("From defer time is {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("From defer time is {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("From defer time is {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("From defer time is {}", l));
    }

    /**
     * concat и concatWith - Lazy операторы: второй флакс не запустится до тех пор, пока не завершится первый
     */
    @Test
    void concatTest() {
        Flux<String> first = Flux.just("A", "B", "C");
        Flux<String> second = Flux.just("D", "E", "F");

        // concat позволяет последовательно объединить два флакса. Сначала пройдут элементы первого, после
        // их окончания - пойдут элементы второго
        Flux<String> concat = Flux.concat(first, second).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("A", "B", "C", "D", "E", "F")
                .expectComplete()
                .verify();
    }

    @Test
    void concatWithTest() {
        Flux<String> first = Flux.just("A", "B", "C");
        Flux<String> second = Flux.just("D", "E", "F");

        // concatWith позволяет последовательно подсоединить к первому флаксу элементы второго.
        // Сначала пройдут элементы первого, после их окончания - пойдут элементы второго
        Flux<String> concat = first.concatWith(second).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("A", "B", "C", "D", "E", "F")
                .expectComplete()
                .verify();
    }

    @Test
    void combineLastTest() {
        Flux<String> first = Flux.just("A", "B", "C");
        Flux<String> second = Flux.just("D", "E", "F");

        // Позволяет комбинировать элементы двух флаксов.
        // Принимает элемент первого+элемент второго и возвращает их комбинацию по заданной функции
        // Нет гарантий что элементы флаксов будут строго соответствовать друг другу(первый-первому, второй-второму и т.д.)
        // Операция происходит каждый раз с последними испущенными флаксами событиями
        Flux<String> combined = Flux.combineLatest(first, second, (s1, s2) -> s1.concat("-").concat(s2))
                .log();

        combined.subscribe();

        StepVerifier.create(combined)
                .expectSubscription()
                .expectNext("C-D", "C-E", "C-F")
                .expectComplete()
                .verify();
    }

    @Test
    void combineLastWithDelayTest() {
        Flux<String> first = Flux.just("A", "B", "C").delayElements(Duration.ofMillis(5));
        Flux<String> second = Flux.just("D", "E", "F");

        Flux<String> combined = Flux.combineLatest(first, second, (s1, s2) -> s1.concat("-").concat(s2))
                .log();

        combined.subscribe();
        //A-F
    }

    @Test
    void mergeTest() throws InterruptedException {
        Flux<String> first = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(200));
        Flux<String> second = Flux.just("D", "E", "F");

        // merge не дожидается завершения первого флакса, а сразу запускает второй, как только появляется возможность
        // При этом флаксы могут исполняться в отдельном потоке
        Flux<String> merged = Flux.merge(first, second).log();

        merged.subscribe(log::info);

        Thread.sleep(300);

        StepVerifier.create(merged)
                .expectSubscription()
                .expectNext("D", "E", "F", "A", "B", "C")
                .expectComplete()
                .verify();
    }

    @Test
    void mergeWithTest() throws InterruptedException {
        Flux<String> first = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(200));
        Flux<String> second = Flux.just("D", "E", "F");

        Flux<String> merged = first.mergeWith(second).log();

        merged.subscribe(log::info);

        StepVerifier.create(merged)
                .expectSubscription()
                .expectNext("D", "E", "F", "A", "B", "C")
                .expectComplete()
                .verify();
    }

    // https://stackoverflow.com/questions/67857350/project-reactor-what-are-differences-between-flux-concat-flux-mergesequential

    /**
     * mergeSequential похоже на concat, но выполняется eager
     *
     * @throws InterruptedException
     */
    @Test
    void mergeSequentialTest() throws InterruptedException {
        Flux<String> first = Flux.just("A", "B", "C");
        Flux<String> second = Flux.just("D", "E", "F");

        Flux<String> merged = Flux.mergeSequential(first, second, first)
                .delayElements(Duration.ofMillis(100))
                .log();

        merged.subscribe(log::info);

        StepVerifier.create(merged)
                .expectSubscription()
                .expectNext("A", "B", "C", "D", "E", "F", "A", "B", "C")
                .expectComplete()
                .verify();
    }

    @Test
    void concatErrorTest() {
        Flux<String> first = Flux.just("A", "B", "C")
                .map(s -> {
                    if (s.equals("C")) throw new RuntimeException();
                    return s;
                });
        Flux<String> second = Flux.just("D", "E", "F");

        Flux<String> concat = Flux.concat(first, second).log();

        // После возникновения исключения - прекратится исполнение всего флакса полностью
        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("A", "B")
                .expectError(RuntimeException.class)
                .verify();

        // При использовании concatDelayError - все элементы флакса, для которых не возникло исключения, отработают,
        // а исключение будет отложено на момент завершения флакса
        Flux<String> concatWithError = Flux.concatDelayError(first, second).log();

        StepVerifier.create(concatWithError)
                .expectSubscription()
                .expectNext("A", "B", "D", "E", "F")
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void mergeErrorTest() {
        Flux<String> first = Flux.just("A", "B", "C")
                .map(s -> {
                    if (s.equals("C")) throw new RuntimeException();
                    return s;
                });
        Flux<String> second = Flux.just("D", "E", "F");

        Flux<String> merge = Flux.merge(first, second).log();

        // После возникновения исключения - прекратится исполнение всего флакса полностью
        StepVerifier.create(merge)
                .expectSubscription()
                .expectNext("A", "B")
                .expectError(RuntimeException.class)
                .verify();

        // При использовании mergeDelayError - все элементы флакса, для которых не возникло исключения, отработают,
        // а исключение будет отложено на момент завершения флакса
        Flux<String> mergeWithError = Flux
                .mergeDelayError(1, first, second)
                .log()
                .doOnError(thr -> log.error("OnError {}", thr.getClass().getSimpleName()));

        StepVerifier.create(mergeWithError)
                .expectSubscription()
                .expectNext("A", "B", "D", "E", "F")
                .expectError(RuntimeException.class)
                .verify();
    }

    /**
     * flatMap - преобразует элемент текущего флакса в другой флакс, который подменит текущий
     */
    @Test
    void flatMapTest() {
        Flux<String> flux = Flux.just("a", "b", "c");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::getByPref)
                .log();

        flatFlux.subscribe(OperatorsTest.log::info);

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("AWord", "WordA", "BWord", "WordB", "CWord", "WordC")
                .verifyComplete();
    }

    @Test
    void flatMapSequentialTest() {
        Flux<String> flux = Flux.just("a", "b", "c");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::getByPref)
                .log();

        flatFlux.subscribe(OperatorsTest.log::info);

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("AWord", "WordA", "BWord", "WordB", "CWord", "WordC")
                .verifyComplete();
    }

    private Flux<String> getByPref(String prefix) {
        return switch (prefix) {
            case "A" -> Flux.just("AWord", "WordA");
            case "B" -> Flux.just("BWord", "WordB");
            case "C" -> Flux.just("CWord", "WordC");
            default -> Flux.just("undefined");
        };
    }

    /**
     * Flux.zip - позволяет объединить элементы нескольких флакс в Tuple, который хранит в себе элементы флаксов.
     * Этот Tuple можно использовать в map или flatMap чтобы сформировать из них новый объект.
     * Flux.zip будет иметь длину самого короткого флакса из набора.
     */
    @Test
    void zipTest() {
        Flux<String> first = Flux.just("first", "second", "third", "fourth");
        Flux<Integer> second = Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Flux<UUID> third = Flux.just(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        Flux<Tuple3<String, Integer, UUID>> zip = Flux.zip(first, second, third);

        zip.flatMap(tuple3 -> Flux.just(
                tuple3.getT2().toString()
                        .concat(" : ")
                        .concat(tuple3.getT1())
                        .concat(" - ")
                        .concat(tuple3.getT3().toString())
        )).log();

//  [first,1,4a83a617-8ca4-4dae-a70b-1453f3c10a35]
//  [second,2,32586946-9cd1-4e11-9b49-7a57fe051100]
//  [third,3,a91a6c14-f4c3-422e-a3ae-989cac23ad0e]

        zip.subscribe(obj -> log.info(obj.toString()));
    }

    @Test
    void zipWithTest() {
        Flux<String> first = Flux.just("first", "second", "third", "fourth").log();
        Flux<Integer> second = Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Flux<UUID> third = Flux.just(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()).log();

        Flux<Tuple2<String, UUID>> zip = first.zipWith(third);

        zip.flatMap(tuple -> Flux.just(
                tuple.getT2().toString()
                        .concat(" : ")
                        .concat(tuple.getT1()))
        );

        zip.subscribe(obj -> log.info(obj.toString()));
    }

}
