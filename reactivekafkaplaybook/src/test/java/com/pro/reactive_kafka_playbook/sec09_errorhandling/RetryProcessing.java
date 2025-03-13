package com.pro.reactive_kafka_playbook.sec09_errorhandling;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RetryProcessing {
    public static void main(String[] args) {

    }
}

class Producer {
    public static void main(String[] args) {
        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class

        );
        var option = SenderOptions.<String, String>create(map);

        var sender = KafkaSender.create(option);
        var flux = Flux.range(1, 100)
                .map(x -> new ProducerRecord<>("order-event", x.toString(), "order-number-" + x))
                .map(x -> SenderRecord.create(x, x.key()));
        sender.send(flux)
                .doOnComplete(() -> sender.close())
                .subscribe();
    }
}

class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {

        var map = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var option = ReceiverOptions.<String, String>create(map).subscription(List.of("order-event"));

        KafkaReceiver.create(option)
                .receive()
                .concatMap(x -> separateProcessing(x))
                .subscribe();
    }

    private static Mono<Void> separateProcessing(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(x -> {
                    if (x.key().equals("5")) {
                        throw new RuntimeException("DB is down");

                    }
                    var index = ThreadLocalRandom.current().nextInt(1, 100);
                    log.info(" log charector e {}  : key {}", x.value().toCharArray()[index],index);
                    x.receiverOffset().acknowledge();
                })


//                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1)).onRetryExhaustedThrow((x, y) -> y.failure()))
                .retryWhen(retrySpec())
                .doOnError((x) -> log.info(x.getMessage()))
                .onErrorResume(IndexOutOfBoundsException.class,ex->Mono.fromRunnable(()->record.receiverOffset().acknowledge()))
//                .doFinally(x->record.receiverOffset().acknowledge())
                //               .onErrorComplete()
                .then();
    }

    public static Retry retrySpec() {
        return Retry.fixedDelay(3, Duration.ofSeconds(1))
                .filter(x -> IndexOutOfBoundsException.class.isInstance(x))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
