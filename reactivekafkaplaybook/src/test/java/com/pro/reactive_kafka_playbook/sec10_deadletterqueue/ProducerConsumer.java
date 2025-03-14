package com.pro.reactive_kafka_playbook.sec10_deadletterqueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

public class ProducerConsumer {
}

class Producer {
    public static void main(String[] args) {

        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var senderOption = SenderOptions.<String, String>create(map);
        var flux = Flux.interval(Duration.ofMillis(100))
                .take(20)
                .map(x -> {
                    var producerRec = new ProducerRecord<>("order-event", x.toString(), "order-item-" + x);
                    var senderRecord = SenderRecord.create(producerRec, producerRec.key());
                    return senderRecord;
                });
        var sender = KafkaSender.create(senderOption);

        sender.send(flux)
                .doOnComplete(() -> sender.close())
                .subscribe();
    }
}

class Consumer {

    public static void main(String[] args) {

        var map = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOption = ReceiverOptions.<String, String>create(map)
                .subscription(List.of("order-event", "order-eventdlt"));

        DeadLetterTopicProducer<String, String> deadLetterTopicProducer = deadLetterTopicProducer();
        OrderEventProcessor processor = new OrderEventProcessor(deadLetterTopicProducer);
        KafkaReceiver.create(receiverOption)
                .receive()
                .concatMap(x -> processor.process(x))
                .subscribe();
    }

    private static Mono<Void> separateProcessing(ReceiverRecord<String, String> record) {
        return
                Mono.just(record)
                        .doOnNext(x -> {
                            if (x.key().equals("5")) {
                                throw new RuntimeException("oops");
                            }
                            System.out.println(x.value());
                            x.receiverOffset().acknowledge();
                        })
                        .then();
    }

    private static DeadLetterTopicProducer<String, String> deadLetterTopicProducer() {

        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var options = SenderOptions.<String, String>create(map);
        var sender = KafkaSender.create(options);
        return new DeadLetterTopicProducer<>(sender, Retry.fixedDelay(2, Duration.ofSeconds(1)));
    }

}