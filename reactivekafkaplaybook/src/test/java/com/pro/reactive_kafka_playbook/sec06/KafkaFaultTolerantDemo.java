package com.pro.reactive_kafka_playbook.sec06;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

class Consumer {
    private static final Logger log=LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        var map = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8081",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions= ReceiverOptions.create(map)
                .subscription(List.of("order-event"));
        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(x->x.receiverOffset().acknowledge())
                .doOnNext(x->log.info("value :{}",x.value()))
                .subscribe();
    }
}

class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        var map =Map.<String,Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:8081",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class
        );
        var senderOption = SenderOptions.<String, String>create(map);
        var flux = Flux.interval(Duration.ofMillis(100))
                .map(x -> new ProducerRecord<>("order-event", x.toString(), "order-event-" + x))
                .map(x -> SenderRecord.create(x, x.key()));
        var sender = KafkaSender.create(senderOption);
        sender
                .send(flux)
                .doOnComplete(() -> sender.close())
                .subscribe();
    }
}
