package com.pro.reactive_kafka_playbook.sec07_batchprocessing;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class Producer {
    public static void main(String[] args) {
        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var senderOptions = SenderOptions.<String, String>create(map);
        var flux = Flux.range(1, 100)
                .map(x -> new ProducerRecord<>("order-event", x.toString(), "order-item"))
                .map(x -> SenderRecord.create(x, x.key()));

        var sender = KafkaSender.create(senderOptions);
                sender.send(flux)
                .doOnComplete(()->sender.close())
                .subscribe();



    }
}
