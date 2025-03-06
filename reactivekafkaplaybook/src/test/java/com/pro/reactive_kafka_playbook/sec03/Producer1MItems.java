package com.pro.reactive_kafka_playbook.sec03;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class Producer1MItems {

    private static final  Logger log = LoggerFactory.getLogger(Producer1MItems.class);
    public static void main(String[] args) {
        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var senderOptions = SenderOptions.<String, String>create(map)
                .maxInFlight(10_000);
        var flux = Flux.range(1, 1_000_000)
                .map(x -> new ProducerRecord<>("order-event", x.toString(), "order-item-" + x))
                .map(x -> SenderRecord.create(x, x.key()));
        var sender = KafkaSender.create(senderOptions);
        var start = System.currentTimeMillis();
        sender
                .send(flux)
                .doOnNext(x -> log.info("correlation id:{} ", x.recordMetadata()))
                .doOnComplete(() -> {
                    log.info("total time taken: {} ms", (System.currentTimeMillis() - start));
                    sender.close();
                })
                .subscribe();


    }
}
