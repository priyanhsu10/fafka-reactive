package com.pro.reactive_kafka_playbook.sec02;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaProducer {

    private static  final Logger log= LoggerFactory.getLogger(KafkaProducer.class);
    public static void main(String[] args) {

        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var senderOption = SenderOptions.<String, String>create(map);
        var sender = KafkaSender.create(senderOption);
        var flux= Flux.range(1,100)
                        .map(x->new ProducerRecord<>("order-event",x.toString(),"order-"+x))
                                .map(x-> SenderRecord.create(x,x.key()));
        sender.send(flux)
                .doOnNext(x->log.info("corelation metadata :{}",x.recordMetadata()))
                .doOnComplete(()->sender.close())
                .subscribe();
    }
}
