package com.pro.reactive_kafka_playbook.sec04;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class Producer {
    public static void main(String[] args) {
        var map= Map.<String,Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class
        );
        var senderOptions= SenderOptions.<String,String>create(map);

        var sender = KafkaSender.create(senderOptions)
                .send(Mono.fromSupplier(()->{

                    var header = new RecordHeaders();
                    header.add("client-id","123".getBytes());
                    header.add("tranz-id","tx 123".getBytes());
                    var p=new ProducerRecord<>("order-event",null,"1","item-1",header);
                    var sr= SenderRecord.create(p,p.key());
                    return  sr;

                }))
                .subscribe();
    }
}
