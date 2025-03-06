package com.pro.reactive_kafka_playbook.sec01;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class Lec02KafkaMultipleTopicConsumer {
    private static final Logger log = LoggerFactory.getLogger(Lec02KafkaMultipleTopicConsumer.class);
    public static void main(String[] args) {
        var map= Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG,"demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"
        );
        var receiverOptions= ReceiverOptions.create(map)
                .subscription(List.of("order-event","order-return"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(x-> log.info("topic {} , value : {}",x.topic(),x.value()) )
                .doOnNext(x->x.receiverOffset().acknowledge())
                .subscribe();
    }
}


