package com.pro.reactive_kafka_playbook.sec07_batchprocessing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Consumer {
    public static void main(String[] args) {
        var map = Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG,"batch-processing",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1",
               ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500
        );
        var receiverOptions= ReceiverOptions.<String,String>create(map).subscription(List.of("order-event"))
                        .commitInterval(Duration.ofSeconds(1));


        KafkaReceiver.create(receiverOptions)
                .receiveAutoAck()
                .log()
                //.concatMap(Consumer::process)
                .flatMap(Consumer::process)
                .subscribe();
    }
    public  static Mono<Void> process(Flux<ConsumerRecord<String,String>> flux){
   return      flux.doFirst(()-> System.out.println("----------------------------------"))
                .doOnComplete(()-> System.out.println(" -----end flux---"))
                .doOnNext(x-> System.out.println(x.value()))
                .then(Mono.delay(Duration.ofMillis(100)))
                .then();
    }
}
