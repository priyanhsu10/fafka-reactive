package com.pro.reactive_kafka_playbook.sec05;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
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

public class MultipleConsumerLoadSharing {

    public static class Consumer1 {
        public static void main(String[] args) {
             new Consumer().run("1");
        }
    }
    public static class Consumer2 {
        public static void main(String[] args) {
           new Consumer().run("2");
        }
    }

    public static class Consumer3 {
        public static void main(String[] args) {
             new Consumer().run("3");
        }
    }

}
class Producer{
    private  final static Logger log =LoggerFactory.getLogger(Producer.class);
    public static void main(String[] args) {
        var map = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var senderOptions = SenderOptions.<String, String>create(map)
                .maxInFlight(10_000);
        var flux = Flux.
        interval(Duration.ofMillis(100)).map(x -> new ProducerRecord<>("order-event", x.toString(), "order-item-" + x))
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
class Consumer {
    private final static Logger log = LoggerFactory.getLogger(Consumer.class);

    public void run(String groupInsetaceid) {
        var map = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "laadshreader",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInsetaceid,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()


        );
        var receiverOptions = ReceiverOptions.create(map)
                .subscription(List.of("order-event"));
        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(x -> log.info("value : {}", x.value()))
                .doOnNext(x->x.receiverOffset().acknowledge())
                .subscribe();
    }

}