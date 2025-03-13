package com.pro.reactive_kafka_playbook.sec10_deadletterqueue;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

public class DeadLetterTopicProducer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterTopicProducer.class);

    private final KafkaSender<K, V> sender;
    private final Retry retrySpec;


    public DeadLetterTopicProducer(KafkaSender<K, V> sender, Retry retrySpec) {
        this.sender = sender;
        this.retrySpec = retrySpec;
    }

    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> receiverRecord) {
        var pr = new ProducerRecord<>(receiverRecord.topic() + "dlt", receiverRecord.key(), receiverRecord.value());
        return SenderRecord.create(pr, pr.key());
    }

    private Mono<SenderResult<K>> produce(ReceiverRecord<K, V> record) {
        var sr = toSenderRecord(record);
        Mono<SenderResult<K>> next = this.sender.send(Mono.just(sr)).next();
        return next;
    }

    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono ->
                mono.retryWhen(this.retrySpec)
                        .onErrorMap(ex -> ex.getCause() instanceof RecordProcessingException, Throwable::getCause)
                        .doOnError(ex -> log.error(ex.getMessage()))
                        .onErrorResume(RecordProcessingException.class,
                                ex -> this.produce(ex.getRecord())
                                        .then(Mono.fromRunnable(() -> ex.getRecord().receiverOffset().acknowledge()))
                        )
                        .then();
    }


}
