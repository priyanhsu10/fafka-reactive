package com.pro.reactive_kafka_playbook.sec10_deadletterqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public class OrderEventProcessor {
    private final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);
    private final DeadLetterTopicProducer<String, String> deadLetterTopicProducer;

    public OrderEventProcessor(DeadLetterTopicProducer<String, String> deadLetterTopicProducer) {
        this.deadLetterTopicProducer = deadLetterTopicProducer;
    }

    public Mono<Void> process(ReceiverRecord<String, String> receiverRecord) {

        return Mono.just(receiverRecord)
                .doOnNext(x -> {
                    if (x.key().endsWith("5")) {
                        throw new RuntimeException("Proccessing Exception");
                    }
                    log.info("key {}, value {}", x.key(), x.value());
                    x.receiverOffset().acknowledge();
                })
                .onErrorMap(x->new RecordProcessingException(receiverRecord,x))
                .transform(deadLetterTopicProducer.recordProcessingErrorHandler());
    }
}
