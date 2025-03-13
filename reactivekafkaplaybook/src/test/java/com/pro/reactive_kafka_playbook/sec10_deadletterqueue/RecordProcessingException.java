package com.pro.reactive_kafka_playbook.sec10_deadletterqueue;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {
    private final ReceiverRecord<?, ?> record;

    public RecordProcessingException(ReceiverRecord<?, ?> record, Throwable ex) {
        super(ex);
        this.record = record;
    }
    public <K,V>  ReceiverRecord<K,V> getRecord(){
        return (ReceiverRecord<K, V>) record;
    }
}
