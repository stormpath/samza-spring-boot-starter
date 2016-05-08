package com.stormpath.spring.boot.samza

import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemProducer

import java.util.concurrent.LinkedBlockingQueue

class MemorySystemProducer implements SystemProducer {

    public final Map<String,LinkedBlockingQueue<OutgoingMessageEnvelope>> queues = new HashMap<>();

    @Override
    void start() {

    }

    @Override
    void stop() {

    }

    @Override
    void register(String source) {
        this.queues.put(source, new LinkedBlockingQueue<>());
    }

    @Override
    void send(String source, OutgoingMessageEnvelope envelope) {
        this.queues.get(source).offer(envelope);
    }

    @Override
    void flush(String source) {
    }
}
