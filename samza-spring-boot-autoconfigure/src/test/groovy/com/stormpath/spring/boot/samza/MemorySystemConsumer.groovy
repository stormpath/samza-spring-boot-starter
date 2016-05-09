package com.stormpath.spring.boot.samza

import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemStreamPartition

import java.util.concurrent.LinkedBlockingQueue

class MemorySystemConsumer implements SystemConsumer {

    private final MemorySystemProducer producer;

    public MemorySystemConsumer(MemorySystemProducer producer) {
        this.producer = producer;
    }

    @Override
    void start() {
    }

    @Override
    void stop() {
    }

    @Override
    void register(SystemStreamPartition systemStreamPartition, String offset) {
    }

    @Override
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {

        Map<SystemStreamPartition, List<IncomingMessageEnvelope>> found = new HashMap<>();

        for(SystemStreamPartition ssp : systemStreamPartitions) {

            String stream = ssp.getStream()

            LinkedBlockingQueue<OutgoingMessageEnvelope> queue = producer.queues.get(stream)

            List<IncomingMessageEnvelope> messages = new ArrayList<>();

            if (queue != null) {

                OutgoingMessageEnvelope omsg;

                while((omsg = queue.poll()) != null) {
                    IncomingMessageEnvelope imsg =
                            new IncomingMessageEnvelope(ssp, null, omsg.getKey(), omsg.getMessage());
                    messages.add(imsg);
                }
            }

            found.put(ssp, messages);
        }

        return found;
    }
}
