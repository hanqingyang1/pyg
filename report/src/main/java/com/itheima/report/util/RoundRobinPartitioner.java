package com.itheima.report.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartitioner implements Partitioner {

    AtomicInteger counter = new AtomicInteger(0);
    @Override
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        Integer partitions = cluster.partitionCountForTopic(topic);
        int cruPartition = counter.incrementAndGet() % partitions;
        if(counter.get() > 65535){
            counter.set(0);
        }
        return cruPartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
