package com.kafkaserver.strategy;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * description
 *
 * @author zhangguoliang
 * @date 2022/8/12 14:58
 */
@Component
public class CustomizePartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        // 获取topic的分区列表
        List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic);
        int partitionCount = partitionInfoList.size();
        Random random = new Random();
        int i = random.nextInt(partitionCount);
        return i ;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}