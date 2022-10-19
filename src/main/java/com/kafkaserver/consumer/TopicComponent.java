package com.kafkaserver.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * description
 *
 * @author zhangguoliang
 * @date 2022/8/12 11:25
 */
@Component
public class TopicComponent {
    private static final Logger log = LoggerFactory.getLogger(TopicComponent.class);
    public static Map<String, Set<String>> map=new HashMap<>();
    @Value("${kafka.topic.group:test_topic001_topic002}")
    private String topic001002;

    /**
    * 简单消费
    */
    @KafkaListener(id="timingConsumer",groupId = "kafka-topic-001",topics = "kafka-topic-001",containerFactory = "singleFactory")
//    @SendTo("kafka-topic-002")
    public String getMeessage(ConsumerRecord<String, String> record,Consumer consumer,Acknowledgment acknowledgment) {
        int partition = record.partition();
        String topic = record.topic();
        String msg = record.value();
        long offset = record.offset();
        System.out.println("消费者[getMeessage]接受消息：topic-->" + topic + ",partition->>" + partition + ",offset->>" + offset + "msg->>" + msg+"发送到kafka-topic-002");
        consumer.commitSync();
        return "topic2-received msg is:"+msg;
    }

    /**
     * 简单消费
     */
//    @KafkaListener(groupId = "subscribeGroup",topics = "kafka-topic-001",containerFactory = "singleFactory")
//    @SendTo("kafka-topic-002")
    public String getMeessagesSubscribe(ConsumerRecord<String, String> record,Consumer consumer,Acknowledgment acknowledgment) {
        int partition = record.partition();
        String topic = record.topic();
        String msg = record.value();
        long offset = record.offset();
        System.out.println("消费者[getMeessagesSubscribe]接受消息：topic-->" + topic + ",partition->>" + partition + ",offset->>" + offset + "msg->>" + msg);
//        acknowledgment.acknowledge();
        consumer.commitSync();
        return "received msg is:"+msg;
    }
    @KafkaListener(groupId = "kafka-topic",topics = {"kafka-topic-002"},containerFactory = "singleFactory")
    public void getBatchMeessage(ConsumerRecord<String, String> record,Acknowledgment acknowledgment) {
        int partition = record.partition();
        String topic = record.topic();
        String msg = record.value();
        long offset = record.offset();
        acknowledgment.acknowledge();
        System.out.println("消费者[getBatchMeessage]接受消息：topic-->" + topic + ",partition->>" + partition + ",offset->>" + offset + ",msg->>" + msg);
    }
    //1.过滤消费
//    @KafkaListener(groupId = "kafka-topic",topics = "kafka-topic-003",containerFactory = "filterContainerFactory2",errorHandler = "myConsumerAwareErrorHandler")
    //2.并发消费
    @KafkaListener(groupId = "kafka-topic",topics = "kafka-topic-003",containerFactory = "batchFactory",errorHandler = "myConsumerAwareErrorHandler")
    //3.initialOffset初始消费偏移量,初始偏移量的意思是每次服务重启时，重新从初始偏移量的地方下拉数据再次消费，与提交无关
//    @KafkaListener(groupId = "kafka-topic-005",topicPartitions =
//            {@TopicPartition(topic = "kafka-topic-005",
//                    partitionOffsets = {@PartitionOffset(partition = "0",initialOffset = "5"),@PartitionOffset(partition = "1",initialOffset = "5")})},containerFactory = "batchFactory")
   /* @KafkaListener(groupId = "kafka-topic-003",topicPartitions =
            {@TopicPartition(topic = "kafka-topic-003",partitions = {"0","1","2"})},containerFactory = "batchFactory")*/
    public void consurmTopic003(List<ConsumerRecord<String,String>> consumerRecords,Consumer consumer,Acknowledgment acknowledgment){
        log.info("consumerRecords size is:{}",consumerRecords.size());
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            log.info("【当前线程thredName is】:{},【offset is】:{},【msg is】:{},【topic is】:{},【patition is】:{}",Thread.currentThread().getName(),
                    consumerRecord.offset(),consumerRecord.value(),
                    consumerRecord.topic(),consumerRecord.partition());
        }
        //手动异步提交
        acknowledgment.acknowledge();
    }

//    @KafkaListener(groupId = "kafka-topic",topics = "kafka-topic-004",containerFactory = "singleFactory")
    public void consurmTopic004(String record,Consumer consumer){
        System.out.println("我是【kafka-topic-004】->msg:"+record);
        //手动异步提交
        consumer.commitAsync();
    }
    //****************************以下是测试消费者分区分配策略展示，分区分配策略分为:RoundRobin(轮询)，RangeRobin（Range），StickyAssignor（粘性）********************************
    @KafkaListener(groupId = "topic",topics = {"topic1","topic2"},containerFactory = "batchFactory")
    public void comsurm01(List<ConsumerRecord<String,String>> consumerRecords,Acknowledgment acknowledgment){
        Set<String> hashSet = new HashSet<>();
        try {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("消费者01【topic is】:{},【partition is】:{},【msg is】:{}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.value());
                hashSet.add(consumerRecord.topic()+"-p"+consumerRecord.partition());
                System.out.println("c1:"+"【"+consumerRecord.topic()+"-p"+consumerRecord.partition()+"】");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            map.put("c1",hashSet);
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(groupId = "topic",topics = {"topic1","topic2"},containerFactory = "batchFactory")
    public void comsurm02(List<ConsumerRecord<String,String>> consumerRecords,Acknowledgment acknowledgment){
        Set<String> hashSet = new HashSet<>();
        try {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("消费者01【topic is】:{},【partition is】:{},【msg is】:{}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.value());
                hashSet.add(consumerRecord.topic()+"-p"+consumerRecord.partition());
                System.out.println("c2:"+"【"+consumerRecord.topic()+"-p"+consumerRecord.partition()+"】");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            map.put("c2",hashSet);
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(groupId = "topic",topics = {"topic1","topic2"},containerFactory = "batchFactory")
    public void comsurm03(List<ConsumerRecord<String,String>> consumerRecords,Acknowledgment acknowledgment){
        Set<String> hashSet = new HashSet<>();
        try {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("消费者01【topic is】:{},【partition is】:{},【msg is】:{}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.value());
                hashSet.add(consumerRecord.topic()+"-p"+consumerRecord.partition());
                System.out.println("c3:"+"【"+consumerRecord.topic()+"-p"+consumerRecord.partition()+"】");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            map.put("c3",hashSet);
            acknowledgment.acknowledge();
        }

    }
}