package com.kafkaserver.config;

import com.kafkaserver.producer.KafkaProController;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * description 全局定义回调
 *
 * @author zhangguoliang
 * @date 2022/9/15 16:16
 */
@Component
public class KafkaSendResultHandler implements ProducerListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaProController.class);

    @Override
    public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
        String topic = recordMetadata.topic();
        int partition = recordMetadata.partition();
        long offset = recordMetadata.offset();
        log.info("消息发送成功:topic is:{},partition is:{},offset is:{}", topic, partition, offset);
    }

    @Override
    public void onError(ProducerRecord producerRecord, Exception exception) {
        log.error("消息发送失败 error msg:{}", exception.getMessage());
    }
}