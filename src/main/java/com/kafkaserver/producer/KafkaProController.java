package com.kafkaserver.producer;

import com.alibaba.fastjson.JSON;
import com.kafkaserver.config.KafkaSendResultHandler;
import com.kafkaserver.consumer.TopicComponent;
import com.minivision.maiot.common.base.utils.DatePattern;
import com.minivision.maiot.common.base.utils.DateTimeUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * description
 *
 * @author zhangguoliang
 * @date 2022/8/12 11:20
 */
@RestController
@RequestMapping("/kafka/push")
public class KafkaProController {

    private static final Logger log = LoggerFactory.getLogger(KafkaProController.class);
    @Autowired
    @Qualifier(value = "kafkaTemplate")
    private KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    private KafkaSendResultHandler kafkaSendResultHandler;

    @Autowired
    @Qualifier(value = "transactionKafkaTemplate")
    private KafkaTemplate<String,String> transactionKafkaTemplate;
  /*  @Value("${spring.kafka.template.default-topic}")
    private String topic;*/
    /**
    * 简单生产者
    */
    @GetMapping("/normal/sendTopic")
    public String sendMessage1(@RequestParam("msg") String msg) throws ExecutionException, InterruptedException {
        Date date = DateTimeUtil.date();
        String now = DateTimeUtil.format(date, DatePattern.NORM_DATETIME_MS_PATTERN);
        //通过指定key的方式，具有相同key的消息会分发到同一个partition,保证其有序性。
        msg="当前消息为【"+msg+"】当前时间为:"+now;
        System.out.println("发送者:"+msg);
        long timeMillis = System.currentTimeMillis();
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send("kafka-topic-001", msg);
        SendResult<String, String> stringStringSendResult = send.get();
        long currentTimeMillis = System.currentTimeMillis();
        log.info("消耗时间为:{}",currentTimeMillis-timeMillis);
        return "消息发送成功:" + msg;
    }

    /**
     * 简单生产者,消息发送到不同的topic
     */
    @GetMapping("/normal/batchSendTopic")
    public String sendMessage2(@RequestParam("msg") String msg) {
        kafkaTemplate.send("kafka-topic-001", msg);
        kafkaTemplate.send("kafka-topic-002", msg);
        return "消息发送成功:" + msg;
    }

    /**
     * 简单生产者,消息发送到不同的topic
     */
    @GetMapping("/normal/callback")
    public String sendMessage3(@RequestParam("msg") String msg) {
        kafkaTemplate.send("kafka-topic-001", msg).addCallback(success -> {
            String topic = success.getRecordMetadata().topic();
            int partition = success.getRecordMetadata().partition();
            long offset = success.getRecordMetadata().offset();
            ProducerRecord<String, String> producerRecord = success.getProducerRecord();
            log.info("producerRecord is:{}", JSON.toJSONString(producerRecord));
            log.info("消息发送成功:topic is:{},partition is:{},offset is:{}", topic, partition, offset);
        }, failure -> {
            log.error("消息发送失败 error msg:{}", failure.getMessage());
        });
        return "消息发送成功:" + msg;
    }

    /**
    * @Description 异步发送
    * @Author zhangguoliang
    * @Date 2022/9/16
    * @Param [msg]
    * @return java.lang.String
    */
    @GetMapping("/normal/callback1")
    public String sendMessage4(@RequestParam("msg") String msg) {
        kafkaTemplate.send("kafka-topic-002", msg).addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error("消息发送失败 error msg:{}", throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                String topic = result.getRecordMetadata().topic();
                int partition = result.getRecordMetadata().partition();
                long offset = result.getRecordMetadata().offset();
                log.info("消息发送成功:topic is:{},partition is:{},offset is:{}", topic, partition, offset);
            }
        });
        return "消息发送成功:" + msg;
    }

    /*
     *  全局回调设置
     */
    @GetMapping("/normal/globalCallback")
    public String sendMessage5(@RequestParam("msg") String msg) {
        //全局回调设置
        //kafkaTemplate.setProducerListener(kafkaSendResultHandler);
        kafkaTemplate.send("kafka-topic-001", msg);
        return "消息发送成功:" + msg;
    }

    /**
     * 无需事务管理器管理事务-发送消息
     */
    //比如我要发送订单给积分业务处理积分情况，后续业务是扣除金钱余额，但后续扣除的时发现余额不足，
    // 但新增积分消息已经发送出去了，这时候可以采用该方法，要么成功要么都不成功
    @GetMapping("/normal/transaction")
    public void send(@RequestParam String msg) {
        transactionKafkaTemplate.executeInTransaction(kafkaOperations -> {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka-topic-001", msg);
            kafkaOperations.send(producerRecord);
            throw new RuntimeException("fail");
        });
    }

    @GetMapping("/normal/transaction1")
    @Transactional
    public void transactionSend(@RequestParam String msg) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka-topic-001", msg);
        kafkaTemplate.send(producerRecord);
        throw new RuntimeException("fail");
    }

    /**
     * 批量推送数据
     */
    @GetMapping("/normal/batchPushMessage")
    public void batchPushMessage(@RequestParam("pushQuantity") Integer pushQuantity){
        for (int i = 0; i < pushQuantity; i++) {
            String msg = "这是第" + (i + 1) + "条数据";
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka-topic-003", msg);
            kafkaTemplate.send(producerRecord);
        }
    }
    /**
    * 批量发送消息，分配消费者
    */
    @GetMapping("/normal/batchPushMessageStrategy")
    public void batchPushMessageStrategy(@RequestParam("pushQuantity") Integer pushQuantity) throws InterruptedException {
        for (int i = 0; i < pushQuantity; i++) {
            String msg = "【topic1】这是第" + (i + 1) + "条数据";
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic1", msg);
            kafkaTemplate.send(producerRecord);
            msg = "【topic2】这是第" + (i + 1) + "条数据";
            ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>("topic2", msg);
            kafkaTemplate.send(producerRecord1);
        }
        Thread.sleep(3000);
        System.out.println(TopicComponent.map);
    }
//==================================================================================================================
    @GetMapping("/send1")
    public String send1(String msg) {
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic001", "000111", message);
        kafkaTemplate.send("topic001",msg).addCallback(
                success ->{
                    String topic = success.getRecordMetadata().topic();
                    int partition = success.getRecordMetadata().partition();
                    long offset = success.getRecordMetadata().offset();
                    System.out.println("send1:{}================{} topic:" + topic + " partition:" + partition + " offset:" + offset);
                },
                failure ->{
                    String message1 = failure.getMessage();
                    System.out.println(message1);
                }
        );
        return "success";
    }

    @GetMapping("/send2")
    public String send2(String msg) {
        kafkaTemplate.send("topic002","000222",msg).addCallback(
                success ->{
                    String topic = success.getRecordMetadata().topic();
                    int partition = success.getRecordMetadata().partition();
                    long offset = success.getRecordMetadata().offset();
                    System.out.println("send:{}================{} topic:" + topic + " partition:" + partition + " offset:" + offset);
                },
                failure ->{
                    String message1 = failure.getMessage();
                    System.out.println(message1);
                }
        );
        return "success";
    }

    /**
     * 批量发送数据发到kafka队列里
     * @param msgs
     */
    @GetMapping("/send3")
    public void send3(@RequestParam List<String> msgs) {
        msgs.forEach(msg->{
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("radar", msg);
            kafkaTemplate.send(producerRecord);
        });
    }

    @GetMapping("/send4")
    public void sendData(@RequestParam String msg) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("t0", msg);
        kafkaTemplate.send(producerRecord);

    }
    @GetMapping("/send5")
    public void send5(@RequestParam String msg) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("t1", msg);
        kafkaTemplate.send(producerRecord);
    }



    @GetMapping("/send6")
    public void send6() {
        for (int i = 0; i < 10; i++) {
            String msg = "这是第" + (i + 1) + "条数据";
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic001", msg);
            kafkaTemplate.send(producerRecord);
        }
    }
}