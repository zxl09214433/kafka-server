package com.kafkaserver.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * description
 *
 * @author zhangguoliang
 * @date 2022/8/15 10:06
 */
@Configuration
public class KafkaConfiguration {


    /**
    * 1.初始化topic 分区数量
    */
    @Bean
    public NewTopic topic001() {
        return new NewTopic("kafka-topic-001", 4, (short) -1);
    }
    @Bean
    public NewTopic topic002(){
        return new NewTopic("kafka-topic-002",3,(short) -1);
    }
    @Bean
    public NewTopic topic003(){
        return new NewTopic("kafka-topic-003",3, (short) -1);
    }

    @Bean
    NewTopic topic004() {
        return new NewTopic("topic1", 4, (short) -1);
    }

    @Bean
    NewTopic topic005() {
        return new NewTopic("topic2", 4, (short) -1);
    }
    @Bean
    NewTopic topic006() {
        return new NewTopic("kafka-topic-005", 4, (short) -1);
    }

/*    @Value("${spring.kafka.template.default-topic}")
    private String topic;

    @Value("${spring.kafka.template.patitions}")
    private Integer patitions;

    @Value("${spring.kafka.template.replications}")
    private Short replications;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPatitions() {
        return patitions;
    }

    public void setPatitions(Integer patitions) {
        this.patitions = patitions;
    }

    public Short getReplications() {
        return replications;
    }

    public void setReplications(Short replications) {
        this.replications = replications;
    }*/
    /**
     * 项目启动时，自动创建topic，指定分区和副本数量
     * @return Topic
     */
   /* @Bean
    public NewTopic topic() {
        return new NewTopic("radar", 7, (short) -1);
    }
    @Bean
    public NewTopic topic001(){
        return new NewTopic("topic001",5,(short) -1);
    }
    @Bean
    public NewTopic topic002(){
        return new NewTopic("topic002",5,(short) -1);
    }*/
}