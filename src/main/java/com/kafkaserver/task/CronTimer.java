package com.kafkaserver.task;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * description
 *
 * @author zhangguoliang
 * @date 2022/9/21 11:30
 */
@EnableScheduling
@Component
public class CronTimer {

    /**
     * @KafkaListener注解所标注的方法并不会在IOC容器中被注册为Bean，
     * 而是会被注册在KafkaListenerEndpointRegistry中，
     * 而KafkaListenerEndpointRegistry在SpringIOC中已经被注册为Bean
     **/
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplateTest;

    /**
     * 定时启动监听器,用来消费某一时间段的数据
     * @param
     * @author yh
     * @date 2022/5/11
     * @return
     */
    @Scheduled(cron = "0 51 19 * * ?")
    public void startListener() {
        System.out.println("启动监听器..." + new Date());
        // "timingConsumer"是@KafkaListener注解后面设置的监听器ID,标识这个监听器
        if (!registry.getListenerContainer("timingConsumer").isRunning()) {
            registry.getListenerContainer("timingConsumer").start();
        }
        //通过指定key的方式，具有相同key的消息会分发到同一个partition,保证其有序性。
//        registry.getListenerContainer("timingConsumer").resume();
    }

    /**
     * 定时停止监听器
     * @param
     * @author yh
     * @date 2022/5/11
     * @return
     */
    @Scheduled(cron = "0 54 19 * * ?")
    public void shutDownListener() {
        System.out.println("关闭监听器..." + new Date());
        registry.getListenerContainer("timingConsumer").pause();
    }

}