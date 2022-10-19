package com.kafkaserver.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * description
 *
 * @author zhangguoliang
 * @date 2022/9/20 14:35
 */
@Component
public class ListenerErrorHandler {

    @Bean
    public ConsumerAwareListenerErrorHandler myConsumerAwareErrorHandler() {
        return new ConsumerAwareListenerErrorHandler() {
            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException exception,
                                      Consumer<?, ?> consumer) {
                System.out.println("--- 发生消费异常 ---");
                System.out.println(message.getPayload());
                System.out.println(exception);
                return null;
            }
        };
    }
}