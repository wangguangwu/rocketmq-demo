package com.wangguangwu.filter.tag;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 消息消费者
 *
 * @author wangguangwu
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        // 1. 创建第一个消费者 Consumer，订阅 Tag1 和 Tag2 的消息
        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer("group1");
        consumer1.setNamesrvAddr("localhost:9876");
        consumer1.subscribe("FilterTagTopic", "Tag1 || Tag2");
        consumer1.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("Consumer1 consumeThread=" + Thread.currentThread().getName() + ", message=" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer1.start();
        System.out.println("Consumer1 started.");

        // 2. 创建第二个消费者 Consumer，订阅所有消息
        // 需要在不同的分组下
        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer("group2");
        consumer2.setNamesrvAddr("localhost:9876");
        consumer2.subscribe("FilterTagTopic", "*");
        consumer2.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("Consumer2 consumeThread=" + Thread.currentThread().getName() + ", message=" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer2.start();
        System.out.println("Consumer2 started.");
    }
}
