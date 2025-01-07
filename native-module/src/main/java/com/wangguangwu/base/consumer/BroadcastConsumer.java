package com.wangguangwu.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * 广播消费者
 *
 * @author wangguangwu
 */
public class BroadcastConsumer {

    public static void main(String[] args) throws Exception {
        // 创建消费者实例，指定 Consumer Group 名称
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group2");
        // 设置 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 设置消费模式为广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 订阅指定的 Topic 和 Tag
        consumer.subscribe("TopicTest", "TagB");

        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                System.out.printf("BroadcastConsumer - Received message: %s%n", new String(message.getBody()));
            }
            // 消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 启动消费者
        consumer.start();
        System.out.println("BroadcastConsumer started...");
    }
}
