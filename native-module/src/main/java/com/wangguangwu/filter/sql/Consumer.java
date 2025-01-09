package com.wangguangwu.filter.sql;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 消息消费者
 *
 * @author wangguangwu
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 创建消费者，并指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("sqlGroup");
        // 指定Nameserver地址
        consumer.setNamesrvAddr("localhost:9876");

        // 使用 SQL92 表达式过滤消息
        // 只消费属性 "i" 大于 5 的消息
        consumer.subscribe("SQLFilterTopic", MessageSelector.bySql("i > 5"));

        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("Received message: " + new String(msg.getBody()) +
                        ", UserProperty[i]: " + msg.getUserProperty("i"));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 启动消费者
        consumer.start();
        System.out.println("SQL Consumer started.");
    }
}
