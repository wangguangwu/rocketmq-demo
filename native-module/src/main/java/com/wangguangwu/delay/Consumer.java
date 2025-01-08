package com.wangguangwu.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 延时消息消费者
 *
 * @author wangguangwu
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        // 1.创建消费者 Consumer，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        // 2.指定 Nameserver 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 3.订阅主题 Topic 和 Tag
        consumer.subscribe("DelayTopic", "*");

        // 4.设置回调函数，处理消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("消息ID：【" + msg.getMsgId() + "】,延迟时间：" + (System.currentTimeMillis() - msg.getStoreTimestamp()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 5.启动消费者consumer
        consumer.start();

        System.out.println("消费者启动");
    }
}
