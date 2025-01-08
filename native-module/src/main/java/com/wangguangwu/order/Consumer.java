package com.wangguangwu.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 顺序消费消息
 *
 * @author wangguangwu
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 1.创建消费者 Consumer，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-group");
        // 2.指定 Nameserver 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 3.订阅主题 Topic 和 Tag
        consumer.subscribe("OrderTopic", "*");

        // 4.注册消息监听器
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                // 模拟处理逻辑
                System.out.printf("线程名称：【%s】消费消息：%s%n", Thread.currentThread().getName(), new String(msg.getBody()));
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });

        // 5.启动消费者
        consumer.start();

        System.out.println("消费者启动");
    }
}
