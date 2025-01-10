package com.wangguangwu.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 事务消息消费者
 *
 * @author wangguangwu
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        // 1. 创建消费者，并指定消费者组名称
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TransactionMQConsumerGroup");

        // 2. 指定 Nameserver 地址
        consumer.setNamesrvAddr("localhost:9876");

        // 3. 订阅主题和标签
        consumer.subscribe("TransactionTopic", "*");

        // 4. 注册消息监听器，处理接收到的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * 消费消息逻辑
             *
             * @param msgs 消息列表
             * @param context 消费上下文（如重试次数、队列信息等）
             * @return 消费状态（成功或重试）
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // 遍历消息列表
                for (MessageExt msg : msgs) {
                    // 打印当前线程名称和消息内容
                    System.out.println("Thread=" + Thread.currentThread().getName() +
                            ", Message=" + new String(msg.getBody()));
                }
                // 返回消费成功状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 5. 启动消费者
        consumer.start();
        System.out.println("消费者启动完成");
    }
}
