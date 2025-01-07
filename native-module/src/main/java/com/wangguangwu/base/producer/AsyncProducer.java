package com.wangguangwu.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 、
 * 发送异步消息
 *
 * @author wangguangwu
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者 Producer
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 设置 NameServer 的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动 Producer 实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < 100; i++) {
            final int index = i;
            // 创建消息，并指定 Topic，Tag 和消息体
            Message msg = new Message("TopicTest",
                    "TagA",
                    ("Hello Async: " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // SendCallback 接收异步返回结果的回调
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    System.out.println("消息发送失败: " + e.getMessage());
                }
            });
        }
    }
}
