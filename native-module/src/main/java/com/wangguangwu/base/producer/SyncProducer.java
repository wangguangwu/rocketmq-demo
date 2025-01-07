package com.wangguangwu.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 发送同步消息
 *
 * @author wangguangwu
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者 Producer
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 设置 NameServer 的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动 Producer 实例
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("TopicTest",
                    "TagA",
                    ("Hello Sync: " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送消息到一个 Broker
            SendResult sendResult = producer.send(msg);
            // 通过 sendResult 返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }
        // 如果不再发送消息，关闭 Producer 实例
        producer.shutdown();
    }
}
