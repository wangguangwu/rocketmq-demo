package com.wangguangwu.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 单向发送消息
 *
 * @author wangguangwu
 */
public class OneWayProducer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者 Producer
        DefaultMQProducer producer = new DefaultMQProducer("group2");
        // 设置 NameServer 的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动 Producer 实例
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建消息，并指定 Topic，Tag 和消息体
            Message msg = new Message("TopicTest",
                    "TagB",
                    ("Hello OneWay: " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送单向消息，没有任何返回结果
            producer.sendOneway(msg);

        }
        // 如果不再发送消息，关闭 Producer 实例
        producer.shutdown();
    }
}
