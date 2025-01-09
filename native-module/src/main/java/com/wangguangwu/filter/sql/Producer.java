package com.wangguangwu.filter.sql;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消息生产者
 *
 * @author wangguangwu
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 创建生产者，并指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("sqlGroup");
        // 指定Nameserver地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        for (int i = 0; i < 10; i++) {
            // 创建消息并设置属性
            Message message = new Message("SQLFilterTopic", "TagA", ("Hello SQL Filter " + i).getBytes());
            // 设置自定义属性 "i"
            message.putUserProperty("i", String.valueOf(i));
            // 打印出消息信息
            System.out.println("Sending message: " +
                    "Topic=" + message.getTopic() + ", " +
                    "Tag=" + message.getTags() + ", " +
                    "Body=" + new String(message.getBody()) + ", " +
                    "UserProperties=" + message.getProperties());
            // 发送消息
            producer.send(message);
        }

        // 关闭生产者
        producer.shutdown();
    }
}
