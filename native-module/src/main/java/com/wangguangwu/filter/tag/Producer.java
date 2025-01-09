package com.wangguangwu.filter.tag;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消息生产者
 *
 * @author wangguangwu
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 2.指定Nameserver地址
        producer.setNamesrvAddr("localhost:9876");
        // 3.启动producer
        producer.start();

        // 定义要使用的 Tags
        String[] tags = {"Tag1", "Tag2", "Tag3"};
        for (int i = 0; i < 3; i++) {
            // 4.创建消息对象，指定主题Topic、Tag和消息体
            /*
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            String tag = tags[i];
            Message msg = new Message("FilterTagTopic", tag, ("Hello" + tag).getBytes());
            // 5.发送消息
            SendResult result = producer.send(msg);

            System.out.println("发送结果:" + result);

        }

        // 6.关闭生产者producer
        producer.shutdown();
    }
}
