package com.wangguangwu.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务消息生产者
 *
 * @author wangguangwu
 */
public class producer {

    public static void main(String[] args) throws Exception {
        // 1. 创建事务消息生产者，并指定生产者组名
        TransactionMQProducer producer = new TransactionMQProducer("TransactionMQProducerGroup");

        // 2. 设置 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");

        // 3. 设置事务监听器，用于执行本地事务和处理事务回查
        producer.setTransactionListener(new TransactionListener() {

            /**
             * 执行本地事务逻辑
             *
             * @param msg 消息对象
             * @param arg 业务参数（可选）
             * @return 本地事务状态（提交、回滚、未知）
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                // 根据消息的标签执行不同的事务逻辑
                if (StringUtils.equals("TAGA", msg.getTags())) {
                    // 模拟事务执行成功，提交消息
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("TAGB", msg.getTags())) {
                    // 模拟事务执行失败，回滚消息
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else if (StringUtils.equals("TAGC", msg.getTags())) {
                    // 模拟事务状态不确定，等待回查
                    return LocalTransactionState.UNKNOW;
                }
                // 默认返回未知状态
                return LocalTransactionState.UNKNOW;
            }

            /**
             * MQ 服务器回查事务状态时调用
             *
             * @param msg 消息对象
             * @return 本地事务状态（提交、回滚、未知）
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                // 打印回查信息，用于分析消息的事务状态
                System.out.println("事务回查，消息的Tag: " + msg.getTags());
                // 模拟直接提交事务
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        // 4. 启动生产者
        producer.start();

        // 定义消息标签
        String[] tags = {"TAGA", "TAGB", "TAGC"};

        // 5. 发送事务消息
        for (int i = 0; i < tags.length; i++) {
            // 创建消息对象，指定主题、标签和消息内容
            Message msg = new Message("TransactionTopic", tags[i], ("Hello World " + i).getBytes());

            // 使用事务方式发送消息
            SendResult result = producer.sendMessageInTransaction(msg, null);

            // 打印发送结果
            System.out.println("发送结果: " + result);
        }
    }
}
