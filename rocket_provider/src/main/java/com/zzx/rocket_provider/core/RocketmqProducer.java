package com.zzx.rocket_provider.core;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.UnsupportedEncodingException;

@Slf4j
@Service
@ConfigurationProperties(prefix = "apache.rocketmq.provider")
@Data
public class RocketmqProducer {


    private String group;

    private String namesrvAddr;

    private Integer timeout;

    private Integer retry;

    private DefaultMQProducer producer;

    /**
     * 初始化producer
     */
    @PostConstruct
    public void init() {

        producer = new DefaultMQProducer(group);
        this.producer.setNamesrvAddr(namesrvAddr);
        if (this.timeout != null) {
            this.producer.setSendMsgTimeout(timeout);
        }
        // 如果发送消息失败，设置重试次数，默认为2次
        if (this.retry != null) {
            this.producer.setRetryTimesWhenSendFailed(retry);
        }
//		producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        try {
            this.producer.start();
            log.info("this.producer is start ! groupName:{},namesrvAddr:{}", group, namesrvAddr);
        } catch (MQClientException e) {
            log.info(e.getMessage());
        }
    }

    /**
     * 释放时自动关闭producer
     */
    @PreDestroy
    public void shutDown() {
        this.producer.shutdown();
    }

    /**
     * 发送消息
     *
     * @param msg          map key:sid（主sessionid） key:subOrderId(订单ID)
     * @param sendCallback
     */
    public void send(String msg, String consumerTopic, String tag, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException, UnsupportedEncodingException {
        org.apache.rocketmq.common.message.Message message = new Message(consumerTopic, tag, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
        this.producer.send(message, sendCallback);
    }
}

