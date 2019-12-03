package com.zzx.rocket_consumer.core;

import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "apache.rocketmq.consumer")
@Data
@Slf4j
public abstract class Consumer {

    private final static String TOPIC = "runnar_topic_match_update";
    protected DefaultMQPushConsumer defaultMQPushConsumer;
    private String namesrvAddr;
    private int maxThread;
    private int minThread;
    private int MaxSize;
    private String group;
    private Integer timeout;
    private Integer retry;

    @PostConstruct
    private void init() throws Exception {
        defaultMQPushConsumer = new DefaultMQPushConsumer();
        // 设置MQ地址
        defaultMQPushConsumer.setNamesrvAddr(namesrvAddr);
        // 设置不实用vip通道
        defaultMQPushConsumer.setVipChannelEnabled(false);
        // 设置线程数量
        defaultMQPushConsumer.setConsumeThreadMax(maxThread);
        defaultMQPushConsumer.setConsumeThreadMin(minThread);

        init(defaultMQPushConsumer);
    }

    private void init(DefaultMQPushConsumer defaultMQPushConsumer) throws Exception {

        // 默认情况下不需要设置instanceName，rocketmq会使用ip@pid(pid代表jvm名字)作为唯一标示
        // 如果同一个jvm中，不同的producer需要往不同的rocketmq集群发送消息，需要设置不同的instanceName
        // defaultMQPushConsumer.setInstanceName(mqCfg.getInstanceName());
        defaultMQPushConsumer.setConsumerGroup(group);
        defaultMQPushConsumer.subscribe(TOPIC, "");
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(getMaxSize());    //每次拉取10条消息
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() { // 这里可以抽离出来，添加一个继承MessageListenerConcurrently的类
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                if (null != msgs && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {

                        if (TOPIC.equals(msg.getTopic())) {
                            try {
                                //消息体没有sid，或者消息不重复
                                consume(msg, context);

                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                                //消息重试三次，则返回成功，不要在重新发送了
                                if (msg.getReconsumeTimes() == 3) {
                                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                                }
                                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                            }
                        } else {
                            // 如果没有return success, consumer会重新消费该消息, 直到return success
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    }
                    // 如果没有return success, consumer会重新消费该消息, 直到return success
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // Consumer对象在使用之前必须要调用start初始化, 初始化一次即可
        defaultMQPushConsumer.start();
    }


    @PreDestroy
    private void shutDown() {
        log.info("消费者：" + defaultMQPushConsumer.getConsumerGroup() + "关闭");
        defaultMQPushConsumer.shutdown();
    }

    protected abstract void consume(MessageExt msg, ConsumeConcurrentlyContext context) throws Exception;

}
