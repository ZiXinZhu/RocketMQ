package com.zzx.rocket_provider.controller;


import com.alibaba.fastjson.JSONObject;
import com.zzx.rocket_provider.core.RocketTopics;
import com.zzx.rocket_provider.core.RocketmqProducer;
import com.zzx.rocket_provider.entity.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@Slf4j
public class RocketController {

    @Autowired(required = false)
    RocketmqProducer producer;

    @GetMapping("/test")
    public void sendMs() {

        Message<JSONObject> message=new Message<>();
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("username","admin");
        jsonObject.put("password","123456");
        message.setId(String.valueOf(UUID.randomUUID()));
        message.setContent(jsonObject);
        // 发送赛事更新消息MQ
        try {

            producer.send(message.toString(), RocketTopics.getTopicOne(), "", new SendCallback() {

                @Override
                public void onSuccess(SendResult sendResult) {
                   log.info("成功");
                }

                @Override
                public void onException(Throwable e) {
                    log.info(e.getMessage());
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            log.info(e.getMessage());
            e.printStackTrace();
        }

    }
}
