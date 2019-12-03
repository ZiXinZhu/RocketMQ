package com.zzx.rocket_consumer.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class RocketListener extends Consumer {
    @Override
    protected void consume(MessageExt msg, ConsumeConcurrentlyContext context) throws Exception {
        //获取返回的字符串
        String result = new String(msg.getBody());
        JSONObject jsonObject = JSON.parseObject(result);
        String id=jsonObject.getString("id");
        JSONObject json=JSON.parseObject(jsonObject.getString("content"));
        log.info("收到*******" + id);
        log.info("内容*******"+json);
    }

}
