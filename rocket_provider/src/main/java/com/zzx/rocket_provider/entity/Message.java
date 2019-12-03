package com.zzx.rocket_provider.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class Message<T> implements Serializable {
    private String id;
    private T content;

    @Override
    public String toString() {
        return "{\"id\":" + "\"" + id + "\"" + "," +
                "\"content\":" + content + "}";
    }
}
