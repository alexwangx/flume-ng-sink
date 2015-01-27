package com.sink.kafka;

import com.alibaba.fastjson.annotation.JSONField;

public class FormatEvent {
    @JSONField(ordinal = 1)
    private long timestamp;

    @JSONField(ordinal = 2)
    private String host;

    @JSONField(ordinal = 3)
    private String topic;

    @JSONField(ordinal = 4)
    private String path;

    @JSONField(ordinal = 5)
    private String body;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
