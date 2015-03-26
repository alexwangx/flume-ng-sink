/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package com.sink.kafka;

import com.alibaba.fastjson.JSON;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class KafkaSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private SinkCounter sinkCounter;
    private Producer<String, String> producer;
    private int batchSize;
    private String eventEncode;
    private boolean isJson;
    private Map<String, ArrayList<String>> map = new HashMap();
    private Map<String, Long> timeoutMap = new HashMap();

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        Status result = Status.READY;
        Event event;
        String msg = null;
        String topic = null;
        try {
            tx.begin();
            event = channel.take();
            if (event != null) {
                sinkCounter.incrementEventDrainAttemptCount();
                topic = event.getHeaders().get("topic");
                if (isJson) {
                    FormatEvent formatEvent = new FormatEvent();
                    formatEvent.setBody(new String(event.getBody(), eventEncode));
                    formatEvent.setHost(event.getHeaders().get("hostip"));
                    formatEvent.setTimestamp(Long.parseLong(event.getHeaders().get("timestamp")));
                    formatEvent.setPath(event.getHeaders().get("path"));
                    formatEvent.setTopic(topic);
                    msg = JSON.toJSONString(formatEvent);
                } else {
                    msg = new String(event.getBody(), eventEncode);
                }
            } else {
                result = Status.BACKOFF;
            }
            dealWithLog(msg,topic);
            tx.commit();
            return result;
        } catch (Exception e) {
            try {
                tx.rollback();
                return Status.BACKOFF;
            } catch (Exception e2) {
                logger.error("kafka sink process ...  Rollback Exception:{}", e2);
            }
            logger.error("kafka sink process ...  KafkaSink Exception:{}", e);
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    private void dealWithLog(String msg,String topic) {
        if(msg == null || topic == null) return;
        if (map.containsKey(topic)) {
            if (map.get(topic).size() < 1) {
                timeoutMap.put(topic,System.currentTimeMillis());
            }
            map.get(topic).add(msg);
        }else {
            ArrayList list = new ArrayList();
            list.add(msg);
            map.put(topic, list);
            timeoutMap.put(topic,System.currentTimeMillis());
        }

        if (map.get(topic).size() >= batchSize || (System.currentTimeMillis() - timeoutMap.get(topic).longValue() > 5000)) {
            producer.send(new ProducerData<String, String>(topic, map.get(topic)));
            sinkCounter.addToEventDrainSuccessCount(map.get(topic).size());
            map.put(topic, new ArrayList());
        }

    }

    @Override
    public void configure(Context context) {
        this.batchSize = context.getInteger("batchSize", 100);
        this.eventEncode = context.getString("eventEncode", "UTF-8");
        this.isJson = context.getBoolean("isJson", true);
        producer = KafkaSinkUtil.getProducer(context);

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        sinkCounter.stop();
        super.stop();
    }
}
