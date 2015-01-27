flume-ng-kafka-sink
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.7.1](http://kafka.apache.org/07/quickstart.html).

Configuration of Kafka Sink
----------

    agent_log.sinks.sink_test.type = com.sink.kafka.KafkaSink
    agent_log.sinks.sink_test.batchSize = 100
    agent_log.sinks.sink_test.isJson = true
    agent_log.sinks.sink_test.eventEncode = GBK
    #kafka configuration
    agent_log.sinks.sink_test.topic = test_topic
    agent_log.sinks.sink_test.zk.connect = 127.0.0.1:2181
    agent_log.sinks.sink_test.producer.type = async
    agent_log.sinks.sink_test.serializer.class = kafka.serializer.StringEncoder


isJson = true
----------
    {"timestamp":1422359573462,"host":"127.0.0.1","topic":"test_topic","path":"/logs/path","body":"logs body"}
    
