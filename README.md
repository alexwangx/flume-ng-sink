flume-ng-kafka-sink
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with [kafka 0.7.1](http://kafka.apache.org/07/quickstart.html).

Configuration of Kafka Sink
=============
| Property Name | Default   | Description         |
| ------------- | :-----:   | :----------         |
| Type          |    -      |                     |
| batchSize     |    100    | The max number of events to send to the kafka|
| isJson        |   true    | format event to json    |
| eventEncode   |   UTF-8   | event encoding |


* Example [kafka configuration](https://kafka.apache.org/08/configuration.html)
```
    agent_log.sinks.sink_test.type = com.sink.kafka.KafkaSink
    agent_log.sinks.sink_test.batchSize = 100
    agent_log.sinks.sink_test.isJson = true
    agent_log.sinks.sink_test.eventEncode = GBK
    agent_log.sinks.sink_test.metadata.broker.list = 127.0.0.1:9092
    agent_log.sinks.sink_test.producer.type = async
    agent_log.sinks.sink_test.serializer.class = kafka.serializer.StringEncoder
```

isJson = true
----------
    {"timestamp":1422359573462,"host":"127.0.0.1","topic":"test_topic","path":"/logs/path","body":"logs body"}
