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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSinkTest {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSinkTest.class);

	private KafkaSink sink;

	@Before
	public void setup() throws Exception {
		sink = new KafkaSink();
		sink.setChannel(new MemoryChannel());

	}

	@After
	public void tearDown() throws Exception {
		sink.stop();
	}

//	@Test
	@Ignore
	public void testProcess() throws EventDeliveryException, InterruptedException {
		Context context = new Context();
		context.put("zk.connect", "127.0.0.1:2181");
		context.put("topic", "test");
		context.put("batchSize", "2");
		context.put("isJson","false");

		Configurables.configure(sink, context);
		Channel channel = new PseudoTxnMemoryChannel();
		Configurables.configure(channel, context);
		sink.setChannel(channel);
		sink.start();

		for (int i = 0; i < 10; i++) {
			Event event = new SimpleEvent();
			event.setBody(("Test event " + i).getBytes());
			channel.put(event);
			sink.process();
			Thread.sleep(200);
		}
	}

}
