/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.manymango.example;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.manymango.example.model.Event;

import java.util.Objects;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class BroadcastStreaming {

	/**
	 * 运行nc -L -p 9999 ，打开端口
	 */
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999, "\n");

		final MapStateDescriptor<String, Event> CONFIG_DESCRIPTOR = new MapStateDescriptor<>("wordsConfig", String.class, Event.class);

		BroadcastStream<Event> broadCastStream = env.socketTextStream("localhost", 8888, "\n")
				.map(s -> {
					// 输入的string，逗号分隔，第一个字段为用户名，第二个字段为事件类型
					String[] strings = s.split(",");
					if (strings.length != 2) {
						return null;
					}
					Event event = new Event();
					event.setName(strings[0]);
					event.setType(Integer.parseInt(strings[1]));
					return event;
				}).returns(Event.class)
				.filter(s -> s.getName().equals("broadcast"))
				.broadcast(CONFIG_DESCRIPTOR);

		SingleOutputStreamOperator<Event> input = dataStreamSource.filter(Objects::nonNull)
				.map(s -> {
					// 输入的string，逗号分隔，第一个字段为用户名，第二个字段为事件类型
					String[] strings = s.split(",");
					if (strings.length != 2) {
						return null;
					}
					Event event = new Event();
					event.setName(strings[0]);
					event.setType(Integer.parseInt(strings[1]));
					return event;
				}).returns(Event.class)
				.filter(s -> !s.getName().equals("broadcast"));


		input.keyBy(Event::getName)
				.connect(broadCastStream)
				.process(new KeyedBroadcastProcessFunction<Object, Event, Event, Object>() {

					private transient MapState<String, Event> eventMapState;

					@Override
					public void processElement(Event value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
						eventMapState.put(System.currentTimeMillis()+"", value);
						System.out.println(eventMapState.values().toString());
						ReadOnlyBroadcastState<String, Event> readOnlyBroadcastState = ctx.getBroadcastState(CONFIG_DESCRIPTOR);
					}

					@Override
					public void processBroadcastElement(Event value, Context ctx, Collector<Object> out) throws Exception {
						System.out.println("  "+value.toString());
						BroadcastState<String, Event> broadcastState = ctx.getBroadcastState(CONFIG_DESCRIPTOR);
						broadcastState.put(value.getName(), value);
					}

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);
						eventMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Event>("eventMapState", String.class, Event.class));
					}
				}).setParallelism(1);


		env.execute("Flink Streaming Java API Skeleton");
	}
}
