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

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.manymango.example.model.Event;

import java.util.List;
import java.util.Map;
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
public class CEPStreaming {

	/**
     * 运行nc -L -p 9999 ，打开端口  win
	 * nc -lk 9999     mac
	 */
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999, "\n");

		SingleOutputStreamOperator<Event> partitionedInput = dataStreamSource.filter(Objects::nonNull)
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
				}).returns(Event.class);

		// 先点击浏览商品，然后将商品加入收藏
		Pattern<Event, ?> pattern = Pattern.<Event>begin("firstly")
				.where(new SimpleCondition<Event>() {
					@Override
					public boolean filter(Event event) throws Exception {
						// 点击商品
						return event.getName().startsWith("a");
					}
				})
				.followedByAny("and")
				.where(new SimpleCondition<Event>() {
					@Override
					public boolean filter(Event event) throws Exception {
						// 将商品加入收藏
						return event.getName().startsWith("b");
					}
				}).oneOrMore()
				.followedByAny("end")
				.where(new SimpleCondition<Event>() {
					@Override
					public boolean filter(Event event) throws Exception {
						// 将商品加入收藏
						return event.getName().startsWith("c");
					}
				});


		PatternStream<Event> patternStream = CEP.pattern(partitionedInput, pattern);

		patternStream.process(new PatternProcessFunction<Event, String>() {
			@Override
			public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {

				out.collect(match.toString());
			}
		}).print();

		env.execute("Flink Streaming Java API Skeleton");
	}


}
