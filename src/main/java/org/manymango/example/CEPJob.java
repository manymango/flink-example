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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.adaptors.PatternFlatSelectAdapter;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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
public class CEPJob {

	/**
     * 运行nc -L -p 9999 ，打开端口
	 */
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> dataStreamSource = env.socketTextStream("", 9999, "\n");
		Pattern<String, ?> pattern = Pattern.<String>begin("start")
				.where(new SimpleCondition<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return "xixi".equals(value) || "haha".equals(value);
					}
				}).next("middle").subtype(String.class).where(new SimpleCondition<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return "xixi".equals(value);
					}
				}).followedBy("end").where(new SimpleCondition<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return "xixi".equals(value);
					}
				});

		PatternStream<String> patternStream = CEP.pattern(dataStreamSource, pattern);

		DataStream<String> result = patternStream.process(new PatternProcessFunction<String, String>() {

			@Override
			public void processMatch(Map<String, List<String>> match, Context ctx, Collector<String> out) throws Exception {
				System.out.println(match);
				out.collect("1111");
			}
		});

		result.print();


		env.execute("Flink Streaming Java API Skeleton");
	}
}
