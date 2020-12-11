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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.scala.DataSet;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class WordCountBatch {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String filePath = Thread.currentThread().getContextClassLoader().getResource("wordcount.txt").getPath();
		DataSource<String> dataSource = env.readTextFile(filePath);
		dataSource.flatMap((FlatMapFunction<String, String>) (value, out) -> {
			String[] strings= value.toLowerCase().split(",");
			for (String str : strings) {
				out.collect(str);
			}
		}).returns(new GenericTypeInfo<>(String.class))
				.map(s -> {
					Object a = new Tuple2(s , 1);
					return a;
				})
				.returns(Object.class)
				.map(s -> (Tuple2)s)
				.returns(Types.TUPLE(Types.STRING, Types.INT))
				.groupBy(0)
				.sum(1)
				.print();

		DataSet<String> dataSet = null;
		dataSet.groupBy(null);

		env.execute("Flink Batch Java API Skeleton");
	}
}
