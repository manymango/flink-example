package org.manymango.example;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.manymango.example.function.CustomSource;
import org.manymango.example.function.MyKeyedProcessFunction;
import org.manymango.example.model.Event;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lcx
 * @date 2020/12/11
 **/
public class CustomSourceStreamJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> stream1 =  env.addSource(new CustomSource())
                .setParallelism(2)
                .filter(Objects::nonNull);
        DataStream<Event> stream2 =  env.addSource(new CustomSource())
                .setParallelism(2)
                .filter(Objects::nonNull);
        DataStream<Event> stream3 =  env.addSource(new CustomSource())
                .setParallelism(2)
                .filter(Objects::nonNull);
        DataStream<Event> stream4 =  env.addSource(new CustomSource())
                .setParallelism(2)
                .filter(Objects::nonNull);

        DataStream<Event> dataStream = stream1.union(stream2).union(stream3).union(stream4)
                .process(new ProcessFunction<Event, Event>() {
                    private int i;

                    private Map<String, Integer> map = new ConcurrentHashMap<>();
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                        String threadName = Thread.currentThread().getName();
                        if (value.getType() ==1) {
                            map.put(threadName, 1);
                            i++;
                        } else {
                            System.out.println("i value " + i +" " + map);
                        }
                    }
                }).setParallelism(1);





        dataStream.print();



		env.execute();
    }
}
