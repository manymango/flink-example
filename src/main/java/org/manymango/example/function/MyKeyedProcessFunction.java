package org.manymango.example.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.manymango.example.model.Event;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lcx
 * @date 2020/12/11
 **/
public class MyKeyedProcessFunction extends KeyedProcessFunction<String, Event, Event> {

    private int i;

    private static Map<String, String> map = new HashMap<>();

    private transient ValueState<Integer> valueState;

    private transient MapState<String, String> mapState;


    @Override
    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
        this.map(value, ctx, out);

    }


    private void i(Event value, Context ctx, Collector<Event> out) {
        if (value.getType() == 1) {
            i++;
        } else {
            System.out.println("i value " + i);
        }
    }


    private void valueSate(Event value, Context ctx, Collector<Event> out) throws IOException {
        if (value.getType() == 1) {
            Integer num = valueState.value();
            if (num==null) {
                valueState.update(1);
            } else {
                valueState.update(num+1);
            }
        } else {
            System.out.println("state value " + valueState.value());
        }
    }

    private void map(Event value, Context ctx, Collector<Event> out) throws IOException {
        if (value.getType() == 1) {
            String uuid = UUID.randomUUID().toString();
            map.put(uuid, uuid);
        } else {
            System.out.println("map size " + map.size());
        }
    }

    private void mapState(Event value, Context ctx, Collector<Event> out) throws Exception {
        if (value.getType() == 1) {
            String uuid = UUID.randomUUID().toString();
            mapState.put(uuid, uuid);
        } else {
            Iterator<String> iterator = mapState.keys().iterator();
            int size = 0;
            while (iterator.hasNext()) {
                iterator.next();
                size++;
            }
            System.out.println("state map size " + size);
        }
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Integer.class);
        valueState = getRuntimeContext().getState(valueStateDescriptor);

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("mapState", String.class, String.class);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

}
