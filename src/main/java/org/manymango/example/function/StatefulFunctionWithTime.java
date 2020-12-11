package org.manymango.example.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lcx
 * @date 2020/11/21
 **/
public class StatefulFunctionWithTime extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Object> {

    private transient ListState<Tuple2<String, Integer>> listState;

    private long time;


    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Object> out) throws Exception {
        listState.add(value);
        out.collect(value);
        System.out.println(listState.get().toString());

        ctx.timerService().registerEventTimeTimer(System.currentTimeMillis()+5);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("KeyedProcessFunction open");
        ListStateDescriptor<Tuple2<String, Integer>> stateDescriptor = new ListStateDescriptor<>("listState", Types.TUPLE(Types.STRING, Types.INT));
        listState = getRuntimeContext().getListState(stateDescriptor);
        time = System.currentTimeMillis();
    }






    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        if (timestamp - time > 3000) {
            System.out.println("time trigger");
        } else {
            System.out.println("time not trigger");
        }
    }



}
