package org.manymango.example.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.manymango.example.model.Event;

/**
 * @author lcx
 * @date 2020/12/02
 **/
public class MyRichWindowFunction extends RichWindowFunction<Event, Event, String, TimeWindow> implements CheckpointedFunction {

    private transient MapState<String, Event> mapState;

    @Override
    public void apply(String s, TimeWindow window, Iterable<Event> input, Collector<Event> out) throws Exception {
        for (Event event : input) {
            mapState.put(event.getName(), event);
            out.collect(event);
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        MapStateDescriptor<String, Event> descriptor = new MapStateDescriptor<String, Event>("mapState", String.class, Event.class);
        mapState = context.getKeyedStateStore().getMapState(descriptor);
    }



}
