package org.manymango.example.component;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.manymango.example.model.Event;

import java.util.Collection;

/**
 * @author lcx
 * @date 2020/11/22 12:42
 */
public class CustomWindow extends WindowAssigner<Event, TimeWindow> {

    @Override
    public Collection<TimeWindow> assignWindows(Event element, long timestamp, WindowAssignerContext context) {
        return null;
    }

    @Override
    public Trigger<Event, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return null;
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
