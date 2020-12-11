package org.manymango.example.function;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.manymango.example.model.Event;

/**
 * @author lcx
 * @date 2020/12/11
 **/
public class CustomSource extends RichParallelSourceFunction<Event> {

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        int index = 0;
        while (index < 1000000) {
            index++;
            Event event = new Event();
            event.setName("x");
            event.setType(1);
            ctx.collect(event);
        }
        Thread.sleep(10000);
        Event event = new Event();
        event.setName("x");
        event.setType(2);
        ctx.collect(event);
        System.out.println("total event "+index);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Override
    public void cancel() {

    }

}
