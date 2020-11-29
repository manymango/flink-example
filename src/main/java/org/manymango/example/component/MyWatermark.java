package org.manymango.example.component;

import org.apache.flink.api.common.eventtime.*;
import org.manymango.example.model.Event;

/**
 * @author lcx
 * @date 2020/11/14 22:35
 */
public class MyWatermark implements WatermarkStrategy<Event> {

    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.getTimestamp();
            }
        };
    }



    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Event>() {

            private long maxTimestamp;
            private long delay = 3000;

            @Override
            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                maxTimestamp = Math.max(maxTimestamp, event.getTimestamp());
                watermarkOutput.emitWatermark(new Watermark(maxTimestamp));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(maxTimestamp));
            }



        };
    }


}
