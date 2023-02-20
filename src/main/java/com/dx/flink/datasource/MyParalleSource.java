package com.dx.flink.datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MyParalleSource implements ParallelSourceFunction<Long> {

    private long number = 1L;
    private boolean isRunning = false;
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(number);
            number++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
