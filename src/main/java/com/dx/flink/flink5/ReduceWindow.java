package com.dx.flink.flink5;

import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class ReduceWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //需要设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.6.102", 7777);
        SingleOutputStreamOperator<Integer> intDStream = dataStream
                .map(number -> Integer.valueOf(number));
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(10));
        windowResult.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer last, Integer current) throws Exception {
                System.out.println("窗口数据"+last + " "+current);
                return last+current;
            }
        }).print();

        env.execute(ReduceWindow.class.getSimpleName());
    }
}