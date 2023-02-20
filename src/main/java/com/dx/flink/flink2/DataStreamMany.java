package com.dx.flink.flink2;

import com.dx.flink.datasource.MyParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamMany {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> numberStream = env.addSource(new MyParalleSource()).setParallelism(2);
        SingleOutputStreamOperator<Long> dataStream = numberStream.map(new MapFunction<Long, Long>() {
                   @Override
                   public Long map(Long value) throws Exception {
                       System.out.println("接受到了数据："+value);
                       return value;
                   }
               });
        SingleOutputStreamOperator<Long> filterDataStream =
                dataStream.filter(new FilterFunction<Long>() {
                    @Override
                    public boolean filter(Long number) throws Exception {
                        return number % 2 == 0;
                    }
                });
        filterDataStream.print().setParallelism(1);
        env.execute("StreamingDemoWithMyNoPralalleSource");
    }
}
