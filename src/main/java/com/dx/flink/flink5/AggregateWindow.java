package com.dx.flink.flink5;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AggregateWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //需要设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.6.102", 7777);
        SingleOutputStreamOperator<Integer> mapStream = dataStreamSource.map(line -> Integer.parseInt(line));
        mapStream.timeWindowAll(Time.seconds(5))
                .aggregate(new MyAggregate())
                .print();
        env.execute("job submit!!!");
    }

    private static class MyAggregate implements AggregateFunction<Integer, Tuple2<Integer,Integer>,Double>{
        /**
         * 初始化 累加器
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0,0);
        }
        /**
         * 针对每个数据的操作
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> add(Integer element, Tuple2<Integer, Integer> accumulator) {
            //个数+1
            //总的值累计
            return new Tuple2<>(accumulator.f0+1,accumulator.f1+element);
        }
        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return (double)accumulator.f1/accumulator.f0;
        }
        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a1, Tuple2<Integer, Integer> b1) {
            return Tuple2.of(a1.f0+b1.f0,a1.f1+b1.f1);
        }
    }
}
