package com.dx.flink.flink3;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReducingStateDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L),Tuple2.of(2L, 5L));
        // 输出：
        //(1,5.0)
        //(2,3.6666666666666665)
        dataStreamSource
                .keyBy(0)
                .flatMap(new SumFunction())
                .print();
        env.execute("TestStatefulApi");
    }

    public static class SumFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        // managed keyed state
        // 用于保存每一个 key 对应的 value 的总值
        private ReducingState<Long> sumState;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册状态
            ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<Long>(
                "sum", // 状态的名字
                new ReduceFunction<Long>() { // 聚合函数
                    @Override
                    public Long reduce(Long value1, Long value2) throws
                            Exception {
                        return value1 + value2;
                    }
                }, Long.class); // 状态存储的数据类型
            sumState = getRuntimeContext().getReducingState(descriptor);
        }
        @Override
        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out) throws Exception {
            // 将数据放到状态中
            sumState.add(element.f1);
            out.collect(Tuple2.of(element.f0, sumState.get()));
        }
    }
}
