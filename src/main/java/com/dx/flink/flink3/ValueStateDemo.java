package com.dx.flink.flink3;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 业务需求: 每三个相同的值过来后求均值
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L)
                        ,Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        // 输出：
        //(1,5.0)
        //(2,3.6666666666666665)
        dataStreamSource
                .keyBy(0)
                .flatMap(new CountWindowAverageWithValueState())
                .print();
        env.execute("TestStatefulApi");
    }

    /**
     * ValueState<T> ：用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
     * value() 获取状态值
     * update() 更新状态值
     * clear() 清除状态
     */
    public static class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        // managed keyed state
        //1. ValueState 保存的是对应的一个 key 的一个状态值
        private ValueState<Tuple2<Long, Long>> countAndSum;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册状态
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<Tuple2<Long, Long>>(
                            "average", // 状态的名字
                            Types.TUPLE(Types.LONG, Types.LONG)); // 状态存储的数据类型
            countAndSum = getRuntimeContext().getState(descriptor);
        }
        @Override
        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
            // 拿到当前的 key 的状态值
            Tuple2<Long, Long> currentState = countAndSum.value();
            // 如果状态值还没有初始化，则初始化
            if (currentState == null) {
                currentState = Tuple2.of(0L, 0L);
            }
            // 更新状态值中的元素的个数
            currentState.f0 += 1;
            // 更新状态值中的总值
            currentState.f1 += element.f1;
            // 更新状态
            countAndSum.update(currentState);
            // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
            if (currentState.f0 >= 3) {
                double avg = (double)currentState.f1 / currentState.f0;
                // 输出 key 及其对应的平均值
                out.collect(Tuple2.of(element.f0, avg));
                // 清空状态值
                countAndSum.clear();
            }
        }
    }

}
