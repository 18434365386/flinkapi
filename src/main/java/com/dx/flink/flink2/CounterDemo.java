package com.dx.flink.flink2;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * 需求:统计文件字母中出现次数
 */
public class CounterDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d","a", "b","a", "b");

        MapOperator<String, String> result = dataSource.map(new RichMapFunction<String, String>() {
            // 1、创建累加器
            private IntCounter numLines = new IntCounter();

            // 2、注册累计计数器
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                // 3、使用累加计数器
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(5);

        // 4、获取累加计数器
//        result.print();
        result.writeAsText("d:\\data\\mycounter");
        JobExecutionResult jobExecutionResult = env.execute("累加计数器......");
        int accumulatorResult = jobExecutionResult.getAccumulatorResult("num-lines");
        System.out.println(accumulatorResult);

    }
}
