package com.dx.flink.product;

import com.dx.flink.pojo.ProductViewCount;
import com.dx.flink.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

import static com.dx.flink.pojo.UserBehavior.string2UserBehavior;

public class HotProduct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1、设置并行度
        env.setParallelism(1);
        // 2、设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 3、读取数据源
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\data\\data1.csv");
//        DataStreamSource<String> dataStreamSource = env.addSource(new FileSource("D:\\data\\data1.csv"));
        // 4、定义Watermark
        SingleOutputStreamOperator<UserBehavior> waterMarkData = dataStreamSource.map(line -> string2UserBehavior(line))
                .assignTimestampsAndWatermarks(new EventTimeExtractor());

        // 5、处理数据
        SingleOutputStreamOperator<String> productIdSort = waterMarkData
                .filter(line -> line.getBehavior().equals("pv"))
                .keyBy(line -> line.getProductId())
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(),new WindowResult())
                .keyBy(line -> line.getWindowEnd())
                .process(new TopHotProduct(3));

        productIdSort.print().setParallelism(1);
        System.out.println("任务完成！！！");
        env.execute("热门商品搜索......");
    }

    /**
     * ①watermark自定义函数
     */
    public static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<UserBehavior> {
        Long currentMaxEventTime = 0L;
        int maxOufOfOrderness = 10000; //最大乱序时间10s

        @Override
        public long extractTimestamp(UserBehavior userBehavior, long previousElementTimestamp) {
            /**
             * 拿到每一个事件的Event Time
             */
            Long timestamp = userBehavior.getTimestamp() * 1000;
            currentMaxEventTime = Math.max(userBehavior.getTimestamp(), currentMaxEventTime);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            /**
             * waterMark周期性产生,默认每隔200毫秒产生一个
             *      设置water产生周期为1000ms
             *      env.getConfig().setAutowatermarkInterval(1000);
             */
            System.out.println("waterMark......");
            return new Watermark(currentMaxEventTime - maxOufOfOrderness);
        }
    }

    /**
     * ②自定义聚合函数
     */
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    /**
     *  ③自定义窗口函数
     */


    public static class WindowResult implements WindowFunction<Long,ProductViewCount,Long,TimeWindow>{

        @Override
        public void apply(Long aLong, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ProductViewCount> collector) throws Exception {
            ProductViewCount productViewCount = new ProductViewCount();
            productViewCount.setProductId(Long.parseLong(aLong.toString()));
            productViewCount.setWindowEnd(timeWindow.getEnd());
            productViewCount.setCount(iterable.iterator().next());
            collector.collect(productViewCount);
        }
    }

    /**
     *  ④自定义处理函数
     */
    public static class TopHotProduct extends KeyedProcessFunction<Long,ProductViewCount,String>{
        private Integer n;
        public TopHotProduct(Integer n) {
            this.n = n;
        }

        // 1、ValueState 保存的是对应的一个 key 的一个状态值,采用managed keyed state
        private ListState<ProductViewCount> tableValueState;

        public void open(Configuration parameters){
            // 2、注册状态,从环境中获取列表状态句柄
            tableValueState = getRuntimeContext().getListState(new ListStateDescriptor<>("info2", Types.POJO(ProductViewCount.class)));
        }
        @Override


        public void processElement(ProductViewCount productViewCount, Context context, Collector<String> collector) throws Exception {
            // 3、添加状态,将count数据添加到列表状态中保存起来
            tableValueState.add(productViewCount);
            // 注册 window end + 1ms后的定时器，等待所有数据到齐开始排序
            context.timerService().registerEventTimeTimer(productViewCount.getWindowEnd() + 1);
        }

        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception{
            // 将数据从状态列表中取出,放入ArrayList方便排序
            ArrayList<ProductViewCount> productViewCountArrayList = new ArrayList<>();
            for ( ProductViewCount productViewCount : tableValueState.get() ){
                productViewCountArrayList.add(productViewCount);
            }
            // 清空状态,释放缓存
            tableValueState.clear();

            // 排序
            productViewCountArrayList.sort(new Comparator<ProductViewCount>() {
                @Override
                public int compare(ProductViewCount t1, ProductViewCount t2) {
                    return t1.getCount().intValue() - t2.getCount().intValue();
                }
            });
            //

            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                ProductViewCount productViewCount = productViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "商品ID：" + productViewCount.getProductId() + " "
                        + "浏览量：" + productViewCount.getCount() + "\n";
                result.append(info);
            }
            result.append("========================================\n");

        }
    }
}



