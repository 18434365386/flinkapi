package com.dx.flink.flink5.source;

import com.dx.flink.flink5.pojo.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

public class VideoOrderSource extends RichParallelSourceFunction<VideoOrder> {


    private volatile Boolean flag = true;

    private Random random = new Random();

    private static List<String> list = new ArrayList<>();
    static {
        list.add("Flink流式技术课程");
    }


    /**
     * run 方法调用前 用于初始化连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("-----open-----");
    }

    /**
     * 用于清理之前
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("-----close-----");
    }


    /**
     * 产生数据的逻辑
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<VideoOrder> ctx) throws Exception {

        while (flag){
            Thread.sleep(1000);
            String id = UUID.randomUUID().toString();
            int userId = random.nextInt(10);
            int money = random.nextInt(100);
            int videoNum = random.nextInt(list.size());
            String title = list.get(videoNum);
            VideoOrder videoOrder = new VideoOrder(id,title,money,userId,new Date());
            //VideoOrderSourceV2
            ctx.collect(videoOrder);
        }


    }

    /**
     * 控制任务取消
     */
    @Override
    public void cancel() {

        flag = false;
    }
}