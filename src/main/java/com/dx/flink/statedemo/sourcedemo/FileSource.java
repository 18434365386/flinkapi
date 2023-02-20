package com.dx.flink.statedemo.sourcedemo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 *  自定义数据源 : 读取文本数据模拟成流式数据发送
 */
public class FileSource implements SourceFunction<String> {
    public String filePath;
    public FileSource(String filePath){
        this.filePath = filePath;
    }
    private InputStream inputStream;
    private BufferedReader txtReader;
    private Random random = new Random();

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        // 1、读取文本数据
         txtReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        // 2、模拟发送数据
        String line = null;
        while ((line = txtReader.readLine()) != null){
            // 模拟发送数据
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            // 发送数据
            sourceContext.collect(line);
        }
        if (txtReader.readLine() == null){
            txtReader.close();
        }
        if (inputStream == null){
            inputStream.close();
        }
    }

    @Override
    public void cancel() {
        try{
            if (txtReader == null){
                txtReader.close();
            }
            if (inputStream == null){
                inputStream.close();
            }
        } catch (Exception e){
        }
    }
}
