package com.dx.flink.statedemo.pojo;

import scala.tools.nsc.doc.model.Public;

import java.util.Date;

public class TableTwo {
    private Long orderId;
    private String timeStamp;
    private String address;

    public TableTwo() {
    }

    public TableTwo(Long orderId, String timeStamp, String address) {
        this.orderId = orderId;
        this.timeStamp = timeStamp;
        this.address = address;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "TableOne{" +
                "orderId=" + orderId +
                ", timeStamp=" + timeStamp +
                ", address='" + address + '\'' +
                '}';
    }
    public static TableTwo string2TableTwo(String line){
        TableTwo tableTwo = new TableTwo();
        if(line != null && line.length() > 0){
            String[] fields = line.split(",");
            tableTwo.setOrderId(Long.parseLong(fields[0]));
            tableTwo.setTimeStamp(fields[1]);
            tableTwo.setAddress(fields[2]);
        }
        return tableTwo;
    }
}
