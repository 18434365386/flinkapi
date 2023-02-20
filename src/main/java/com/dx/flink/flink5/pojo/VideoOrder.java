package com.dx.flink.flink5.pojo;

import java.util.Date;

public class VideoOrder {
    private String tradeNo;
    private String title;
    private int money;
    private int userId;
    private Date createTime;

    public VideoOrder() {
    }

    public VideoOrder(String tradeNo, String title, int money, int userId, Date createTime) {
        this.tradeNo = tradeNo;
        this.title = title;
        this.money = money;
        this.userId = userId;
        this.createTime = createTime;
    }

    public String getTradeNo() {
        return tradeNo;
    }

    public void setTradeNo(String tradeNo) {
        this.tradeNo = tradeNo;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getMoney() {
        return money;
    }

    public void setMoney(int money) {
        this.money = money;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "VideoOrder{" +
                "tradeNo='" + tradeNo + '\'' +
                ", title='" + title + '\'' +
                ", money=" + money +
                ", userId=" + userId +
                ", createTime=" + createTime +
                '}';
    }
}
