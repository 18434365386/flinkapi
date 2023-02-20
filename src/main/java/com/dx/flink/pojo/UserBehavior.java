package com.dx.flink.pojo;


import lombok.Data;


public class UserBehavior {
    private Long userId;
    private Long productId;
    private Long categoryId;
    private String behavior;
    private Long timestamp;


    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long productId, Long categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public static UserBehavior string2UserBehavior(String line){
        UserBehavior userBehavior = new UserBehavior();
        if(line != null && line.length() > 0) {
            String[] fields = line.split(",");
            userBehavior.setUserId(Long.parseLong(fields[0]));
            userBehavior.setProductId(Long.parseLong(fields[1]));
            userBehavior.setCategoryId(Long.parseLong(fields[2]));
            userBehavior.setTimestamp(Long.parseLong(fields[4]));
            userBehavior.setBehavior(fields[3]);
        }
        return userBehavior;
    }
}
