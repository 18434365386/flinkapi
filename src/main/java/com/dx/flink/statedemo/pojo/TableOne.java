package com.dx.flink.statedemo.pojo;

public class TableOne {
    private Long orderId;
    private String productName;
    private Double price;

    public TableOne() {
    }

    public TableOne(Long orderId, String productName, Double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "TableTwo{" +
                "orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }
    public static TableOne string2TableOne(String line){
        TableOne tableOne = new TableOne();
        if(line != null && line.length() > 0){
            String[] fields = line.split(",");
            tableOne.setOrderId(Long.parseLong(fields[0]));
            tableOne.setProductName(fields[1]);
            tableOne.setPrice(Double.parseDouble(fields[2]));
        }
        return tableOne;
    }
}
