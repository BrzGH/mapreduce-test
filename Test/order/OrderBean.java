package com.qf.MR.Test.order;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
  private String orderId;
  private String pid;
  private double amount;

  public OrderBean() {
  }

  public OrderBean(String orderId, String pid, double amount) {
    this.orderId = orderId;
    this.pid = pid;
    this.amount = amount;
  }

  public String getOrderId() {
    return orderId;
  }

  public void setOrderId(String orderId) {
    this.orderId = orderId;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public double getAmount() {
    return amount;
  }

  public void setAmount(double amount) {
    this.amount = amount;
  }

  public int compareTo(OrderBean o) {
    int tmp = this.orderId.compareTo(o.getOrderId());
    if (tmp == 0 ) {
      tmp = (this.amount - o.getAmount() > 0 ?-1:1);
    }
    return tmp;
  }

  //序列化
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(orderId);
    dataOutput.writeUTF(pid);
    dataOutput.writeDouble(amount);
  }

  //反序列化
  public void readFields(DataInput dataInput) throws IOException {

    orderId = dataInput.readUTF();
    pid = dataInput.readUTF();
    amount = dataInput.readDouble();
  }

  @Override
  public String toString() {
    return "OrderBean{" +
        "orderId='" + orderId + '\'' +
        ", pid='" + pid + '\'' +
        ", amount=" + amount +
        '}';
  }
}
