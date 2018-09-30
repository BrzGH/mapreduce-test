package com.qf.MR.Test.PhoneNum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  自定义的流量类
 *  数据必须在网络间传递，所以需要实现序列化及反序列化的方法
 *  自定义类必须实现Writable接口
 */
public class flowBean implements Writable {
  private long upFlow;
  private long downFlow;
  private long sumFlow;

  //如果空参构造函数被覆盖，一定要显式定义一下，否则在反序列化时会报错
  public flowBean() {
  }

  public flowBean(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  public long getUpFlow() {
    return upFlow;
  }

  public void setUpFlow(long upFlow) {
    this.upFlow = upFlow;
  }

  public long getDownFlow() {
    return downFlow;
  }

  public void setDownFlow(long downFlow) {
    this.downFlow = downFlow;
  }

  public long getSumFlow() {
    return sumFlow;
  }

  public void setSumFlow(long sumFlow) {
    this.sumFlow = sumFlow;
  }

  //序列化  将对象的字段信息写入输出流
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(upFlow);
    dataOutput.writeLong(downFlow);
    dataOutput.writeLong(sumFlow);
  }

  //反序列化 从输入流中读取各字段信息
  public void readFields(DataInput dataInput) throws IOException {
    upFlow = dataInput.readLong();
    downFlow = dataInput.readLong();
    sumFlow = dataInput.readLong();
  }

  @Override
  public String toString() {
    return
        upFlow + "\t" + downFlow + "\t" + sumFlow ;
  }

  //排序按照总流量倒序、上行流量、下行流量倒序、手机号正序排列
  public int equals(flowBean o) {

    int tmp = (int)(o.getSumFlow() - this.sumFlow) ;//倒序

    if (tmp != 0){
      return tmp;
    }
    int t = (int)(o.getUpFlow() - this.upFlow);
    if (t != 0){
      return t;
    }
    return (int)(o.getDownFlow() - this.downFlow);
  }
}
