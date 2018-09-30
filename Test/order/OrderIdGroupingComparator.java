package com.qf.MR.Test.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator {
  public OrderIdGroupingComparator(){
    super(OrderBean.class,true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    OrderBean a1 = (OrderBean)a;
    OrderBean b1 = (OrderBean)b;
    //将orderID相同的bean都视为相同，从而聚合到一组中
    return a1.getOrderId().compareTo(b1.getOrderId());
  }
}
