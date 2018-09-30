package com.qf.MR.Test.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
  @Override
  public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
    //指定OrderId相同的bean发往相同的reduceTask
    return ((key.getOrderId().hashCode()&Integer.MAX_VALUE)%numPartitions);
  }
}
