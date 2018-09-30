package com.qf.MR.combiner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner <Text, NullWritable>{
  @Override
  public int getPartition(Text key, NullWritable nullWritable, int numPartitions) {
    String word = key.toString();
    char firstChar = word.charAt(0);
    int partNum = 0;
    if (firstChar>='a' && firstChar <='z'){
      partNum=0;
    } else if(firstChar>='A' && firstChar <='Z'){
      partNum=1;
    }else if (firstChar>='0' && firstChar <='9'){
      partNum= 2;
    } else {
      partNum= 3;
    }

    return partNum % numPartitions;
  }
}
