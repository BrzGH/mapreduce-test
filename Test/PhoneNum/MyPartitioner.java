package com.qf.MR.Test.PhoneNum;

import com.qf.MR.Test.PhoneNum.flowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, flowBean> {
  @Override
  public int getPartition(Text key, flowBean flowBean, int numPartitions) {

    String prefix = key.toString().substring(0,3);
    int num = Integer.parseInt(prefix);
    int partNum = 0;
    if (num == 134){
      partNum=0;
    } else if(num == 135){
      partNum=1;
    }else if (num == 136){
      partNum= 2;
    }else if (num == 137){
      partNum= 3;
    } else {
      partNum= 4;
    }
    return partNum % numPartitions;
  }
}
