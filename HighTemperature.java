package com.qf.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 2018070235.6
 * 2018070328
 * 2018080434
 * 2018080536.6
 * 2018080632.8
 *
 * 求每个月的最高温度
 * 201807	35.6
 * 201808	36.6
 */
public class HighTemperature {

  static class TemMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      super.map(key, value, context);
      //数据类型装换
      String line = value.toString();
      //切分数据
      String month = line.substring(0,6);
      double temperature = Double.parseDouble(line.substring(8));
      //
      context.write(new Text(month),new DoubleWritable(temperature));
    }

  }

  static class TemReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      super.reduce(key, values, context);

      double max = Double.MIN_VALUE;
      //循环遍历迭代器
      for (DoubleWritable temperature : values){
        if (temperature.get() > max ) {
          max = temperature.get();
        }
      }
      //往下发送数据
      context.write(key,new DoubleWritable(max));
    }
  }

  public static void main(String[] args) {
    //创建一个Configuration配置项
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS","hdfs://hadoop01:9000");
    //高可用集群的设置项
//        conf.set("fs.defaultFS","hdfs://qianfeng");
//        conf.set("dfs.nameservices","qianfeng");
//        conf.set("dfs.ha.namenodes.qianfeng","nn1,nn2");
//        conf.set("dfs.namenode.rpc-address.qianfeng.nn1","hadoop01:9000");
//        conf.set("dfs.namenode.rpc-address.qianfeng.nn2","hadoop02:9000");
//        conf.set("dfs.client.failover.proxy.provider.qianfeng","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    //创建一个job 对象

    Job job = null;
    try {
      job = Job.getInstance(conf,"HighTemperature");
    } catch (IOException e) {
      e.printStackTrace();
    }

    //描述对象
    //设置job的执行路径
    job.setJarByClass(HighTemperature.class);

    //设置mapTask调用的业务逻辑类
    job.setMapperClass(TemMapper.class);

    //设置map端数据输出的类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    //设置mapTask调用的业务类
    job.setReducerClass(TemReducer.class);

    //设置reduce端的输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    //设置job的输入文件的路径
    try {
      FileInputFormat.setInputPaths(job,new Path(args[0]));
    } catch (IOException e) {
      e.printStackTrace();
    }

    //设置job的输出文件的路径
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    //提交job
    //job.submit();
    try {
      boolean b = job.waitForCompletion(true);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

}
