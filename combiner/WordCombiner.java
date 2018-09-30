package com.qf.MR.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCombiner {
  static class MyMapper extends Mapper<LongWritable,Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] words = line.split(" ");

      for (String word : words) {
        context.write(new Text(word),NullWritable.get());
      }
    }
  }

  static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key,NullWritable.get());
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    //1、获取配置项
    Configuration conf = new Configuration();

    //2、设置连接参数
    conf.set("fs.defaultFS","file:///");
    conf.set("mapreduce.framework.name","local");

    //3、获取Job对象
    Job job = Job.getInstance(conf, "MyPartitionerDemo");

    //4、设置Job的执行路径
    job.setJarByClass(WordCombiner.class);

    //5、设置map端的相关参数
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    //6、设置reduce端的相关参数
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    //设置自定义分区类
    job.setPartitionerClass(MyPartitioner.class);

    //设置reduceTask的数量
    job.setNumReduceTasks(4);

    //设置输入输出的路径
    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\multiplefiles\\words.txt"));
    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\multiplefiles\\output"));

    //提交job
    boolean b = job.waitForCompletion(true);

    //退出程序
    System.exit(b?0:1);
  }
}
