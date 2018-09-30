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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Score {
  static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] str = line.split(" ");

      context.write(new Text(((FileSplit)context.getInputSplit()).getPath().toString()), new DoubleWritable(Integer.parseInt(str[1])));

    }
  }

  static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
      for (DoubleWritable i :values) {
        count ++;
        sum += i.get();
      }


      context.write(key, new DoubleWritable(sum/count));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    conf.set("fs.defaultFS","file:///");
    conf.set("mapreduce.framework.name","local");

    Job job = Job.getInstance(conf,"Socer");
    job.setJarByClass(Score.class);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\score\\"));

    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\score\\output"));

    boolean b = job.waitForCompletion(true);
    System.exit(b?0:1);
  }

}
