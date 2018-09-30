package com.qf.MR.Test.PhoneNum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class flowCount extends ToolRunner implements Tool {

  static class MyMapper extends Mapper<LongWritable, Text,Text, flowBean> {

    //调用规律：只在map方法调用前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：没读一行数据调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split("\t");
      String phoneNmb = fields[1];
      long upFlow = Long.parseLong(fields[fields.length - 3]);
      long downFlow = Long.parseLong(fields[fields.length - 2]);

      flowBean bean = new flowBean(upFlow,downFlow);
      context.write(new Text(phoneNmb),bean);
    }

    //调用规律：调用完map方法之后调用一次，仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }


  static class MyReducer extends Reducer<Text,flowBean,Text,flowBean> {

    //调用规律：在调用reduce方法之前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：reducetask将相同key的<k,v>分为一组，然后调用一次reduce方法
    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {
      Iterator<flowBean> it = values.iterator();

      long upFlow = 0;
      long downFlow = 0;

      while (it.hasNext()){
        flowBean next = it.next();
        upFlow += next.getUpFlow();
        downFlow += next.getDownFlow();
      }

      flowBean bean = new flowBean(upFlow,downFlow);

      context.write(key,bean);

    }

    //调用规律：在调用reduce方法之后调用一次，并且仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }


  public int run(String[] strings) throws Exception {
    Configuration conf = getConf();
    setConf(conf);

    Job job = Job.getInstance(conf,"flowCouunt");
    job.setJarByClass(flowCount.class);



    //设置map端的属性
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(flowBean.class);

    //设置reduce端的属性
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(flowBean.class);

    //设置自定义分区orderIdPartitioner
    job.setPartitionerClass(MyPartitioner.class);
    job.setNumReduceTasks(5);

    //6、设置输入输出的参数
    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\wordcount\\flowinput"));
    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\wordcount\\flowoutputpartition"));

    //7、提交job
    boolean b = job.waitForCompletion(true);

    return (b?0:1);


  }

  public void setConf(Configuration conf) {
    conf.set("fs.defaultFS","file:///");
    conf.set("mapreduce.framework.name","local");
  }

  public Configuration getConf() {
    return new Configuration();
  }

  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new Configuration(),new flowCount(),args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
