package com.qf.MR.model02;

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

public class model02 extends ToolRunner implements Tool{

  static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{

    //调用规律：只在map方法调用前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：没读一行数据调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      super.map(key, value, context);
    }

    //调用规律：调用完map方法之后调用一次，仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }


  static class MyReducer extends Reducer<Text,Text,Text,Text> {

    //调用规律：在调用reduce方法之前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：reducetask将相同key的<k,v>分为一组，然后调用一次reduce方法
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      super.reduce(key, values, context);
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

    //2、创建job
    Job job = Job.getInstance(conf,"model02");
    //3、设置job的执行路径
    job.setJarByClass(model02.class);

    //4、设置map端的属性
    job.setMapperClass(MyMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    //5、设置reduce端的属性
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //6、设置输入输出的参数
    FileInputFormat.setInputPaths(job,new Path(""));
    FileOutputFormat.setOutputPath(job,new Path(""));
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
      System.exit(ToolRunner.run(new Configuration(),new model02(),args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
