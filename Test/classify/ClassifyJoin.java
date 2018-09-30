package com.qf.MR.Test.classify;

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
import java.util.ArrayList;
import java.util.Iterator;

public class ClassifyJoin extends ToolRunner implements Tool {

  static class MyMapper extends Mapper<LongWritable, Text,Text,Text> {

    //调用规律：只在map方法调用前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }


    private  static Text k = new Text();
    private  static Text v = new Text();
    //调用规律：没读一行数据调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] relations = line.split("\t");

      //ClassifyBean bean = new ClassifyBean(field[0],field[1],field[2]);

      //1、写左表 p r-c parent
      k.set(relations[1]);
      v.set(relations[0] + "  " + relations[2]);
      context.write(k,v);

      //2、写右表 c r-p child
      k.set(relations[0]);
      v.set(relations[1] + "  " + relations[2]);
      context.write(k,v);


    }

    //调用规律：调用完map方法之后调用一次，仅调用一次
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
    job.setJarByClass(ClassifyJoin.class);

    //4、设置map端的属性
    job.setMapperClass(MyMapper.class);
//    job.setOutputKeyClass(LongWritable.class);
//    job.setOutputValueClass(Text.class);

//    //5、设置reduce端的属性
//    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //6、设置输入输出的参数
    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\wordcount\\classifydata"));
    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\wordcount\\classifydataout"));
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
      System.exit(ToolRunner.run(new Configuration(),new ClassifyJoin(),args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
