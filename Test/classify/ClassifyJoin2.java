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

public class ClassifyJoin2 extends ToolRunner implements Tool{

  static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    private static Text k = new Text();
    private static Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //
      String line = value.toString();

      String[] relations = line.split("/t");

      //1、写左表
      k.set(relations[1]);
      v.set("1-"+relations[0]);
      context.write(k,v);

      //2、写右表 c r-p child
      k.set(relations[0]);
      v.set("2-"+relations[1]);
      context.write(k,v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }

  static class MyReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    private static Text k = new Text();
    private static Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      ArrayList<String> grandchild = new ArrayList<String>();
      ArrayList<String> grandparent = new ArrayList<String>();

      Iterator<Text> it = values.iterator();

      while (it.hasNext()){
        String relation = it.next().toString();

        //判断关系
        if(relation.charAt(0) == '1'){
          grandchild.add(relation.split("-")[1]);
        }else if(relation.charAt(0) == '2'){
          grandparent.add(relation.split("-")[1]);
        }
      }

      //使用笛卡尔积关联查询
      for (int i = 0; i < grandchild.size(); i++) {
        for (int j = 0; j < grandparent.size() ; j++) {
          k.set(grandchild.get(i));
          v.set(grandparent.get(j));
          context.write(k,v);
        }
      }
    }

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
    job.setJarByClass(ClassifyJoin2.class);

    //4、设置map端的属性
    job.setMapperClass(MyMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    //5、设置reduce端的属性
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //6、设置输入输出的参数
    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\wordcount\\classifydataout"));
    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\wordcount\\classifydataout2"));
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
      System.exit(ToolRunner.run(new Configuration(),new ClassifyJoin2(),args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
