package com.qf.MR.Test.friends;

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
import java.util.Collections;
import java.util.Iterator;

public class Friends_step2 extends ToolRunner implements Tool {

  static class MyMapper extends Mapper<LongWritable, Text,Text,Text> {

    //调用规律：只在map方法调用前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    private static Text v = new Text();
    private static Text k = new Text();

    //调用规律：没读一行数据调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      String[] split = line.split("\t");
      String filed = split[0];
      String[] users = split[1].split(",");

      ArrayList<String> userList = new ArrayList<String>();

      for (String user:users){
        if (user != ""){
          userList.add(user);
        }
      }
      Collections.sort(userList);

      v.set(filed);
      for (int i = 0;i < userList.size()-1 ; i++){
        for (int j = i+1;j<userList.size();j++){
          k.set(userList.get(i)+"&"+userList.get(j));
          context.write(k,v);
        }
      }

    }

    //调用规律：调用完map方法之后调用一次，仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }


  private static Text v = new Text();
  static class MyReducer extends Reducer<Text,Text,Text,Text> {

    //调用规律：在调用reduce方法之前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：reducetask将相同key的<k,v>分为一组，然后调用一次reduce方法
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = values.iterator();
      StringBuffer sb = new StringBuffer();
      while (it.hasNext()){
        Text user = it.next();

        sb.append(user.toString());
        sb.append(",");
      }
      v.set(sb.toString());
      context.write(key,v);
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
    job.setJarByClass(Friends_step2.class);

    //4、设置map端的属性
    job.setMapperClass(MyMapper.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    //5、设置reduce端的属性
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //6、设置输入输出的参数
    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\wordcount\\sharedFriendsop0"));
    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\wordcount\\sharedFriendsop0out"));
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
      System.exit(ToolRunner.run(new Configuration(),new Friends_step2(),args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
