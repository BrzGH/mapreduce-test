package com.qf.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 需求：有以下几个文件，每个文件代表一个学科的成绩
 * chinese.txt
 * zs1 80
 * zs2 81
 * zs3 82
 * zs4 83
 * zs5 84
 *
 * math.txt
 * zs1 80
 * zs2 81
 * zs3 82
 * zs4 83
 * zs5 84
 *
 * english.txt
 * zs1 80
 * zs2 81
 * zs3 82
 * zs4 83
 * zs5 84
 *
 * 需要求出各学科的平均成绩
 * 输出格式
 * chinese	math	english
 * 80	87	83
 */
public class AverageScore {
  static class AverScoreMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{

//    String fileName = null;


    //调用规律：在调用map方法之前调用且仅调用一次
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//      InputSplit inputSplit = context.getInputSplit();
//      fileName = ((FileSplit)inputSplit).getPath().getName();
//
//    }

    //调用规律：maptask没读取一行数据就调用一次map方法


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // super.map(key, value, context);
      String line = value.toString();

      StringTokenizer token = new StringTokenizer(line);//以空格分割字符串

      String name = token.nextToken();
      String strScore = token.nextToken();

      //Text name = new Text(strName);
      double score = Double.parseDouble(strScore);

      context.write(new Text(name),new DoubleWritable(score));
    }

    //调用规律：在调用map方法之后调用且仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      //super.cleanup(context);


    }
  }

  static class AverScoreReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

    //调用规律：在调用reducer之前调用且只调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      //super.setup(context);

      //System.out.println("\t+chinese\tmath\tenglish\t");
    }

    //调用规律：reducetask将相同的key的<K,V>分为一组，然后调用一次Reducer方法

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      //super.reduce(key, values, context);
      double sum = 0;
      int count = 0;

      for (DoubleWritable score : values){
        sum += score.get();//计算总分
        count++; //统计总的科目数
      }
      double average = sum/count;
      context.write(key,new DoubleWritable(average));

    }

    //调用规律：在调用reduce方法之后调用且仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
     // super.cleanup(context);

    }
  }

  public static void main(String[] args)throws Exception {
    //
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS","hdfs://hadoop01:9000");

    Job job = Job.getInstance(conf,"AverageScore");

    job.setJarByClass(AverageScore.class);

    job.setMapperClass(AverScoreMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setReducerClass(AverScoreReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.setInputPaths(job,new Path(args[0]));

    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    boolean b = job.waitForCompletion(true);

    System.exit(b?0:1);
  }
}
