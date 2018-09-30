package com.qf.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName model01
 * @Description TODO
 * @Author Chenfg
 * @Date 2018/9/20 0020 15:54
 * @Version 1.0
 */
public class model01 {
    static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
        //调用规律：在调用map方法之前调用一次，并且仅调用一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        //调用规律：maptask每读取一行数据就调用一次map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

        //调用规律：在调用map方法之后调用一次，并且仅调用一次
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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://qianfeng");

        Job job = Job.getInstance(conf, "model");

        job.setJarByClass(model01.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }
}
