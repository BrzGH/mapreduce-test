package com.qf.MR.Test.secondsort;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @ClassName model02
 * @Description TODO
 * @Author Chenfg
 * @Date 2018/9/26 0026 9:55
 * @Version 1.0
 */
public class SecondSortKV extends ToolRunner implements Tool{
    static class MyMapper extends Mapper<LongWritable,Text,SecondSortBean,Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        private static Text v = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] words = line.split(" ");

            int first = Integer.parseInt(words[0]);
            int second = Integer.parseInt(words[1]);
            SecondSortBean k = new SecondSortBean(first, second);

            context.write(k,v);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    private static LongWritable v = new LongWritable();

    static class MyReducer extends Reducer<SecondSortBean,Text,SecondSortBean,NullWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(SecondSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        setConf(conf);

        //2、创建Job
        Job job = Job.getInstance(conf,"secondSort");

        //3、设置Job的执行路径
        job.setJarByClass(SecondSortKV.class);

        //4、设置map端的属性
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(SecondSortBean.class);
        job.setMapOutputValueClass(Text.class);

        //5、设置reduce端的属性
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(SecondSortBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6、设置输入输出的参数
        FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\wordcount\\secondSort"));

        FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\wordcount\\secondSortoutput2"));

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
            System.exit(ToolRunner.run(new Configuration(),new SecondSortKV(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
