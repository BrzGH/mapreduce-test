package com.qf.MR.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 *         map方法的调用规律：
 *         maptask没读取到一行数据就会调用此意map方法
 */
public class W4 {
    static class WCMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1、数据类型转换
            String line = value.toString();

            //2、切分数据
            String[] words = line.split(" ");

            //3、循环遍历单词
            for (String word : words) {
                if (word.equals("sensitive.txt")){

                }
                context.write(new Text(word),NullWritable.get());
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建一个Configuration配置项
        Configuration conf = new Configuration();

        //2、配置连接参数
//        conf.set("fs.defaultFS","hdfs://qianfeng");
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");


        //3、创建一个job对象
        Job job = Job.getInstance(conf,"wordCount");

        //4、描述对象
        //5、设置Job的执行路径
        job.setJarByClass(W4.class);

        //6、设置mapTask调用的业务逻辑类
        job.setMapperClass(WCMapper.class);

        //7、设置map端数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);



        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);



       // job.setNumReduceTasks(3);

        //10、设置Job的输入文件的路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\brz\\IdeaProjects\\GP1813Hadoop\\src\\main\\java\\com\\qf\\MR\\Test\\data.txt"));

        //11、设置Job的输出文件的路径
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\brz\\IdeaProjects\\GP1813Hadoop\\src\\main\\java\\com\\qf\\MR\\Test\\data"));

        //12、提交job
//        job.submit();
        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }
}
