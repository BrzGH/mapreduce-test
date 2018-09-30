package com.qf.MR.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Iterator;

/**
 *
 * 分析统计出贵州各县（区）分别有多少种无公害农产品？
 */
public class W1 {
    static class WCMapper extends Mapper<LongWritable, Text, Text, Text>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1、数据类型转换
            String line = value.toString();

            //2、切分数据
            String[] fields = line.split("/t");

            String country = fields[1];
            String proclass = fields[3];

            //4、输出结果
            context.write(new Text(country),new Text(proclass));

        }
    }

    /**
     *
     *     reduce方法调用的规律：框架会从map阶段的输出结果中找出所有的key相同的<k,v>数据对组成一组数据，
     *     然后调用一次reduce()方法
     */
    static class WCReducer extends Reducer<Text, Text, Text, LongWritable>{

      @Override
      protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        //循环遍历迭代器
        Iterator<Text> it = values.iterator();
        while (it.hasNext()){
          count++;
        }

        //往下发送数据
        context.write(key,new LongWritable(count));
      }

    }




    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建一个Configuration配置项
        Configuration conf = new Configuration();

        //2、配置连接参数

        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");

        //3、创建一个job对象
        Job job = Job.getInstance(conf,"wordCount");

        //4、描述对象
        //5、设置Job的执行路径
        job.setJarByClass(W1.class);

        //6、设置mapTask调用的业务逻辑类
        job.setMapperClass(WCMapper.class);

        //7、设置map端数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //8、设置mapTask调用的业务类
        job.setReducerClass(WCReducer.class);

        //9、设置reduce端的数据的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);



        //10、设置Job的输入文件的路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\brz\\IdeaProjects\\GP1813Hadoop\\src\\main\\java\\com\\qf\\MR\\Test\\data.txt"));

        //11、设置Job的输出文件的路径
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\brz\\IdeaProjects\\GP1813Hadoop\\src\\main\\java\\com\\qf\\MR\\Testout1"));

        //12、提交job
//        job.submit();
        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }
}
