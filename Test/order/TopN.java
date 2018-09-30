package com.qf.MR.Test.order;

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

/**
 * 根据Id输出最大的，分组输出
 */
public class TopN extends ToolRunner implements Tool {

  static class MyMapper extends Mapper<LongWritable, Text,OrderBean,NullWritable> {

    //调用规律：只在map方法调用前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：没读一行数据调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] field = line.split(",");

      OrderBean bean = new OrderBean(field[0],field[1],Double.parseDouble(field[2]));
      context.write(bean, NullWritable.get());
    }

    //调用规律：调用完map方法之后调用一次，仅调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }


  static class MyReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {

    //调用规律：在调用reduce方法之前调用一次，并且仅调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    //调用规律：reducetask将相同key的<k,v>分为一组，然后调用一次reduce方法
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

      context.write(key,NullWritable.get());
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

    //2、创建Job
    Job job = Job.getInstance(conf,"topN");

    //3、设置Job的执行路径
    job.setJarByClass(TopN.class);

    //4、设置map端的属性
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(OrderBean.class);
    job.setMapOutputValueClass(NullWritable.class);

    //5、设置reduce端的属性
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(OrderBean.class);
    job.setOutputValueClass(NullWritable.class);


    //设置自定义分区orderIdPartitioner
    job.setPartitionerClass(OrderPartitioner.class);
    job.setNumReduceTasks(3);

    //设置自定义分组比较器
    job.setGroupingComparatorClass(OrderIdGroupingComparator.class);


    //6、设置输入输出的参数
    FileInputFormat.setInputPaths(job,new Path("F:\\hdfs\\wordcount\\gpinput"));

    FileOutputFormat.setOutputPath(job,new Path("F:\\hdfs\\wordcount\\gpoutput"));

    //7、提交job
    boolean b = job.waitForCompletion(true);

    return (b?0:1);
  }

  public void setConf(Configuration configuration) {
    configuration.set("fs.defaultFS","file:///");
    configuration.set("mapreduce.frame.name","local");
  }

  public Configuration getConf() {
    return new Configuration();
  }

  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new Configuration(),new TopN(),args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
