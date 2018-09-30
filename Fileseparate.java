package com.qf.MR;

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

/**

 * 词频统计
 * 输出数据：
 * word,n
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     框架在调用咱们写的map方法时，会将数据作为参数（一个key，一个value）传递给map方法
 *     KEYIN：是框架（maptask）要传递给map方法的输入参数中的key的数据类型
 *     VALUEIN：是框架（maptask）要产地给map方法的输入参数中的value的数据类型
 *
 *     在默认情况下，框架传入的key是框架从待处理数据（文本文件）中读取到的‘某一行’数据的起始偏移量，所以类型是Long
 *     框架传入的value是框架从待处理数据中读到的‘某一行’的内容，所以类型是String
 *
 *     但是，Long或者String等java的原生态数据类型的序列化的效率较低，所以hadoop对其进行了封装改造，
 *     有替代品：LongWriable/Text
 *
 *     map方法处理完数据后需要返回一个结果（一个key一个value的键值对数据）
 *     KEYOUT：是咱们的map方法处理完成后返回结果中的key的数据类型
 *     VALUEOUT：是咱们的map方法处理完成后返回结果中的value的数据类型
 *
 *
 *
 *     思路：
 *     map阶段的处理逻辑：
 *     1、用空格切分单词
 *     2、循环遍历单词
 *     3、输出结果，<word,1>
 *
 *         map方法的调用规律：
 *         maptask没读取到一行数据就会调用此意map方法
 */
public class Fileseparate {
  static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      InputSplit inputSplit = context.getInputSplit();
      String fileName = ((FileSplit)inputSplit).getPath().getName();


      //1、数据类型转换
      String line = value.toString();

      //2、切分数据
      String[] words = line.split(" ");

      //3、循环遍历单词
      for (String word : words) {
        //4、输出结果
        context.write(new Text(word),new LongWritable(1));
      }
    }
  }

  /**
   * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
   *     reduce()方法要接收的输入参数是一个key一个迭代器<T>的values
   *     KEYIN：框架（reducetask）要传递给reduce方法的输入参数的key的数据类型
   *     VALUEIN：框架（reducetask）要传递给reduce方法的输入参数的value的数据类型
   *
   *
   *      KEYOUT：reduce方法处理后的数据的返回结果的key的数据类型
   *      VALUEOUT：reduce方法处理后的数据的返回结果的value的数据类型
   *
   *     reduce方法调用的规律：框架会从map阶段的输出结果中找出所有的key相同的<k,v>数据对组成一组数据，
   *     然后调用一次reduce()方法
   */
  static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      //为什么用迭代器
      /**
       * map端的输出数据：
       <a,1>
       <b,1>
       <c,1>
       <a,1>
       <a,1>
       <d,1>

       reduce端的输入数据：
       <a,<1,1,1>>
       */

      int count = 0;
      //循环遍历迭代器
      for (LongWritable i :values) {
        count += i.get();
      }

      //往下发送数据
      context.write(key,new LongWritable(count));
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    //1、创建一个Configuration配置项
    Configuration conf = new Configuration();

    //2、配置连接参数
    // conf.set("fs.defaultFS","hdfs://hadoop01:9000");

    conf.set("fs.defaultFS","hdfs:///");
    conf.set("mapreduce.framework.name","local");

    //高可用集群的设置项
//    conf.set("fs.defaultFS","hdfs://qianfeng");
//    conf.set("dfs.nameservices","qianfeng");
//    conf.set("dfs.ha.namenodes.qianfeng","nn1,nn2");
//    conf.set("dfs.namenode.rpc-address.qianfeng.nn1","hadoop01:9000");
//    conf.set("dfs.namenode.rpc-address.qianfeng.nn2","hadoop02:9000");
//    conf.set("dfs.client.failover.proxy.provider.qianfeng","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    //3、创建一个job对象
    Job job = Job.getInstance(conf,"wordCount");

    //4、描述对象
    //5、设置Job的执行路径
    job.setJarByClass(Fileseparate.class);

    //6、设置mapTask调用的业务逻辑类
    job.setMapperClass(WCMapper.class);

    //7、设置map端数据输出的类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    //8、设置ReducerTask调用的业务类
    //job.setNumReduceTasks(3);
    job.setReducerClass(WCReducer.class);

    //9、设置reduce端的数据的输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    //10、设置Job的输入文件的路径
    FileInputFormat.setInputPaths(job,new Path(args[0]));

    //11、设置Job的输出文件的路径
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    //12、提交job
//        job.submit();
    boolean b = job.waitForCompletion(true);

    System.exit(b?0:1);
  }
}
