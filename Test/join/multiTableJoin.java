package com.qf.MR.Test.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class multiTableJoin extends ToolRunner implements Tool{
    static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
        //定义容器存储缓存的数据
        private static Map<String,String> sexMap =  new HashMap<String,String>();
        private static Map<String,String> usersMap =  new HashMap<String,String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取分布式缓存文件
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            //URI[] cacheFiles = context.getCacheFiles();
            BufferedReader bf = null;
            for (Path p : paths) {
                String fileName = p.getName();
                if (fileName.equals("sex")){
                    bf = new BufferedReader(new FileReader(new File(p.toString())));
                    String str = null;
                    while((str = bf.readLine()) != null){
                        String[] sexs = str.split("\t");
                        sexMap.put(sexs[0],sexs[1]);
                    }
                    bf.close();
                }else if (fileName.equals("users")){
                    bf = new BufferedReader(new FileReader(new File(p.toString())));
                    String str = null;
                    while((str = bf.readLine()) != null){
                        String[] users = str.split("\t");
                        usersMap.put(users[0],users[1]);
                    }
                    bf.close();
                }
            }
        }

        private static Text k = new Text();
        private static Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //
            String line = value.toString();

            String[] fields = line.split("\t");

            String uId = fields[0];
            String sexId = fields[1];
            String d = fields[2];

            //获取关联关系
            String sexName = sexMap.get(sexId);
            String userName = usersMap.get(uId);

            v.set(sexName + "\t" + userName + "\t" +d );
            k.set(uId);

            context.write(k,v);
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
        Job job = Job.getInstance(conf,"multiTableJoin");

        //3、设置Job的执行路径
        job.setJarByClass(multiTableJoin.class);

        //4、设置map端的属性
        job.setMapperClass(MyMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);

//        //5、设置reduce端的属性
//        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置分布式缓存文件
//        URI[] uris = {new URI(args[0]),new URI(args[1])};
//        job.setCacheFiles(uris);

        DistributedCache.addCacheFile(new URI(args[0]),conf);
        DistributedCache.addCacheFile(new URI(args[1]),conf);

        //6、设置输入输出的参数
        FileInputFormat.setInputPaths(job,new Path(args[2]));

        FileOutputFormat.setOutputPath(job,new Path(args[3]));

        //7、提交job
        boolean b = job.waitForCompletion(true);

        return (b?0:1);
    }


    public void setConf(Configuration conf) {
//        conf.set("fs.defaultFS","file:///");
//        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","hdfs://qianfeng");
    }

    public Configuration getConf() {
        return new Configuration();
    }

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new Configuration(),new multiTableJoin(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
