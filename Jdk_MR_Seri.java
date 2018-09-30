package com.qf.MR;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;

/**
 * JDK序列化和MR序列化之间的比较
 */
public class Jdk_MR_Seri {
  public static void main(String[] args) throws Exception{
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    ByteArrayOutputStream ba2 = new ByteArrayOutputStream();

    //定义两个DataOutputStream,用于将普通的对象进行jdk标准序列化
    DataOutputStream dout = new DataOutputStream(ba);
    DataOutputStream dout2 = new DataOutputStream(ba2);

    ObjectOutputStream obout = new ObjectOutputStream(dout2);

    //定义两个bean，作为序列化的源对象


  }
}
