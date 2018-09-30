package com.qf.MR.Test.secondsort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * 自定义分组比较器：
 * 1、需要实现RawComparator<secondSortBean>
 *  2、需要重写两个比较方法 对象比较 和 字节比较
 *  3、重写字节比较的方法时，需要注意属性的类型的字节数要与之相等。
 *  4、默认使用对象中的第一个属性的值进行分组。
 */
public class MyGroupingComparator implements RawComparator<SecondSortBean>{
    /**
     * 字节分组比较
     * 默认使用对象的第一个属性来进行分组
     * @param b1
     * @param s1
     * @param l1
     * @param b2
     * @param s2
     * @param l2
     * @return
     */

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1,s1,4,b2,s2,4);
    }

    /**
     * 该方法比较和自定义数据类型中的compareTo方法一样
     * @param o1
     * @param o2
     * @return
     */

    public int compare(SecondSortBean o1, SecondSortBean o2) {
        return 0;
    }
}
