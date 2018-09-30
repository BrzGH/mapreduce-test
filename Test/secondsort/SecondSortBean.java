package com.qf.MR.Test.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondSortBean implements WritableComparable<SecondSortBean>{
    private int first;
    private int second;

    public SecondSortBean() {
    }

    public SecondSortBean(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }


    public int compareTo(SecondSortBean o) {
        int tmp = this.first - o.getFirst(); //当前对象 - 传递进来的对象 升序
//        int tmp = o.getFirst() - this.first; //倒序
        if (tmp != 0){
            return tmp;
        }

//        return this.second - o.getSecond();  //升序
        return o.getSecond() - this.second;  //倒序
    }

    //序列化

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    //反序列化

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second=in.readInt();
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondSortBean that = (SecondSortBean) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public String toString() {
        return "secondSortBean{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
