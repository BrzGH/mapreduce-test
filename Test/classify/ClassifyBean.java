package com.qf.MR.Test.classify;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClassifyBean implements WritableComparable<ClassifyBean> {
  private String cid;
  private String parentId;
  private String name;

  public ClassifyBean() {
  }

  public ClassifyBean(String cid, String parentId, String name) {
    this.cid = cid;
    this.parentId = parentId;
    this.name = name;
  }

  public String getCid() {
    return cid;
  }

  public void setCid(String cid) {
    this.cid = cid;
  }

  public String getParentId() {
    return parentId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int compareTo(ClassifyBean o) {
    int tmp = this.cid.compareTo(o.getParentId());
    if (tmp == 0 ) {
      tmp = (Integer.parseInt(this.cid) - Integer.parseInt(o.getCid())> 0 ?-1:1);
    }
    return tmp;
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(cid);
    dataOutput.writeUTF(parentId);
    dataOutput.writeUTF(name);
  }

  public void readFields(DataInput dataInput) throws IOException {
    cid = dataInput.readUTF();
    parentId = dataInput.readUTF();
    name = dataInput.readUTF();
  }
}
