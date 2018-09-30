package com.qf.MR.Test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProBean implements WritableComparable<ProBean> {

  private String city;//市
  private String country;//县（区）
  private String industry;//所属行业
  private String proclass;//产品种类
  private String proname;//产品名称
  private String applicantname;//申请人全称
  private String applicantaddress;//申请人地址
  private String origin; //认定产地地址
  private String date; //证书到期日期

  public ProBean() {
  }

  public ProBean(String country, String proclass) {
    this.country = country;
    this.proclass = proclass;
  }

  public ProBean(String city, String country, String industry, String proclass, String proname, String applicantname, String applicantaddress, String origin, String date) {
    this.city = city;
    this.country = country;
    this.industry = industry;
    this.proclass = proclass;
    this.proname = proname;
    this.applicantname = applicantname;
    this.applicantaddress = applicantaddress;
    this.origin = origin;
    this.date = date;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getIndustry() {
    return industry;
  }

  public void setIndustry(String industry) {
    this.industry = industry;
  }

  public String getProclass() {
    return proclass;
  }

  public void setProclass(String proclass) {
    this.proclass = proclass;
  }

  public String getProname() {
    return proname;
  }

  public void setProname(String proname) {
    this.proname = proname;
  }

  public String getApplicantname() {
    return applicantname;
  }

  public void setApplicantname(String applicantname) {
    this.applicantname = applicantname;
  }

  public String getApplicantaddress() {
    return applicantaddress;
  }

  public void setApplicantaddress(String applicantaddress) {
    this.applicantaddress = applicantaddress;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin(String origin) {
    this.origin = origin;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public int compareTo(ProBean o) {
    return 0;
  }

  public void write(DataOutput dataOutput) throws IOException {

    dataOutput.writeUTF(city);
    dataOutput.writeUTF(country);
    dataOutput.writeUTF(industry);
    dataOutput.writeUTF(proclass);
    dataOutput.writeUTF(proname);
    dataOutput.writeUTF(applicantname);
    dataOutput.writeUTF(applicantaddress);
    dataOutput.writeUTF(origin);
    dataOutput.writeUTF(date);
  }

  public void readFields(DataInput dataInput) throws IOException {

    city = dataInput.readUTF();
    country = dataInput.readUTF();
    industry = dataInput.readUTF();
    proclass = dataInput.readUTF();
    proname = dataInput.readUTF();
    applicantname = dataInput.readUTF();
    applicantaddress = dataInput.readUTF();
    origin = dataInput.readUTF();
    date = dataInput.readUTF();

  }

  @Override
  public String toString() {
    return "ProBean{" +
        "country='" + country + '\'' +
        ", proclass='" + proclass + '\'' +
        '}';
  }
}
