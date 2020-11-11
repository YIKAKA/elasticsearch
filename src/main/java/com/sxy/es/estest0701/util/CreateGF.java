package com.sxy.es.estest0701.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.databricks.spark.csv.util.TextFile;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.spark.rdd.EsSpark;

import java.util.HashMap;
import java.util.Map;

public class CreateGF {

  public static void main(String[] args){
    SparkConf conf = new SparkConf();
    conf.setMaster("local");
    conf.setAppName("CSVToES");
    conf.set("es.index.auto.create", "true");
    conf.set("es.nodes","192.168.2.23");
    conf.set("es.port","9200");
    SparkContext sc = new SparkContext(conf);

    //不要最前面和最后面的[],也没有，
    RDD<String> inputRDD = TextFile.withCharset(sc,"C:\\Users\\lenovo\\Desktop\\GF.json","UTF-8");
    JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String,Object>>(){
        public Map<String,Object> call(String s) throws Exception {
          System.out.println(s);
          HashMap<String,Object> result =new HashMap<String,Object>();
          JSONObject jo = JSON.parseObject(s); // 创建一个包含原始json串的json对象
          String satellite = jo.getString("satellite");
          String sensor = jo.getString("sensor");
          String productMode = jo.getString("productMode");
          String level = jo.getString("level");
          String spatialResolution =  jo.getString("spatialResolution");
          String temporalResolution = jo.getString("temporalResolution");
          String productType = jo.getString("productType");
          String productNameChn = jo.getString("productNameChn");
          String startTime = jo.getString("startTime");
          String endTime = jo.getString("endTime");
          String bandCount = jo.getString("bandCount");
          String location = jo.getString("boundary");
          String quickLook =  jo.getString("quickLook");
          String provider = jo.getString("provider");

          result.put("imageid", productNameChn);
          result.put("boundary", location);
          result.put("satellite", satellite);
          result.put("sensor", sensor);
          result.put("cloud", "30");
          result.put("level", level);
          result.put("resolution", spatialResolution);
          result.put("quick-look", quickLook);
          result.put("start-time", startTime);
          result.put("end-time", endTime);

          result.put("productMode", productMode);
          result.put("temporalResolution", temporalResolution);
          result.put("bandCount", bandCount);
          result.put("productType", productType);
          result.put("provider", provider);
          return result;
        }
      });
    //写入到索引
    EsSpark.saveToEs(rdd.rdd(), "image_gf/_doc");
  }
}
