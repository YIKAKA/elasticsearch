package com.sxy.es.estest0701.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.databricks.spark.csv.util.TextFile;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.spark.rdd.EsSpark;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadGeoJSON {

  public static void main(String[] args){
    SparkConf conf = new SparkConf();
    conf.setMaster("local");
    conf.setAppName("CSVToES");
    conf.set("es.nodes","192.168.10.135");
    conf.set("es.port","9200");
    SparkContext sc = new SparkContext(conf);

    RDD<String> inputRDD = TextFile.withCharset(sc,"E:\\xm\\es20200720\\地名地址库\\GADM\\china\\china2.geojsonl.json","UTF-8");
    JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String,Object>>(){
        public Map<String,Object> call(String s) throws Exception {
          HashMap<String,Object> result =new HashMap<String,Object>();
          JSONObject jo = JSON.parseObject(s); // 创建一个包含原始json串的json对象
          JSONObject properties = jo.getJSONObject("properties");
          String name_0 = properties.getString("NAME_2");
          String name_1 = properties.getString("VARNAME_2");
          String name_2 = properties.getString("NL_NAME_2");
          String country = properties.getString("NAME_0");
          String province = properties.getString("NAME_1");
//          String city = properties.getString("NAME_2");
          JSONObject geometry =  jo.getJSONObject("geometry");
          result.put("name1",name_0);
          result.put("name2",name_1);
          result.put("name3",name_2);
          result.put("location",geometry);
          result.put("country",country);
          result.put("province",province);
//          result.put("city",city);
          return result;
        }
      });
    //写入到索引
    EsSpark.saveToEs(rdd.rdd(), "city/_doc");
  }
}
