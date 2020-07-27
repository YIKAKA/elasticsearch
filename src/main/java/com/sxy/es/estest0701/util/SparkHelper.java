package com.sxy.es.estest0701.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import com.databricks.spark.csv.util.TextFile;
import org.elasticsearch.spark.rdd.EsSpark;

import java.io.IOException;
import java.util.*;

public class SparkHelper {

    private List<String> csvSchemaColumns = null;

    public int csvToES() throws IOException {
        SparkConf conf = new SparkConf();
        conf.setMaster("ubts81");
        conf.setAppName("CSVToES");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes","192.168.137.81");
        conf.set("es.nodes","192.168.137.82");
        conf.set("es.nodes","192.168.137.83");
        conf.set("es.port","9200");
        SparkContext sc = new SparkContext(conf);

        RDD<String> inputRDD = TextFile.withCharset(sc,"F:\\es\\LANDSAT_TM_C1.csv\\LANDSAT_TM_C1.csv","UTF-8");

        String header = inputRDD.first();

        //获取csv的字段schema信息
        CSVRecord csvRecord = CSVParser.parse(header, CSVFormat.DEFAULT).getRecords().get(0);
        csvSchemaColumns = new ArrayList<String>();

        Iterator<String> iterator = csvRecord.iterator();
        for (int i = 0;iterator.hasNext();i++){
            csvSchemaColumns.add(i,iterator.next());
        }

        //执行读取csv文件数据，生成key,value对
        JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String,String>>() {
            public Map<String, String> call(String v1) throws Exception {
                HashMap<String,String> resmap=new HashMap<String,String>();
                String[] fields=v1.split(",");
                for(int i=0;i<csvSchemaColumns.size();i++){
                    if(!csvSchemaColumns.get(i).equals(fields[i])){
                        resmap.put(csvSchemaColumns.get(i),fields[i]);
                    }else{
                        resmap.put("null","null");
                    }
                }
                return resmap;
            }
            //过滤掉首行的列名
        }).filter(new Function<Map<String, String>, Boolean>() {
            public Boolean call(Map<String, String> v1) throws Exception {
                return v1.get("null")!="null";
            }
        });
        //写入到索引为 spark，type为 docs4 下
        EsSpark.saveToEs(rdd.rdd(), "spark/docs");

        return 0;
    }


}
