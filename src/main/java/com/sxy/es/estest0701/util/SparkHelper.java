package com.sxy.es.estest0701.util;

import com.databricks.spark.csv.util.TextFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.spark.rdd.EsSpark;

import java.io.IOException;
import java.util.*;

public class SparkHelper {

//    private List<String> csvSchemaColumns = null;

    public static void main(String[] args) {
       List<String> csvSchemaColumns = null;
//    public int csvToES() throws IOException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("CSVToES");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes","192.168.137.81,192.168.137.82,192.168.137.83");
        conf.set("es.port","9200");
        SparkContext sc = new SparkContext(conf);

        RDD<String> inputRDD = TextFile.withCharset(sc,"F:\\es\\LANDSAT_TM_C1.csv\\LANDSAT_TM_C1.csv","UTF-8");

        String header = inputRDD.first();
//        String header = "LANDSAT_PRODUCT_ID,upperRightCornerLatitude,lowerLeftCornerLatitude,upperRightCornerLongitude,lowerRightCornerLongitude,lowerRightCornerLatitude,upperLeftCornerLongitude,lowerLeftCornerLongitude,upperLeftCornerLatitude";
        //获取csv的字段schema信息
        CSVRecord csvRecord = null;
        try {
            csvRecord = CSVParser.parse(header, CSVFormat.DEFAULT).getRecords().get(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        csvSchemaColumns = new ArrayList<String>();

        Iterator<String> iterator = csvRecord.iterator();
        for (int i = 0;iterator.hasNext();i++){
            csvSchemaColumns.add(i,iterator.next());
        }

        //执行读取csv文件数据，生成key,value对
        List<String> finalCsvSchemaColumns = csvSchemaColumns;
        JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String,Object>>() {
            public Map<String, Object> call(String v1) throws Exception {
                HashMap<String,String> resmap =new HashMap<String,String>();
                HashMap<String,Object> result =new HashMap<String,Object>();
                String[] fields=v1.split(",");
                List<Integer> l = Arrays.asList(3,21,26,28,31,32,35,38,42);
                l.forEach(i -> {
                        if(!finalCsvSchemaColumns.get(i).equals(fields[i])){
                            resmap.put(finalCsvSchemaColumns.get(i),fields[i]);
                            resmap.put(finalCsvSchemaColumns.get(i),fields[i]);
                        }else{
                            resmap.put("null","null");
                        }
                });
                if (resmap.get("null") == "null"){
                    result.put("null","null");
                }else {
                    double lowerLeftCornerLongitude = Double.parseDouble(resmap.get("lowerLeftCornerLongitude"));
                    double lowerLeftCornerLatitude = Double.parseDouble(resmap.get("lowerLeftCornerLatitude"));
                    double lowerRightCornerLongitude = Double.parseDouble(resmap.get("lowerRightCornerLongitude"));
                    double lowerRightCornerLatitude = Double.parseDouble(resmap.get("lowerRightCornerLatitude"));
                    double upperRightCornerLongitude = Double.parseDouble(resmap.get("upperRightCornerLongitude"));
                    double upperRightCornerLatitude = Double.parseDouble(resmap.get("upperRightCornerLatitude"));
                    double upperLeftCornerLongitude = Double.parseDouble(resmap.get("upperLeftCornerLongitude"));
                    double upperLeftCornerLatitude = Double.parseDouble(resmap.get("upperLeftCornerLatitude"));
                    String wkt = "POLYGON ((" + lowerLeftCornerLongitude + " " + lowerLeftCornerLatitude + "," + lowerRightCornerLongitude + " " + lowerRightCornerLatitude + "," + upperRightCornerLongitude + " " + upperRightCornerLatitude + "," + upperLeftCornerLongitude + " " + upperLeftCornerLatitude + "," + lowerLeftCornerLongitude + " " + lowerLeftCornerLatitude + "))";

                    result.put("LANDSAT_PRODUCT_ID", resmap.get("LANDSAT_PRODUCT_ID"));
                    result.put("location", wkt);
                }
                return result;
            }
            //过滤掉首行的列名
        }).filter(new Function<Map<String, Object>, Boolean>() {
            public Boolean call(Map<String, Object> v1) throws Exception {
                return v1.get("null")!="null";
            }
        });
        //写入到索引
        EsSpark.saveToEs(rdd.rdd(), "landsat02/_doc");

//        return 0;
    }


}
