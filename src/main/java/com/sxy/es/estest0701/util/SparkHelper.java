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
    public static void main(String[] args) {
       List<String> csvSchemaColumns = null;
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("CSVToES");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes","192.168.10.135");
        conf.set("es.port","9200");
        SparkContext sc = new SparkContext(conf);

        RDD<String> inputRDD = TextFile.withCharset(sc,"C:\\Users\\lenovo\\Desktop\\LANDSAT_TM_C1.csv","UTF-8");

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
          //第一行的列名
            csvSchemaColumns.add(i,iterator.next());
        }

        //执行读取csv文件数据，生成key,value对
        List<String> finalCsvSchemaColumns = csvSchemaColumns;
        JavaRDD rdd = inputRDD.toJavaRDD().map(new Function<String, Map<String,Object>>() {
            public Map<String, Object> call(String v1) throws Exception {
                HashMap<String,String> resmap =new HashMap<String,String>();
                HashMap<String,Object> result =new HashMap<String,Object>();
                String[] fields=v1.split(",");
               //需要的字段在表格中的位置，0开头
//                List<Integer> l = Arrays.asList(3,4,15,21,26,28,30,31,32,33,35,36,38,42);
                List<Integer> l = Arrays.asList(3,4,5,11,13,15,21,22,26,28,30,31,32,33,35,36,38,41,42);
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
                    double sceneCenterLongitude = Double.parseDouble(resmap.get("sceneCenterLongitude"));
                    double sceneCenterLatitude = Double.parseDouble(resmap.get("sceneCenterLatitude"));
                    String sceneStartTime = resmap.get("sceneStartTime");
                    String sceneStopTime = resmap.get("sceneStopTime");

                    Map<String, String> p1 = new HashMap<String,String>();
                    p1.put("MAP_PROJECTION_L1", resmap.get("MAP_PROJECTION_L1"));
                    Map<String, String> p2 = new HashMap<String,String>();
                    p2.put("imageQuality1", resmap.get("imageQuality1"));
                    Map<String, String> p3 = new HashMap<String,String>();
                    p3.put("ELLIPSOID",resmap.get("ELLIPSOID"));
                    Map<String,Object> other_properties = new HashMap<String,Object>();
                    other_properties.put("p1",p1);
                    other_properties.put("p2",p2);
                    other_properties.put("p3",p3);

                    String wkt = "POLYGON ((" + lowerLeftCornerLongitude + " " + lowerLeftCornerLatitude + "," + lowerRightCornerLongitude + " " + lowerRightCornerLatitude + "," + upperRightCornerLongitude + " " + upperRightCornerLatitude + "," + upperLeftCornerLongitude + " " + upperLeftCornerLatitude + "," + lowerLeftCornerLongitude + " " + lowerLeftCornerLatitude + "))";
                    String centerPointWKT = "POINT(" + sceneCenterLongitude + " " + sceneCenterLatitude + ")";

                    result.put("imageid", resmap.get("LANDSAT_PRODUCT_ID"));
//                    result.put("location", wkt);
                    result.put("boundary", wkt);
                    result.put("center-point", centerPointWKT);
                    result.put("satellite", "Landsat");
                    result.put("sensor", "TM");
                    result.put("cloud", resmap.get("cloudCover"));
                    result.put("level", "1");
                    result.put("resolution", resmap.get("GRID_CELL_SIZE_REFLECTIVE"));
                    result.put("quick-look", resmap.get("browseURL"));

                    result.put("start-time",paseDateTomillise(sceneStartTime));
                    result.put("end-time", paseDateTomillise(sceneStopTime));
                    result.put("owner", "NASA");
                    result.put("other-properties", other_properties);
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
        EsSpark.saveToEs(rdd.rdd(), "images/_doc");
    }
    /**
   *  把"yyyy:day:HH:mm:ss"格式日期转换成毫秒
   *@param strDate
   *@return 转换后毫秒的值
   *@author hongj
   */
  public static long paseDateTomillise(String strDate){
    String[] s = strDate.split(":");
    int sum=0;
    for(int year=1970;year<Integer.valueOf(s[0]).intValue();year++){
      if(year%4==0&&year%100!=0||year%400==0){   //闰年和平年，一年的天数不同
        sum+=366;
      }else {
        sum+=365;
      }
    }
    long dmillis = (sum + Long.valueOf(s[1]).longValue())* 24L * 60L * 60000L;
    long hmillis = Long.valueOf(s[2]).longValue() * 60L * 60000L;
    long mmillis =  Long.valueOf(s[3]).longValue() * 60000L;
    long smillis = (long)Double.valueOf(s[4]).doubleValue() * 1000L;
    return dmillis+hmillis+mmillis+smillis;
  }

}
