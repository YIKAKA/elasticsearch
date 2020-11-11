package com.sxy.es.estest0701.util;

import com.databricks.spark.csv.util.TextFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.elasticsearch.spark.rdd.EsSpark;

import java.io.IOException;
import java.util.*;

public class CreateLandsat {
    public static void main(String[] args) {
       List<String> csvSchemaColumns = null;
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("CSVToES");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes","192.168.2.23");
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
                List<Integer> l = Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
                ,43,44,45,46,47,48,49);
//                List<Integer> l = Arrays.asList(3,4,5,11,13,15,21,22,26,28,30,31,32,33,35,36,38,41,42);
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
                    String THERMAL_LINES = resmap.get("THERMAL_LINES");
                    String sunAzimuth = resmap.get("sunAzimuth");
                    String REFLECTIVE_SAMPLES = resmap.get("REFLECTIVE_SAMPLES");
                    String MAP_PROJECTION_L1 = resmap.get("MAP_PROJECTION_L1");
                    String cartURL = resmap.get("cartURL");
                    String sunElevation = resmap.get("sunElevation");
                    String path = resmap.get("path");
                    String GROUND_CONTROL_POINTS_MODEL = resmap.get("GROUND_CONTROL_POINTS_MODEL");
                    String row = resmap.get("row");
                    String imageQuality1 = resmap.get("imageQuality1");
                    String REFLECTIVE_LINES = resmap.get("REFLECTIVE_LINES");
                    String ELLIPSOID = resmap.get("ELLIPSOID");
                    String GEOMETRIC_RMSE_MODEL = resmap.get("GEOMETRIC_RMSE_MODEL");
                    String browseAvailable = resmap.get("browseAvailable");
                    String dayOrNight = resmap.get("dayOrNight");
                    String CPF_NAME = resmap.get("CPF_NAME");
                    String DATA_TYPE_L1 = resmap.get("DATA_TYPE_L1");
                    String THERMAL_SAMPLES = resmap.get("THERMAL_SAMPLES");
                    String dateUpdated = resmap.get("dateUpdated");
                    String sensor = resmap.get("sensor");
                    String GROUND_CONTROL_POINTS_VERSION = resmap.get("GROUND_CONTROL_POINTS_VERSION");
                    String acquisitionDate = resmap.get("acquisitionDate");
                    String PROCESSING_SOFTWARE_VERSION = resmap.get("PROCESSING_SOFTWARE_VERSION");
                    String COLLECTION_CATEGORY = resmap.get("COLLECTION_CATEGORY");
                    String CLOUD_COVER_LAND = resmap.get("CLOUD_COVER_LAND");
                    String GEOMETRIC_RMSE_MODEL_X = resmap.get("GEOMETRIC_RMSE_MODEL_X");
                    String GEOMETRIC_RMSE_MODEL_Y = resmap.get("GEOMETRIC_RMSE_MODEL_Y");
                    String UTM_ZONE = resmap.get("UTM_ZONE");
                    String DATE_L1_GENERATED = resmap.get("DATE_L1_GENERATED");
                    String GRID_CELL_SIZE_THERMAL = resmap.get("GRID_CELL_SIZE_THERMAL");
                    String DATUM = resmap.get("DATUM");
                    String COLLECTION_NUMBER = resmap.get("COLLECTION_NUMBER");
                    String sceneID = resmap.get("sceneID");
                    String receivingStation = resmap.get("receivingStation");


                    String wkt = "POLYGON ((" + lowerLeftCornerLongitude + " " + lowerLeftCornerLatitude + "," + lowerRightCornerLongitude + " " + lowerRightCornerLatitude + "," + upperRightCornerLongitude + " " + upperRightCornerLatitude + "," + upperLeftCornerLongitude + " " + upperLeftCornerLatitude + "," + lowerLeftCornerLongitude + " " + lowerLeftCornerLatitude + "))";
                    String centerPointWKT = "POINT(" + sceneCenterLongitude + " " + sceneCenterLatitude + ")";

                    result.put("imageid", resmap.get("LANDSAT_PRODUCT_ID"));
                    result.put("boundary", wkt);
                    result.put("center-point", centerPointWKT);
                    result.put("satellite", "Landsat");
                    result.put("sensor", sensor);
                    result.put("cloud", resmap.get("cloudCover"));
                    result.put("level", DATA_TYPE_L1);
                    result.put("resolution", resmap.get("GRID_CELL_SIZE_REFLECTIVE"));
                    result.put("quick-look", resmap.get("browseURL"));
                    result.put("start-time",paseDateTomillise(sceneStartTime));
                    result.put("end-time", paseDateTomillise(sceneStopTime));

                    result.put("row", row);
                    result.put("sunAzimuth", sunAzimuth);
                    result.put("REFLECTIVE_SAMPLES", REFLECTIVE_SAMPLES);
                    result.put("MAP_PROJECTION_L1", MAP_PROJECTION_L1);
                    result.put("cartURL", cartURL);
                    result.put("sunElevation", sunElevation);
                    result.put("path", path);
                    result.put("GROUND_CONTROL_POINTS_MODEL", GROUND_CONTROL_POINTS_MODEL);
                    result.put("THERMAL_LINES", THERMAL_LINES);
                    result.put("imageQuality1", imageQuality1);
                    result.put("REFLECTIVE_LINES", REFLECTIVE_LINES);
                    result.put("ELLIPSOID", ELLIPSOID);
                    result.put("GEOMETRIC_RMSE_MODEL", GEOMETRIC_RMSE_MODEL);
                    result.put("browseAvailable", browseAvailable);
                    result.put("dayOrNight", dayOrNight);
                    result.put("CPF_NAME", CPF_NAME);
                    result.put("THERMAL_SAMPLES", THERMAL_SAMPLES);
                    result.put("dateUpdated", dateUpdated);
                    result.put("GROUND_CONTROL_POINTS_VERSION", GROUND_CONTROL_POINTS_VERSION);
                    result.put("acquisitionDate", acquisitionDate);
                    result.put("PROCESSING_SOFTWARE_VERSION", PROCESSING_SOFTWARE_VERSION);
                    result.put("COLLECTION_CATEGORY", COLLECTION_CATEGORY);
                    result.put("CLOUD_COVER_LAND", CLOUD_COVER_LAND);
                    result.put("GEOMETRIC_RMSE_MODEL_X", GEOMETRIC_RMSE_MODEL_X);
                    result.put("GEOMETRIC_RMSE_MODEL_Y", GEOMETRIC_RMSE_MODEL_Y);
                    result.put("UTM_ZONE", UTM_ZONE);
                    result.put("DATE_L1_GENERATED", DATE_L1_GENERATED);
                    result.put("GRID_CELL_SIZE_THERMAL", GRID_CELL_SIZE_THERMAL);
                    result.put("DATUM", DATUM);
                    result.put("COLLECTION_NUMBER", COLLECTION_NUMBER);
                    result.put("sceneID", sceneID);
                    result.put("receivingStation", receivingStation);

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
        EsSpark.saveToEs(rdd.rdd(), "image_landsat/_doc");
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
