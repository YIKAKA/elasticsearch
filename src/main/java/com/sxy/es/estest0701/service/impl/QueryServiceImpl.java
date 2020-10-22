package com.sxy.es.estest0701.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sxy.es.estest0701.model.SearchResult;
import com.sxy.es.estest0701.model.data;
import com.sxy.es.estest0701.service.QueryService;
import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.elasticsearch.search.sort.SortOrder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class QueryServiceImpl implements QueryService {

    static RestClientBuilder builder = RestClient.builder(
            new HttpHost("192.168.10.135",9200,"http")
    );
    static RestHighLevelClientHelper helper = new RestHighLevelClientHelper(builder);

    @Override
    public SearchResult search(String place, String address,int rank, String geometry, String relation, List<String> satellites,
                         List<String> sensors, List<String> levels, double minResolution, double maxResolution,
                         long startTime, long endTime, int start, int length, String objects, String shapefilePath) throws IOException, ParseException {
        SearchResult result = new SearchResult();
        List<data> datasSum = new ArrayList<>();
        SearchHits hits;
        List<String> satellitesSum;
        List<String> sensorsSum;
        SearchRequest searchRequest = new SearchRequest("images");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchResponse searchResponse = null;
        //place 地址 模糊查询--使用高德API，获取经纬度，构造POINT，与ES进行地理运算
        if (null != place){
            //根据地名获取WKT格式的字符串
            geometry = getWKTByGD(place);
        }
        //todo 地名检索 精准的矢量边界
        //address 精确查询--需要再建一个GADM全球行政区划的index
        if (null != address){
            String id = getIDByName(address, rank);
            GeoShapeQueryBuilder geoQuery = new GeoShapeQueryBuilder("boundary",id);
            switch (rank){
                case 1:
                    geoQuery.indexedShapeIndex("country");
                    break;
                case 2:
                    geoQuery.indexedShapeIndex("province");
                    break;
                case 3:
                    geoQuery.indexedShapeIndex("city");
                    break;
                case 4:
                    geoQuery.indexedShapeIndex("county");
                    break;
            }
            geoQuery.indexedShapePath("location");
            queryBuilder.filter(geoQuery);
        }
        //satellite
        if (null != satellites && satellites.size() != 0){
            for (String satellite: satellites) {
                queryBuilder.must(QueryBuilders.termQuery("satellite", satellite));
            }
        }
        //sensor
        if (null != sensors && sensors.size() != 0){
            for (String sensor: sensors
            ) {
                queryBuilder.must(QueryBuilders.termQuery("sensor", sensor));
            }
        }
        //level
        if (null != levels && levels.size() != 0){
            for (String level: levels) {
                queryBuilder.must(QueryBuilders.termQuery("level", level));
            }
        }
        //resoulution
        if (minResolution != 0) {
            queryBuilder.must(QueryBuilders.rangeQuery("resolution").gte(minResolution));
        }
        if (maxResolution != 0) {
            queryBuilder.must(QueryBuilders.rangeQuery("resolution" ).lte(maxResolution));
        }
        //time
        if (startTime != 0) {
            queryBuilder.must(QueryBuilders.rangeQuery("start-time").gte(startTime));
        }
        if (endTime != 0) {
            queryBuilder.must(QueryBuilders.rangeQuery("end-time").lte(endTime));
        }
        //todo 上传shapefile矢量文件
        if (null != geometry){
            WKTReader wktReader = new WKTReader();
            Geometry geom = wktReader.read(geometry);
            ShapeBuilder shapeBuilder = null;
            if("Point".equals(geom.getGeometryType())){
                shapeBuilder = new PointBuilder(geom.getCoordinate().x,geom.getCoordinate().y);
            }else if ("LineString".equals(geom.getGeometryType())){
                shapeBuilder = new LineStringBuilder(new CoordinatesBuilder().coordinates(geom.getCoordinates()).close());
            }else if ("Polygon".equals(geom.getGeometryType())){
                shapeBuilder = new PolygonBuilder(new CoordinatesBuilder().coordinates(geom.getCoordinates()).close());
            }
            GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("boundary", shapeBuilder);
            switch (relation) {
                case "WITHIN":
                    geoQuery.relation(ShapeRelation.WITHIN);
                case "CONTAINS":
                    geoQuery.relation(ShapeRelation.CONTAINS);
                case "DISJOINT":
                    geoQuery.relation(ShapeRelation.DISJOINT);
                default:
                    geoQuery.relation(ShapeRelation.INTERSECTS);
            }
            queryBuilder.filter(geoQuery);
        }

        searchSourceBuilder.query(queryBuilder);
        //每页可以显示多少
        searchSourceBuilder.size(length);
        //先是排序完的
        //无法用landsat_id排序
        searchSourceBuilder.sort("imageid",SortOrder.DESC);
        //不是第一个
        //之后是需要传入上一页的sortvalue
        if(!"start".equals(objects)) {
            //为什么一定要object[]类型，因为sort字段可以设置多个，而且类型也不一样
            Object[] objects1 = new Object[1];
            objects1[0] = objects;
            searchSourceBuilder.searchAfter(objects1);
        }
        searchSourceBuilder.trackTotalHits(true);
        //查询
        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest.source().toString());
        searchResponse = helper.search(searchRequest);
        hits = searchResponse.getHits();
        //todo 没有结果的时候
        for (SearchHit hit : hits) {
            Map<String, Object> item = hit.getSourceAsMap();//结果取成MAP
            //处理每一条记录
            data tanSat  = new data();
            tanSat.setBoundary(item.get("boundary").toString());
            tanSat.setCloud(item.get("cloud").toString());
            tanSat.setOtherProperties(item.get("other-properties"));
            tanSat.setResolution(item.get("resolution").toString());
            tanSat.setSatellite(item.get("satellite").toString());
            tanSat.setSensor(item.get("sensor").toString());
            tanSat.setTime(item.get("start-time").toString());
            tanSat.setImageID(item.get("imageid").toString());
//            tanSat.setHasEntity(item.get("hasEntity").toString());
            datasSum.add(tanSat);
        }
        //获取所有结果的卫星
        Map<String,List<data>> satelliteListMap=datasSum.stream()
          .collect(Collectors.groupingBy(data::getSatellite));
        Set<String> satelliteListSet = satelliteListMap.keySet();
        satellitesSum = new  ArrayList<String>(satelliteListSet);
        //获取所有结果的传感器
        Map<String,List<data>> sensorListMap=datasSum.stream()
          .collect(Collectors.groupingBy(data::getSensor));
        Set<String> sensorListSet = sensorListMap.keySet();
        sensorsSum = new  ArrayList<String>(sensorListSet);
        //searchafter 参数
        if (hits.getHits().length == 0){
            Object[] imageid = new Object[]{"start"};
            objects = imageid[0].toString();
        }else {
            Object[] imageid = hits.getHits()[hits.getHits().length - 1].getSortValues();
            objects = imageid[0].toString();
        }
        TotalHits totalHits = hits.getTotalHits();
        //the total number of hits,must be interpreted in the context of totalHits.relation
        long numHits = totalHits.value;
        result.setTotalCount(numHits);
        result.setDatas(datasSum);
        result.setSatellites(satellitesSum);
        result.setSensors(sensorsSum);
        result.setObjects(objects);
        return result;
    }

    public static String getWKTByGD(String place){
       String path = "https://restapi.amap.com/v3/geocode/geo?address="+place+"&output=JSON&key=285fa381b04ac0b349db564c7660adcf";
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        String result = null;
        String wkt = null;
        try {
            // 创建远程url连接对象
            URL url = new URL(path);
            // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接方式：get
            connection.setRequestMethod("GET");
            // 设置连接主机服务器的超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取远程返回的数据时间：60000毫秒
            connection.setReadTimeout(60000);
            // 发送请求
            connection.connect();
            // 通过connection连接，获取输入流
            if (connection.getResponseCode() == 200) {
                is = connection.getInputStream();
                // 封装输入流is，并指定字符集
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                // 存放数据
                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
                JSONObject jsonObject = JSON.parseObject(result);
                String location = jsonObject.getJSONArray("geocodes").get(0).toString();
                JSONObject locationJSON = JSON.parseObject(location);
                String jw = locationJSON.get("location").toString();
                //将，替换为空格
                wkt = "POINT("+jw.replace(","," ")+")";
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            connection.disconnect();// 关闭远程连接
        }
        return wkt;
    }

    public static String getIDByName(String address,int rank) throws IOException {
        String geom = "";
        String id = null;
        SearchRequest searchRequest = null;
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchResponse searchResponse = null;
        switch (rank){
            case 1:
                searchRequest = new SearchRequest("country");
                break;
            case 2:
                searchRequest = new SearchRequest("province");
                break;
            case 3:
                searchRequest = new SearchRequest("city");
                break;
            case 4:
                searchRequest = new SearchRequest("county");
                break;
        }
        queryBuilder.must(QueryBuilders.multiMatchQuery(address,"name1","name2","name3"));
        searchSourceBuilder.size(1);
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest.source().toString());
        searchResponse = helper.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        //todo 没有结果的时候
        for (SearchHit hit : hits){
            id =  hit.getId();
        }
        return id;
    }
}
