package com.sxy.es.estest0701.service.impl;

import com.sxy.es.estest0701.model.SearchResult;
import com.sxy.es.estest0701.service.QueryService;
import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class QueryServiceImpl implements QueryService {

    RestClientBuilder builder = RestClient.builder(
            new HttpHost("192.168.137.81",9200,"http"),
            new HttpHost("192.168.137.82",9200,"http"),
            new HttpHost("192.168.137.83",9200,"http")
    );
    RestHighLevelClientHelper helper = new RestHighLevelClientHelper(builder);

    @Autowired
    public void search() throws IOException {
        //restricts the request to an index
        SearchRequest searchRequest = new SearchRequest("landsat02");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("LANDSAT_PRODUCT_ID","LT05_L1GS_002055_20060610_20161121_01_T2"));
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = helper.search(searchRequest);

        //details about the search execution itself ,access to the documents returned
        RestStatus status = searchResponse.status();//the HTTP status code
        System.out.println(status);
        TimeValue took = searchResponse.getTook();//execution time
        System.out.println(took);
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();//whether the request terminated early
        boolean timeout = searchResponse.isTimedOut();//timed out

        //information about the execution on the shard level
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
//        for (ShardSearchFailure failure:searchResponse.getShardFailures()){
//
//        }
//        get access to the returned documents
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();
        //the total number of hits,must be interpreted in the context of totalHits.relation
        long numHits = totalHits.value;
        //whether the number of hits is accurateEQUAL_TO ,or a lower bound of the total GREATER_THAN_OR_EQUAL_TO
        TotalHits.Relation relation = totalHits.relation;
        float maxScore = hits.getMaxScore();
        System.out.println(searchResponse.getHits().getHits().length);
        System.out.println(searchResponse.getHits().getTotalHits().value);
        System.out.println(searchResponse.getTook().getSecondsFrac());
    }

    public SearchResult geoSearch(String wkt,String relation) throws IOException, ParseException {
        //restricts the request to an index
        SearchRequest searchRequest = new SearchRequest("landsat02");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        String wkt =  "POLYGON((-45 45, -45 -45, 45 -45, 45 45,-45 45))";
        //Utility class to create search queries.
        WKTReader wktReader = new WKTReader();
        Geometry geom = wktReader.read(wkt);
        ShapeBuilder shapeBuilder = new PolygonBuilder(new CoordinatesBuilder().coordinates(geom.getCoordinates()).close());
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("location", shapeBuilder);
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
        searchSourceBuilder.query(geoQuery);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = helper.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        List<Map<String, Object>> records = new ArrayList<>();
//        GeometryFactory geometryFactory = new GeometryFactory();
//        WKTWriter wktWriter = new WKTWriter();
//        GeometryJSON geometryJSON = new GeometryJSON();
        for (SearchHit hit : hits) {
            Map<String, Object> item = hit.getSourceAsMap();//结果取成MAP
            //****以下的代码是对map结果的处理，把经纬度单独拿出来，后续按照需求再选择是否需要。****/
//            //处理每一条记录
//            int priority = (int) item.getOrDefault(Constant.PRIORITY, 0);//JDK8检查一个map中匹配提供键的值是否找到，没找到匹配项就使用一个默认值
//            item.put("_score", hit.getScore() - priority * 0.42f);//priority是index的优先权
////            //如果包含了geo_point
//            if (item.containsKey(Constant.ES_POINT)) {
//                Map<String, Object> thePoint = (Map<String, Object>) item.remove(Constant.ES_POINT);
//                if (thePoint != null) {
//                    double lng = (double) thePoint.get(Constant.LON);
//                    double lat = (double) thePoint.get(Constant.LAT);
//                    item.put(Constant.LNG, BigDecimal.valueOf(lng).setScale(6, RoundingMode.DOWN).doubleValue());
//                    item.put(Constant.LAT, BigDecimal.valueOf(lat).setScale(6, RoundingMode.DOWN).doubleValue());
//                }
//            }
////            //如果包含了geo_shape
//            if (item.containsKey(Constant.ES_SHAPE)) {
//                try {
//                    Map<String, Object> theShape = (Map<String, Object>) item.remove(Constant.ES_SHAPE);
//                    if (theShape != null) {
//                        JSONObject geojson = new JSONObject(theShape);
//                         Geometry geom1 = geometryJSON.read(geojson.toString());
//                        if (geom1 instanceof LineString || geom1 instanceof MultiLineString) {
//                            Coordinate[] coors = geom1.getCoordinates();
//                            int offset = (coors.length + 1) / 2;
//                            //中间位置
//                            item.put(Constant.LNG, coors[offset].x);
//                            item.put(Constant.LAT, coors[offset].y);
//                        } else {
//                            Point center = geom1.getCentroid();
//                            item.put(Constant.LNG, center.getX());
//                            item.put(Constant.LAT, center.getY());
//                        }
//                    }
//                } catch (IOException e) {
//                    log.error("读取geojson失败", e);
//                }
//            }
            records.add(item);//添加到结果集
        }
        SearchResult searchResult = new SearchResult();
        searchResult.setCurpage(0);
        searchResult.setPagecount(0);
        searchResult.setCurresult(searchResponse.getHits().getHits().length);
        searchResult.setTotal(searchResponse.getHits().getTotalHits().value);
        searchResult.setTime(searchResponse.getTook().getSecondsFrac());
        searchResult.setCluster("landsat");
        searchResult.setBound("");
        searchResult.setFeatures(records);
        return searchResult;
    }

    public SearchResult geoSearchByPreindexed() throws IOException {
        //restricts the request to an index
        SearchRequest searchRequest = new SearchRequest("landsat02");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        GeoShapeQueryBuilder qb = QueryBuilders.geoShapeQuery("location","deu");
        qb.relation(ShapeRelation.INTERSECTS);
        qb.indexedShapeIndex("shapes");
        qb.indexedShapePath("location");

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query.filter(qb);

        searchSourceBuilder.query(query);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = helper.search(searchRequest);

        SearchHits hits = searchResponse.getHits();
        List<Map<String, Object>> records = new ArrayList<>();
        for (SearchHit hit : hits) {
            Map<String, Object> item = hit.getSourceAsMap();//结果取成MAP
            records.add(item);//添加到结果集
        }
        SearchResult searchResult = new SearchResult();
        searchResult.setCurpage(0);
        searchResult.setPagecount(0);
        searchResult.setCurresult(searchResponse.getHits().getHits().length);
        searchResult.setTotal(searchResponse.getHits().getTotalHits().value);
        searchResult.setTime(searchResponse.getTook().getSecondsFrac());
        searchResult.setCluster("landsat");
        searchResult.setBound("");
        searchResult.setFeatures(records);
        return searchResult;
    }
}
