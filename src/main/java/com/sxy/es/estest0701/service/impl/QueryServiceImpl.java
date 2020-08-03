package com.sxy.es.estest0701.service.impl;


import com.sxy.es.estest0701.config.CommonSetting;
import com.sxy.es.estest0701.model.SearchResult;
import com.sxy.es.estest0701.service.QueryService;
import com.sxy.es.estest0701.tool.Constant;
import com.sxy.es.estest0701.type.Tuple3;
import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.json.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class QueryServiceImpl implements QueryService {
    @Autowired
    CommonSetting setting;

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

    public Tuple3<Integer, Envelope, List<Map<String, Object>>> toJson(SearchResponse response, Geometry filter
            , ShapeRelation relation, List<String> includes, List<String> excludes, boolean hasDistance) {
        List<Map<String, Object>> records = new ArrayList<>();
        List<Map<String, Object>> entities = new ArrayList<>();
        boolean hasPoint = true;
        boolean hasGeometry = true;
        if (excludes != null && (excludes.contains(Constant.LON) || excludes.contains(Constant.LAT)))
            hasPoint = false;
        if (excludes != null && excludes.contains(Constant.GEOMETRY))
            hasGeometry = false;
        Envelope envelope = new Envelope();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> item = hit.getSourceAsMap();
            //处理每一条记录
            int priority = (int) item.getOrDefault(Constant.PRIORITY, 0);
            item.put("_score", hit.getScore() - priority * 0.42f);
            if (hasDistance) {
                Object[] sorts = hit.getSortValues();
                if (sorts != null && sorts.length > 0)
                    item.put(Constant.DISTANCE, String.format("%.2f米", (double) sorts[0]));
            }
            if (item.containsKey(Constant.ES_POINT)) {
                Map<String, Object> thePoint = (Map<String, Object>) item.remove(Constant.ES_POINT);
                if (thePoint != null) {
                    double lng = (double) thePoint.get(Constant.LON);
                    double lat = (double) thePoint.get(Constant.LAT);
                    Geometry geom = geometryFactory.createPoint(new Coordinate(lng, lat));
                    if (filter != null && !satisfy(filter, geom, relation))
                        continue;//跳过当前记录
                    if (hasPoint) {
                        item.put(Constant.LNG, BigDecimal.valueOf(lng).setScale(6, RoundingMode.DOWN).doubleValue());
                        item.put(Constant.LAT, BigDecimal.valueOf(lat).setScale(6, RoundingMode.DOWN).doubleValue());
                    }
                    envelope.expandToInclude(lng, lat);
                }
            }
            if (item.containsKey(Constant.ES_SHAPE)) {
//                try {
                    Map<String, Object> theShape = (Map<String, Object>) item.remove(Constant.ES_SHAPE);
                    if (theShape != null) {
                        GeometryJSON gjson = new GeometryJSON();
                        JSONObject geojson = new JSONObject(theShape);
                        Reader reader = new StringReader(geojson.toString());
//                        Geometry geom = (Geometry)gjson.read(reader);  问题！！！！！！！！
                        Geometry geom = null;
                        if (filter != null && !satisfy(filter, geom, relation))
                            continue;//跳过当前记录
                        geom = TopologyPreservingSimplifier.simplify(geom, setting.getGeomDistTolerance());
                        if (hasGeometry)
                            item.put(Constant.GEOMETRY, wktWriter.write(geom));
                        if (geom instanceof LineString || geom instanceof MultiLineString) {
                            Coordinate[] coors = geom.getCoordinates();
                            int offset = (coors.length + 1) / 2;
                            item.put(Constant.LNG, coors[offset].getX());
                            item.put(Constant.LAT, coors[offset].getY());
                        } else {
                            Point center = geom.getCentroid();
                            item.put(Constant.LNG, center.getX());
                            item.put(Constant.LAT, center.getY());
                        }
                        envelope.expandToInclude(geom.getEnvelopeInternal());
                    }
//                } catch (IOException e) {
//                    log.error("读取geojson失败", e);
//                }
            }
            String dtype = (String) item.get(Constant.DTYPE);
            if (dtype.startsWith("fe_")) {
                entities.add(item);//添加到实体结果集
            }
            records.add(item);//添加到结果集
        }
        //不进行数据聚合取匹配度前5的实体记录进行优先排序
        List<Map<String, Object>> subEntities = entities.stream()
                .sorted(Comparator.comparingDouble(s -> -(float) s.get("_score"))).limit(5).collect(Collectors.toList());
        records.removeAll(subEntities);
        records.sort(Comparator.comparingDouble(s -> -(float) s.get("_score")));
        records.addAll(0, subEntities);
        records.forEach(map -> {
            map.remove("_index");
            map.remove("_type");
            map.remove("_id");
            map.remove("_score");
//            map.remove("dtype");
//            map.remove("entiid6");
//            map.remove("the_point");
//            map.remove("the_shape");
//            map.remove("keywords");
            if (excludes != null)
                excludes.forEach(map::remove);
            if (includes != null && !includes.isEmpty())
                map.keySet().retainAll(includes);
        });
        return new Tuple3<>(entities.size(), envelope, records);
    }

    @Override
    public SearchResult response2Result(Integer page, Integer pagecap, SearchResponse response, Geometry geomFilter, List<String> excludes, String orderBy, Object defaultValue, boolean numberOrder, boolean asc) {
        SearchResult result = new SearchResult();
        result.setCurpage(page);
        result.setPagecount(pagecap);
        result.setCurresult(response.getHits().getHits().length);
        result.setTotal(response.getHits().getTotalHits().value);
        result.setTime(response.getTook().getSecondsFrac());

        Tuple3<Integer, Envelope, List<Map<String, Object>>> tp = toJson(response, geomFilter, null, Collections.emptyList(), excludes, false);
        result.setEntitytotal(tp._1());
        Envelope envelope = tp._2();
        if (envelope != null && !envelope.isNull())
            result.setBound(String.format("%.6f,%.6f,%.6f,%.6f", envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()));
        List<Map<String, Object>> features = tp._3();
        if (orderBy != null && !orderBy.isEmpty() && defaultValue != null) {
            Comparator<Map<String, Object>> comparator = (m1, m2) -> {
                if (numberOrder) {
                    return ((double) m1.getOrDefault(orderBy, defaultValue) - (double) m2.getOrDefault(orderBy, defaultValue)) > 0
                            ? 1 : -1;
                } else {
                    return m1.getOrDefault(orderBy, defaultValue).toString().compareTo(m2.getOrDefault(orderBy, defaultValue).toString());
                }
            };
            if (!asc)
                comparator = comparator.reversed();
            features.sort(comparator);
        }
        if (pagecap == null)
            pagecap = 10;
        if (pagecap < features.size())
            features = new ArrayList<>(features.subList(0, pagecap));
        result.setFeatures(features);
        return result;
    }

    public void geoSearch() throws IOException {
        //restricts the request to an index
        SearchRequest searchRequest = new SearchRequest("landsat02");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //Utility class to create search queries.
        double[] x = new double[];
        org.elasticsearch.geometry.LinearRing linearRing = new org.elasticsearch.geometry.LinearRing();
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("location",
                new org.elasticsearch.geometry.Polygon(linearRing)
        ));
        geoQuery.relation(ShapeRelation.INTERSECTS);

        searchSourceBuilder.query(geoQuery);

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
        System.out.println(successfulShards);
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure:searchResponse.getShardFailures()){
            System.out.println(failure);
        }
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
    public static void main(String[] args) {
        QueryServiceImpl q = new QueryServiceImpl();
        try {
            q.search();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
