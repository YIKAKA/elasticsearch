package com.sxy.es.estest0701.service.impl;

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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;

@Slf4j
@Service
public class QueryServiceImpl implements QueryService {

    static RestClientBuilder builder = RestClient.builder(
            new HttpHost("192.168.10.135",9200,"http")
    );
    static RestHighLevelClientHelper helper = new RestHighLevelClientHelper(builder);

    @Override
    public SearchResult search(String place, String address, String geometry, String relation, List<String> satellites,
                         List<String> sensors, List<String> levels, double minResolution, double maxResolution,
                         long startTime, long endTime, int start, int length, String objects, String shapefilePath) throws IOException, ParseException {
        SearchResult result = new SearchResult();
        List<data> datasSum = new ArrayList<>();
        SearchHits hits = null;
        List<String> satellitesSum = Arrays.asList("landsat","sentinel");
        List<String> sensorsSum = Arrays.asList("TM","GTM+");
        SearchRequest searchRequest = new SearchRequest("landsat");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchResponse searchResponse = null;
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
        queryBuilder.filter(geoQuery);

        searchSourceBuilder.query(queryBuilder);
        //每页可以显示多少
        searchSourceBuilder.size(length);
        //先是排序完的
        //无法用landsat_id排序
        searchSourceBuilder.sort("_id",SortOrder.DESC);
        //不是第一个
        //之后是需要传入上一页的sortvalue
        if("start".equals(objects)) {
//        if(!objects.get(0).toString().equals("start")) {
            //为什么一定要object[]类型，因为sort字段可以设置多个，而且类型也不一样
            searchSourceBuilder.searchAfter(new List[]{Collections.singletonList(objects)});
        }
        searchSourceBuilder.trackTotalHits(true);
        //查询
        searchRequest.source(searchSourceBuilder);
        searchResponse = helper.search(searchRequest);
        hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> item = hit.getSourceAsMap();//结果取成MAP
            //处理每一条记录
            data tanSat  = new data();
            tanSat.setBoundary("a");
            tanSat.setCloud("a");
            tanSat.setOtherProperties(null);
            tanSat.setResolution("a");
            tanSat.setSatellite("a");
            tanSat.setSensor("a");
            tanSat.setTime(objects);
            datasSum.add(tanSat);
        }
        objects = String.valueOf(hits.getHits()[hits.getHits().length - 1].getSortValues());
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
}
