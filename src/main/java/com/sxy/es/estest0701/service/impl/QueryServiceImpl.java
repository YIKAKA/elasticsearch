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
    Object[] objects = null;
    SearchHits hits = null;

    @Override
    public SearchResult search(String place, String address, String geometry, String relation, List<String> satellites,
                         List<String> sensors, List<String> levels, double minResolution, double maxResolution,
                         long startTime, long endTime, int start, int length, String shapefilePath) throws IOException, ParseException {
        SearchResult result = new SearchResult();
        List<data> datasSum = new ArrayList<>();
        List<String> satellitesSum = Arrays.asList("landsat","sentinel");
        List<String> sensorsSum = Arrays.asList("TM","GTM+");
        //只有当第一页时才会初始化，之后就直接那前一页最后的sortvalue
        if (start == 1){
            objects = new Object[]{"start"};
            hits = null;
        }
//        int i = 0;
        //start 是第几页，length 这一页有几条数据
        //start最开始是1
        //第2页需要传入第一页最后的sortvalue
        //前端也是一页一页找，所以可以设置成全局
        //或者标识一下这是第几次查询，点击加载下一页就是2。
//        while (i < start){
            SearchResponse searchResponse = searchAfter(objects, place, address, geometry, relation, satellites, sensors, levels, minResolution, maxResolution,
              startTime, endTime, start, length, shapefilePath);
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
                tanSat.setTime(objects.toString());
                datasSum.add(tanSat);
            }
            objects = hits.getHits()[hits.getHits().length-1].getSortValues();
//            i++;
//        }
        TotalHits totalHits = hits.getTotalHits();
        //the total number of hits,must be interpreted in the context of totalHits.relation
        long numHits = totalHits.value;
        result.setTotalCount(numHits);
        result.setDatas(datasSum);
        result.setSatellites(satellitesSum);
        result.setSensors(sensorsSum);
        return result;
    }

    public static SearchResponse searchAfter(Object[] objects, String place, String address, String geometry, String relation, List<String> satellites,
                                             List<String> sensors, List<String> levels, double minResolution, double maxResolution,
                                             long startTime, long endTime, int start, int length, String shapefilePath) throws ParseException, IOException {
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
        searchSourceBuilder.sort("_id",SortOrder.DESC);
        //不是第一个
        //之后是需要传入上一页的sortvalue
        if(!objects[0].toString().equals("start")) {
            searchSourceBuilder.searchAfter(objects);
        }
        searchSourceBuilder.trackTotalHits(true);
        searchRequest.source(searchSourceBuilder);
        searchResponse = helper.search(searchRequest);
        return searchResponse;
    }

}
