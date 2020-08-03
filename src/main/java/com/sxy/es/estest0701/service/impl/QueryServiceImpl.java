package com.sxy.es.estest0701.service.impl;

import com.sxy.es.estest0701.model.SearchResult;
import com.sxy.es.estest0701.service.QueryService;
import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    public SearchResult geoSearch() throws IOException, ParseException {
        //restricts the request to an index
        SearchRequest searchRequest = new SearchRequest("landsat02");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String wkt =  "POLYGON((-45 45, -45 -45, 45 -45, 45 45,-45 45))";
        //Utility class to create search queries.
        WKTReader wktReader = new WKTReader();
        Geometry geom = wktReader.read(wkt);
        ShapeBuilder shapeBuilder = new PolygonBuilder(new CoordinatesBuilder().coordinates(geom.getCoordinates()).close());
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("location", shapeBuilder);

        geoQuery.relation(ShapeRelation.INTERSECTS);

        searchSourceBuilder.query(geoQuery);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = helper.search(searchRequest);

        SearchResult searchResult = new SearchResult();
        searchResult.setCurpage(0);
        searchResult.setPagecount(0);
        searchResult.setCurresult(searchResponse.getHits().getHits().length);
        searchResult.setTotal(searchResponse.getHits().getTotalHits().value);
        searchResult.setTime(searchResponse.getTook().getSecondsFrac());
        searchResult.setEntitytotal(searchResponse.getHits().getHits().length);
        searchResult.setCluster("landsat");
        searchResult.setBound("");
        searchResult.setFeatures(null);
        return searchResult;
    }
    public static void main(String[] args) {
        QueryServiceImpl q = new QueryServiceImpl();
        try {
            q.geoSearch();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}
