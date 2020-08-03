package com.sxy.es.estest0701.service;

import com.sxy.es.estest0701.model.SearchResult;
import com.sxy.es.estest0701.type.Tuple3;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface QueryService {

    GeometryFactory geometryFactory = new GeometryFactory();
//    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();
//    GeometryJSON geometryJSON = new GeometryJSON();

    void search() throws IOException;
//    boolean search() throws IOException;

    Tuple3<Integer, Envelope, List<Map<String, Object>>> toJson(SearchResponse response, Geometry filter
            , ShapeRelation relation, List<String> includes, List<String> excludes, boolean hasDistance);

    SearchResult response2Result(Integer page, Integer pagecap, SearchResponse response, Geometry geomFilter
            , List<String> excludes, String orderBy, Object defaultValue, boolean numberOrder, boolean asc);

    default boolean satisfy(Geometry filter, Geometry geometry, ShapeRelation relation) {
        if (relation == null)
            relation = ShapeRelation.INTERSECTS;
        switch (relation) {
            case WITHIN:
                return filter.within(geometry);
            case CONTAINS:
                return filter.contains(geometry);
            case DISJOINT:
                return filter.disjoint(geometry);
            default:
                return filter.intersects(geometry);
        }
    }
}
