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
    SearchResult search(String place, String address, String geometry, String relation, List<String> satellites,
                        List<String> sensors, List<String> levels, double minResolution, double maxResolution,
                        long startTime, long endTime, int start, int length, String shapefilePath) throws IOException, ParseException;
}
