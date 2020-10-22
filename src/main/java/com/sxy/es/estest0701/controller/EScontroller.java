package com.sxy.es.estest0701.controller;

import com.sxy.es.estest0701.model.Result;
import com.sxy.es.estest0701.model.SearchResult;
import com.sxy.es.estest0701.service.QueryService;
import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.json.JSONObject;
import org.locationtech.jts.io.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;


@RestController
public class EScontroller {

    @Autowired
    private QueryService queryService;
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/search")
    public Result<SearchResult> listPublicTanSatLayersByGeometryByES(HttpServletRequest request,
     @RequestParam(value = "place", required = false) String place,
     @RequestParam(value = "address", required = false) String address,
     @RequestParam(value = "rank", required = false, defaultValue = "1") int rank,
     @RequestParam(value = "geometry", required = false) String geometry,
     @RequestParam(value = "relation", required = false, defaultValue = "INTERSECTS") String relation,
     @RequestParam(value = "satellites", required = false) List<String> satellites,
     @RequestParam(value = "sensors", required = false) List<String> sensors,
     @RequestParam(value = "levels", required = false) List<String> levels,
     @RequestParam(value = "minResolution", required = false, defaultValue = "0") double minResolution,
     @RequestParam(value = "maxResolution", required = false, defaultValue = "0") double maxResolution,
     @RequestParam(value = "startTime", required = false, defaultValue = "0") long startTime,
     @RequestParam(value = "endTime", required = false, defaultValue = "0") long endTime,
     @RequestParam(value = "length", required = false, defaultValue = "10") int length,
     @RequestParam(value = "objects", required = false) String objects,
     @RequestParam(value = "shapefilePath", required = false) String shapefilePath) throws IOException, ParseException {
        Result<SearchResult> result = null;
        SearchResult searchResult = null;
        searchResult = queryService.search(place, address, rank, geometry, relation, satellites, sensors, levels, minResolution, maxResolution,
          startTime, endTime, length, objects,shapefilePath);
        if(searchResult.getTotalCount() != 0){
            result = new Result<SearchResult>().status("ok").result(searchResult);
        }else{
            result = new Result<SearchResult>().status("OK").msg("无结果").result(searchResult);
        }
        return result;
    }

}
