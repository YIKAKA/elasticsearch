package com.sxy.es.estest0701.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@RestController
public class EScontroller {

    @Autowired
    private QueryService queryService;

    RestClientBuilder builder = RestClient.builder(
        new HttpHost("192.168.137.81",9200,"http"),
                    new HttpHost("192.168.137.82",9200,"http"),
                    new HttpHost("192.168.137.83",9200,"http")
            );    RestHighLevelClientHelper helper = new RestHighLevelClientHelper(builder);

    @RequestMapping(value = "/deleteIndex", method = RequestMethod.POST)
    public Boolean deleteIndex(@RequestBody(required = false) String requestBody) throws IOException {
        JSONObject params = new JSONObject(requestBody);
        String indice = params.optString("indice");
        return helper.delete(indice);
    }

    @RequestMapping(value = "/createIndex", method = RequestMethod.POST)
    public Boolean createIndex(@RequestBody(required = false) String requestBody) throws IOException {
        return helper.createIfNotExist("landsat02",3,2);
    }

    @RequestMapping(value = "/deleteBulk", method = RequestMethod.POST)
    public int deleteBulk(@RequestBody(required = false) String requestBody) throws IOException {
        List<String> idList = new ArrayList<>();
        idList.add("0");
        idList.add("1");
        idList.add("2");
        idList.add("3");
        idList.add("4");
        idList.add("5");
        idList.add("6");
        idList.add("7");
        idList.add("8");
        return helper.delete("landsat", idList);
    }

    @RequestMapping(value = "/updateSettings", method = RequestMethod.POST)
    public boolean updateSettings(){
        return helper.updateSettings();
    }

//    @RequestMapping(value = "/search", method = {RequestMethod.GET, RequestMethod.POST})
//    public Result<SearchResult> search(HttpServletRequest req, @RequestBody(required = false) String requestBody) throws JsonProcessingException {
//        Result<SearchResult> result = new Result<>();
//        SearchResult searchResult = new SearchResult();
//        searchResult.setCurpage(1);
//        searchResult.setPagecount(1);
//        searchResult.setCurresult(0);
//        searchResult.setTotal(0);
//        searchResult.setEntitytotal(0);
//        searchResult.setTime(0D);
//        searchResult.setFeatures(Collections.emptyList());
//        try {
//            queryService.search();
////            if (a){
////                result.status("ok").result(searchResult);
////            }else {
////                result.status("no").result(searchResult);
////            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            result.status("查询失败").result(searchResult);
//        }
//        return result;
//    }

    @RequestMapping(value = "/geoSearch",method = {RequestMethod.GET, RequestMethod.POST})
    public Result<SearchResult> geoSearch(HttpServletRequest req, @RequestBody(required = false) String requestBody){
        String area = req.getParameter("area");
        String relation = req.getParameter("relation");
        String page = req.getParameter("page");
        String pagecap = req.getParameter("pagecap");
        Result<SearchResult> result = new Result<>();
        SearchResult a ;
        try {
            a = queryService.geoSearch(area,relation, Integer.parseInt(page), Integer.parseInt(pagecap));
            result.status("ok").result(a);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
            result.status("error").msg(e.getMessage());
        }
        return result;
    }

    @RequestMapping(value = "/geoSearchByPreindexed",method = {RequestMethod.GET, RequestMethod.POST})
    public Result<SearchResult> geoSearchByPreindexed(HttpServletRequest req, @RequestBody(required = false) String requestBody){
        String relation = req.getParameter("relation");
        String page = req.getParameter("page");
        String pagecap = req.getParameter("pagecap");
        Result<SearchResult> result = new Result<>();
        SearchResult a ;
        try {
            a = queryService.geoSearchByPreindexed(relation, Integer.parseInt(page), Integer.parseInt(pagecap));
            result.status("ok").result(a);
        } catch (IOException e) {
            e.printStackTrace();
            result.status("error").msg(e.getMessage());
        }
        return result;
    }

    @RequestMapping(value = "/scrollSearchByPreindexed",method = {RequestMethod.GET, RequestMethod.POST})
    public Result<SearchResult> scrollSearchByPreindexed(HttpServletRequest req, @RequestBody(required = false) String requestBody){
        String relation = req.getParameter("relation");
        Result<SearchResult> result = new Result<>();
        SearchResult a ;
        try {
            a = queryService.scrollSearchByPreindexed(relation);
            result.status("ok").result(a);
        } catch (IOException e) {
            e.printStackTrace();
            result.status("error").msg(e.getMessage());
        }
        return result;
    }


    @RequestMapping(value = "/scrollSearchByPreindexed",method = {RequestMethod.GET, RequestMethod.POST})
    public Result<SearchResult> scrollSearchByPreindexed(HttpServletRequest req, @RequestBody(required = false) String requestBody){
        String relation = req.getParameter("relation");
        Result<SearchResult> result = new Result<>();
        SearchResult a ;
        try {
            a = queryService.scrollSearchByPreindexed(relation);
            result.status("ok").result(a);
        } catch (IOException e) {
            e.printStackTrace();
            result.status("error").msg(e.getMessage());
        }
        return result;
    }
}
