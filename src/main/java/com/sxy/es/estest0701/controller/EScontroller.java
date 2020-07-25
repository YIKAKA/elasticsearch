package com.sxy.es.estest0701.controller;

import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;


@RestController
public class EScontroller {
    RestClientBuilder builder = RestClient.builder(
        new HttpHost("192.168.137.81",9200,"http"),
                    new HttpHost("192.168.137.82",9200,"http"),
                    new HttpHost("192.168.137.83",9200,"http")
            );
    RestHighLevelClientHelper helper = new RestHighLevelClientHelper(builder);

    @RequestMapping(value = "/deleteIndex", method = RequestMethod.POST)
    public Boolean deleteIndex(@RequestBody(required = false) String requestBody) throws IOException {
        JSONObject params = new JSONObject(requestBody);
        String indice = params.optString("indice");
        return helper.delete(indice);
    }

    @RequestMapping(value = "/createIndex", method = RequestMethod.POST)
    public Boolean createIndex(@RequestBody(required = false) String requestBody) throws IOException {
        return helper.createIfNotExist("landsat",3,2);
    }

    @RequestMapping(value = "/insertDoc", method = RequestMethod.POST)
    public Boolean insertDoc(@RequestBody(required = false) String requestBody) throws IOException {
        return helper.insertDoc();
    }

}
