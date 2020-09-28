package com.sxy.es.estest0701.util;

import com.csvreader.CsvReader;
import com.google.inject.internal.util.$FinalizableWeakReference;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 用于操作ES的帮助类，绝对不可以在controller中的每个方法中单独创建，否则系统资源会被占满
public class RestHighLevelClientHelper implements Closeable {
    private RestClientBuilder builder;
    private RestHighLevelClient client;

    //两种构造方法
    public RestHighLevelClientHelper(List<String> hosts, int port) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < httpHosts.length; i++)
            httpHosts[i] = new HttpHost(hosts.get(i), port);

        this.builder = RestClient.builder(httpHosts);
        this.client = new RestHighLevelClient(builder);
    }

    public RestHighLevelClientHelper(RestClientBuilder builder) {
        this.builder = builder;
        this.client = new RestHighLevelClient(builder);
    }

    /**
     * 刷新当前RestClient，防止长时间运行导致CPU占用过高
     */
    public synchronized void refresh() {
        try {
            RestHighLevelClient prevClient = client;
            client = new RestHighLevelClient(builder);
            Thread.sleep(1000);
            prevClient.close();
        } catch (IOException | InterruptedException e) {
           // log.error("RestElasticSearchHelper.client closed with exception", e);
        } finally {
            // log.info("RestElasticSearchHelper refreshed successfully.");
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /**
     * 读取集群的健康状态
     */
    public ClusterHealthStatus getClusterHealth() throws IOException {
        return client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT).getStatus();
    }

    /**
     * 判断索引是否存在
     *
     * @param indice 索引名
     * @return 当且仅当所有索引都存在时返回true
     */
    public boolean exists(String indice) throws IOException {
        GetIndexRequest request = new GetIndexRequest().indices(indice);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    public boolean exists(String indice, String type) throws IOException {
        GetIndexRequest request = new GetIndexRequest().indices(indice).types(type);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 创建索引
     *
     * @param indice   索引名
     * @param shards   索引分片数
     * @param replicas 索引副本数（从0起算）
     */
//    public boolean createIfNotExist(String indice, int shards, int replicas) throws IOException {
//        if (exists(indice))
//            return true;
//        XContentBuilder settings = XContentFactory.jsonBuilder().startObject()
////                .startObject("analysis")
////                .startObject("analyzer")
////                .startObject("ik_max_word").field("tokenizer", "ik_max_word").endObject()
////                .startObject("ik_smart").field("tokenizer", "ik_smart").endObject()
////                .endObject()
////                .endObject()
//                .field("number_of_shards", shards)
//                .field("number_of_replicas", replicas)
//                .endObject();
//        CreateIndexRequest request = new CreateIndexRequest(indice).settings(settings);
//        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
//        return response.isAcknowledged() && response.isShardsAcknowledged();
//    }

    /**
     * 创建索引
     *
     * @param indice   索引名
     * @param shards   索引分片数
     * @param replicas 索引副本数（从0起算）
     */
    public boolean createIfNotExist(String indice, int shards, int replicas) throws IOException {
        if (exists(indice))
            return true;
        XContentBuilder settings = XContentFactory.jsonBuilder().startObject()
//                .startObject("analysis")
//                .startObject("analyzer")
//                .startObject("ik_max_word").field("tokenizer", "ik_max_word").endObject()
//                .startObject("ik_smart").field("tokenizer", "ik_smart").endObject()
//                .endObject()
//                .endObject()
                .field("number_of_shards", shards)
                .field("number_of_replicas", replicas)
                .field("max_result_window",10000)
                .endObject();
        CreateIndexRequest request = new CreateIndexRequest(indice).settings(settings);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("location");
                {
                    builder.field("type","geo_shape");
                }
                builder.endObject();
                builder.startObject("c");
                {
                    builder.field("type","text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        //index mappings
        request.mapping("_doc", builder);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        return response.isAcknowledged() && response.isShardsAcknowledged();
    }

    //删除索引
    public boolean delete(String... indices) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indices);
        return client.indices().delete(request, RequestOptions.DEFAULT).isAcknowledged();
    }

    public boolean delete(String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index).type("_doc").id(id);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        return response.getResult() == DocWriteResponse.Result.NOT_FOUND;
    }

    public int delete(String index, List<String> idList) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (String id : idList) {
            bulkRequest.add(new DeleteRequest(index).type("_doc").id(id));
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        int error = 0;
        for (BulkItemResponse response : bulkResponse) {
            if (response.isFailed())
                error++;
        }
        return error;
    }

    public long deleteDtype(String indice, String dtype) throws IOException {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.indices(indice);
        request.types("_doc");
        request.setQuery(QueryBuilders.termQuery("dtype", dtype));
        return client.deleteByQuery(request, RequestOptions.DEFAULT).getDeleted();
    }

    //添加数据
    public void insertDoc() throws IOException {
        List<Map<String,Double>> list = readFromCSV(',',"E:\\xm\\es20200720\\LANDSAT_TM_C1.csv\\LANDSAT_TM_C1_01.csv");
        for (int i = 0; i<list.size();i++) {
            double lowerLeftCornerLongitude = list.get(i).get("lowerLeftCornerLongitude");
            double lowerLeftCornerLatitude =  list.get(i).get("lowerLeftCornerLatitude");
            double lowerRightCornerLongitude =  list.get(i).get("lowerRightCornerLongitude");
            double lowerRightCornerLatitude =  list.get(i).get("lowerRightCornerLatitude");
            double upperRightCornerLongitude =  list.get(i).get("upperRightCornerLongitude");
            double upperRightCornerLatitude =  list.get(i).get("upperRightCornerLatitude");
            double upperLeftCornerLongitude =  list.get(i).get("upperLeftCornerLongitude");
            double upperLeftCornerLatitude =  list.get(i).get("upperLeftCornerLatitude");
            String wkt =  "POLYGON (("+lowerLeftCornerLongitude+" "+lowerLeftCornerLatitude+","+lowerRightCornerLongitude+" "+lowerRightCornerLatitude+","+upperRightCornerLongitude+" "+upperRightCornerLatitude+","+upperLeftCornerLongitude+" "+upperLeftCornerLatitude+","+lowerLeftCornerLongitude+" "+lowerLeftCornerLatitude+"))";

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("location",  wkt);
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest("landsat")
                    .id(String.valueOf(i)).source(builder);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        }
    }


    public static<T> List<T> readFromCSV(Character separator, String filePath) {
        CsvReader reader = null;
        List<T> result = new ArrayList<>();
        try {
            //如果生产文件乱码，windows下用gbk，linux用UTF-8
            reader = new CsvReader(filePath, separator, Charset.forName("GBK"));

            // 读取标题
            reader.readHeaders();
            // 逐条读取记录，直至读完
            while (reader.readRecord()) {
                Map<String, Double> jsonMap = new HashMap<String,Double>();
                jsonMap.put("lowerLeftCornerLongitude", Double.valueOf(reader.get("lowerLeftCornerLongitude")));
                jsonMap.put("lowerLeftCornerLatitude", Double.valueOf(reader.get("lowerLeftCornerLatitude")));
                jsonMap.put("lowerRightCornerLongitude", Double.valueOf(reader.get("lowerRightCornerLongitude")));
                jsonMap.put("lowerRightCornerLatitude", Double.valueOf(reader.get("lowerRightCornerLatitude")));
                jsonMap.put("upperRightCornerLongitude", Double.valueOf(reader.get("upperRightCornerLongitude")));
                jsonMap.put("upperRightCornerLatitude", Double.valueOf(reader.get("upperRightCornerLatitude")));
                jsonMap.put("upperLeftCornerLongitude", Double.valueOf(reader.get("upperLeftCornerLongitude")));
                jsonMap.put("upperLeftCornerLatitude", Double.valueOf(reader.get("upperLeftCornerLatitude")));
                result.add((T) jsonMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != reader) {
                reader.close();
            }
        }
        return result;
    }

    public SearchResponse search(SearchRequest request) throws IOException {
        return client.search(request, RequestOptions.DEFAULT);
    }

    public SearchResponse scroll(SearchScrollRequest request) throws IOException {
        return client.scroll(request, RequestOptions.DEFAULT);
    }
    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest) throws IOException {
        return client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }
    //Update Indices Settings API
    public boolean updateSettings(){
        boolean acknowledged;
        //可以针对单个，多个，全部index设置
        UpdateSettingsRequest request = new UpdateSettingsRequest("landsat02");
        String settingKey = "index.max_result_window";
        int settingValue = 100000000;
        Settings settings =
                Settings.builder()
                .put(settingKey, settingValue)
                .build();
        request.settings(settings);
        try {

            AcknowledgedResponse updateSettingsResponse = client.indices().putSettings(request, RequestOptions.DEFAULT);
            acknowledged = updateSettingsResponse.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
            acknowledged = false;
        }
        return acknowledged;
    }

    public static void main(String[] args){
        RestClientBuilder builder = RestClient.builder(
          new HttpHost("192.168.10.135",9200,"http")
//            new HttpHost("192.168.137.82",9200,"http"),
//            new HttpHost("192.168.137.83",9200,"http")
        );
         RestHighLevelClientHelper helper = new RestHighLevelClientHelper(builder);

        try {
            helper.insertDoc();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
