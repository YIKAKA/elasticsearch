package com.sxy.es.estest0701.util;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;


// 用于操作ES的帮助类，绝对不可以在controller中的每个方法中单独创建，否则系统资源会被占满
public class RestHighLevelClientHelper implements Closeable {
    private RestClientBuilder builder;
    private RestHighLevelClient client;

    /**
     * 两种构造方法
     */
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
     * 检索文档
     */
    public SearchResponse search(SearchRequest request) throws IOException {
        return client.search(request, RequestOptions.DEFAULT);
    }

}
