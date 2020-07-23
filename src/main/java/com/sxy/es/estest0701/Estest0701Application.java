package com.sxy.es.estest0701;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class Estest0701Application {

    public static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("192.168.137.81",9200,"http"),
                    new HttpHost("192.168.137.82",9200,"http"),
                    new HttpHost("192.168.137.83",9200,"http")
            )
    );

    public static void createIndex() throws IOException {
        //create index request
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        //index settings
        request.settings(Settings.builder()
            .put("index.number_of_shards",3)
            .put("index.number_of_replicas",2)
        );
        String source = "                \"{\\n\" +\n" +
                "                        \" \\\"properties\\\": {\\n\"+\n" +
                "                        \"  \\\"message\\\":{\\n\"+\n" +
                "                        \"   \\\"type\\\":\\\"text\\\"\\n\"+\n" +
                "                        \"  }\\n\"+\n" +
                "                        \" }\\n\"+\n" +
                "                     \"}\",";
//        Map<String, Object> message = new HashMap<>();
//        message.put("type","text");
//        Map<String, Object> properties = new HashMap<>();
//        properties.put("message",message);
//        Map<String, Object> mapping = new HashMap<>();
//        mapping.put("properties",properties);
//        request.mapping(mapping);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("message");
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

        //index aliases
        request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user","kimchy")));

        //timeout to wait for the all nodes to acknowledge the index creation as a TimeValue
//        request.setTimeout(TimeValue.timeValueMinutes(2));
        //timeout to connect to the master node as a timevalue
//        request.setMasterTimeout(TimeValue.timeValueMinutes(1));

        //the number of acrive shard copies to wait for before the create index api return a response,as an int
//        request.waitForActiveShards(ActiveShardCount.from(2));
        //the number of acrive shard copies to wait for before the create index api return a response,as an acriveshardcount
        request.waitForActiveShards(ActiveShardCount.DEFAULT);

        CreateIndexResponse createIndexResponse = client.indices().create(request,RequestOptions.DEFAULT);
        //indices whether all of the nodes have acknowledged the request
        boolean acknowledged = createIndexResponse.isAcknowledged();
        //indicates whether the requisite number of shard copied were started for each shard in the index before timing out
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        System.out.println("ac"+acknowledged);
        System.out.println("sac"+shardsAcknowledged);
    }
    public static void main(String[] args) {
        try {
            createIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        GetIndexRequest request = new GetIndexRequest().indices(".kibana_1");
//        try {
//            System.out.println(client.indices().exists(request, RequestOptions.DEFAULT));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        SpringApplication.run(Estest0701Application.class, args);
    }
}
