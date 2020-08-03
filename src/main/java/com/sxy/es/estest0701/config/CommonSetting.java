package com.sxy.es.estest0701.config;

import com.sxy.es.estest0701.util.RestHighLevelClientHelper;
import lombok.Data;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
@Component
@ConfigurationProperties(prefix = "settings")
@Data
public class CommonSetting {
    /**
     * elasticsearch查询结果的最小得分
     */
    private Float esMinScore;
    private int cpuCoreSize;
    private int maxTaskPoolSize;
    private int taskQueueCapacity;
    private int keepAliveSeconds;
    private float esFetchFactor;
    private double geomDistTolerance;

    @Bean
    public RestHighLevelClientHelper getRestEsHelper(RestClientBuilder builder) {
        return new RestHighLevelClientHelper(builder);
    }
}
