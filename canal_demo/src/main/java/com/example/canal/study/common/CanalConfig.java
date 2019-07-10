package com.example.canal.study.common;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

/**
 * 配置一些跟canal相关到配置与公共bean
 * @author haha
 */
@Configuration
public class CanalConfig {
    // @Value 获取 application.properties配置中端内容
    @Value("${canal.server.ip}")
    private String canalIp;
    @Value("${canal.server.port}")
    private Integer canalPort;
    @Value("${canal.destination}")
    private String destination;
    @Value("${elasticSearch.server.ip}")
    private String elasticSearchIp;
    @Value("${elasticSearch.server.port}")
    private Integer elasticSearchPort;
    @Value("${zookeeper.server.ip}")
    private String zkServerIp;

    /**
     * 获取简单canal-server连接
      */
    @Bean
    public CanalConnector canalSimpleConnector() {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalIp, canalPort), destination, "", "");
        return canalConnector;
    }
    /**
     * 通过连接zookeeper获取canal-server连接
      */
    @Bean
    public CanalConnector canalHaConnector() {
        CanalConnector canalConnector = CanalConnectors.newClusterConnector(zkServerIp, destination, "", "");
        return canalConnector;
    }

    /**
     * elasticsearch 7.x客户端
      */
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(elasticSearchIp, elasticSearchPort))
        );
        return client;
    }
}