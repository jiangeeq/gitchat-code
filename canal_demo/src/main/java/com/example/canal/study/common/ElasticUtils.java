package com.example.canal.study.common;

import com.alibaba.fastjson.JSON;
import com.example.canal.study.pojo.Student;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import java.util.Map;

/**
 * es的crud工具类
 * @author haha
 */
@Slf4j
@Component
public class ElasticUtils {
    @Autowired
    private  RestHighLevelClient restHighLevelClient;

    /**
     * 新增
     * @param student
     * @param index 索引
     */
    public  void saveEs(Student student, String index) {
        IndexRequest indexRequest = new IndexRequest(index)
                .id(student.getId())
                .source(JSON.toJSONString(student), XContentType.JSON)
                .opType(DocWriteRequest.OpType.CREATE);

        try {
            IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            log.info("保存数据至ElasticSearch成功：{}", response.getId());
        } catch (Exception e) {
            log.error("保存数据至elasticSearch失败: {}", e);
        }
    }

    /**
     * 查看
     * @param index 索引
     * @param id _id
     * @throws Exception
     */
    public  void getEs(String index, String id) {
        GetRequest getRequest = new GetRequest(index, id);
        GetResponse response = null;
        try {
            response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> fields = response.getSource();
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        } catch (Exception e) {
            log.error("从elasticSearch获取数据失败: {}", e);
        }
    }

    /**
     * 更新
     * @param student
     * @param index 索引
     * @throws Exception
     */
    public  void updateEs(Student student, String index)  {
        UpdateRequest updateRequest = new UpdateRequest(index, student.getId());
        updateRequest.doc(JSON.toJSONString(student), XContentType.JSON);
        UpdateResponse response = null;
        try {
            response = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            log.info("更新数据至ElasticSearch成功：{}", response.getId());
        } catch (Exception e) {
            log.error("更新数据至elasticSearch失败: {}", e);
        }
    }

    /**
     * 根据id删除数据
     * @param index 索引
     * @param id _id
     * @throws Exception
     */
    public  void DeleteEs(String index, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        DeleteResponse response = null;
        try {
            response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            log.info("从elasticSearch删除数据成功：{}", response.getId());
        } catch (Exception e) {
            log.error("从elasticSearch删除数据失败: {}", e);
        }
    }
}
