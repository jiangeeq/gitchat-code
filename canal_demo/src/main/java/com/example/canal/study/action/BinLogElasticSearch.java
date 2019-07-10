package com.example.canal.study.action;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canal.study.common.CanalDataParser;
import com.example.canal.study.common.ElasticUtils;
import com.example.canal.study.pojo.Student;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 获取binlog数据并发送到es中
 *
 * @author haha
 */
@Slf4j
@Component
public class BinLogElasticSearch {
    @Autowired
    private CanalConnector canalSimpleConnector;
    @Autowired
    private ElasticUtils elasticUtils;
    //@Qualifier("canalHaConnector")使用名为canalHaConnector的bean
    @Autowired
    @Qualifier("canalHaConnector")
    private CanalConnector canalHaConnector;

    public void binLogToElasticSearch() throws IOException {
        openCanalConnector(canalSimpleConnector);
        // 轮询拉取数据
        Integer batchSize = 5 * 1024;
        while (true) {
//            Message message = canalHaConnector.getWithoutAck(batchSize);
            Message message = canalSimpleConnector.getWithoutAck(batchSize);
            long id = message.getId();
            int size = message.getEntries().size();
            log.info("当前监控到binLog消息数量{}", size);
            if (id == -1 || size == 0) {
                try {
                    // 等待4秒
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //1. 解析message对象
                List<CanalEntry.Entry> entries = message.getEntries();
                List<CanalDataParser.TwoTuple<CanalEntry.EventType, Map>> rows = CanalDataParser.printEntry(entries);

                for (CanalDataParser.TwoTuple<CanalEntry.EventType, Map> tuple : rows) {
                    if (tuple.eventType == CanalEntry.EventType.INSERT) {
                        Student student = createStudent(tuple);
                        // 2。将解析出的对象同步到elasticSearch中
                        elasticUtils.saveEs(student, "student_index");
                        // 3.消息确认已处理
                        canalSimpleConnector.ack(id);
//                        canalHaConnector.ack(id);
                    }
                    if (tuple.eventType == CanalEntry.EventType.UPDATE) {
                        Student student = createStudent(tuple);
                        elasticUtils.updateEs(student, "student_index");
                        // 3.消息确认已处理
                        canalSimpleConnector.ack(id);
//                        canalHaConnector.ack(id);
                    }
                    if (tuple.eventType == CanalEntry.EventType.DELETE) {
                        elasticUtils.DeleteEs("student_index", tuple.columnMap.get("id").toString());
                        canalSimpleConnector.ack(id);
//                        canalHaConnector.ack(id);
                    }
                }
            }
        }
    }

    /**
     * 封装数据至Student对象中
     *
     * @param tuple
     * @return
     */
    private Student createStudent(CanalDataParser.TwoTuple<CanalEntry.EventType, Map> tuple) {
        Student student = new Student();
        student.setId(tuple.columnMap.get("id").toString());
        student.setAge(Integer.parseInt(tuple.columnMap.get("age").toString()));
        student.setName(tuple.columnMap.get("name").toString());
        student.setSex(tuple.columnMap.get("sex").toString());
        student.setCity(tuple.columnMap.get("city").toString());
        return student;
    }

    /**
     * 打开canal连接
     *
     * @param canalConnector
     */
    private void openCanalConnector(CanalConnector canalConnector) {
        //连接CanalServer
        canalConnector.connect();
        // 订阅destination
        canalConnector.subscribe();
    }

    /**
     * 关闭canal连接
     *
     * @param canalConnector
     */
    private void closeCanalConnector(CanalConnector canalConnector) {
        //关闭连接CanalServer
        canalConnector.disconnect();
        // 注销订阅destination
        canalConnector.unsubscribe();
    }
}