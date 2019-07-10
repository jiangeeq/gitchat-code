package com.example.canal.study;

import com.example.canal.study.action.BinLogElasticSearch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 应用的启动类
 * @author haha
 */
@SpringBootApplication
public class CanalDemoApplication implements ApplicationRunner {
    @Autowired
    private BinLogElasticSearch binLogElasticSearch;

    public static void main(String[] args) {
        SpringApplication.run(CanalDemoApplication.class, args);
    }
    // 程序启动则执行run方法
    @Override
    public void run(ApplicationArguments args) throws Exception {
        binLogElasticSearch.binLogToElasticSearch();
    }
}
