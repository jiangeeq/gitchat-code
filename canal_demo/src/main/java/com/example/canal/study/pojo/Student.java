package com.example.canal.study.pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * 普通的实体domain对象
 * @Data 用户生产getter、setter方法
  */
@Data
public class Student implements Serializable {
    private String id;
    private String name;
    private int age;
    private String sex;
    private String city;
}
