package com.xfishs.pojo;

import lombok.*;

@Data  //生成get set方法
@ToString //生成toString方法
@Setter
@Getter
@NoArgsConstructor
public class Person {

    private int id;
    private String name ;
    private int age ;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
