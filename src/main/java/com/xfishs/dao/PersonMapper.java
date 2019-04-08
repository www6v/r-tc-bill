package com.xfishs.dao;

import com.xfishs.pojo.Person;

/**
 * Created by www6v on 2019/4/8.
 */
public interface PersonMapper {
    Person selectPerson(int i);
    int insertPerson(Person p);
}
