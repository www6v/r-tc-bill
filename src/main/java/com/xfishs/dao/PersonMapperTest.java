package com.xfishs.dao;

import com.xfishs.pojo.Person;
import com.xfishs.utils.MybatisUtil;
import org.apache.ibatis.session.SqlSession;

public class PersonMapperTest {

    public static void main(String args[]) {

        Person person1 = new Person();
        person1.setName("bruce");
        person1.setAge(18);

        SqlSession sqlSession = MybatisUtil.getSqlSession();
        PersonMapper personMapper = sqlSession.getMapper(PersonMapper.class);
//        Person person = personMapper.selectPerson(1);

        int i = personMapper.insertPerson(person1);

        System.out.println("insert : " + i);

        sqlSession.commit();
        sqlSession.close();
    }
}