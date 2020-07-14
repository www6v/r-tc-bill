package cloud.rtc.bill.mybatis;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;

public class MybatisUtil {

    private static SqlSessionFactory sqlSessionFactory = null;

    static {
        try {
//            Reader reader = Resources.getResourceAsReader("mybatis-config.xml");
            Reader reader = Resources.getResourceAsReader("mybatis-config-bill.xml");
            sqlSessionFactory= new SqlSessionFactoryBuilder().build(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private MybatisUtil(){}

    public static SqlSession getSqlSession(){
        return sqlSessionFactory.openSession();
    }

}
