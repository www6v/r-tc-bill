package ucloud.utrc.bill.mybatis;

import com.xfishs.dao.PersonMapper;
import com.xfishs.pojo.Person;
import com.xfishs.utils.MybatisUtil;
import org.apache.ibatis.session.SqlSession;

public class RtcBillDataAccess {

    public static void main(String args[]) {

        String roomId = "";
        Integer count = 0;

        insertDB(roomId,count);
    }

    public static void insertDB(String roomId, Integer count) {
        RtcBillEntity rtcBillEntity = new RtcBillEntity();
        rtcBillEntity.setRoomId(roomId);
        rtcBillEntity.setCount(count);

        SqlSession sqlSession = MybatisUtil.getSqlSession();
        RtcBillMapper rtcBillMapper = sqlSession.getMapper(RtcBillMapper.class);
//        Person person = personMapper.selectPerson(1);

        int i = rtcBillMapper.insertBill(rtcBillEntity);

        System.out.println("insert : " + i);

        sqlSession.commit();
        sqlSession.close();
    }
}