package ucloud.utrc.bill.mybatis;

import org.apache.ibatis.session.SqlSession;

public class RtcBillDataAccess {
    public static void insertDB(String roomId, Integer count, String appId) {
        RtcBillEntity rtcBillEntity = new RtcBillEntity();
        rtcBillEntity.setRoomId(roomId);
        rtcBillEntity.setCount(count);
        rtcBillEntity.setAppId(appId);

        SqlSession sqlSession = MybatisUtil.getSqlSession();
        RtcBillMapper rtcBillMapper = sqlSession.getMapper(RtcBillMapper.class);

        int i = rtcBillMapper.insertBill(rtcBillEntity);

        System.out.println("insert : " + i);

        sqlSession.commit();
        sqlSession.close();
    }
}