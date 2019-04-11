package ucloud.utrc.bill.mybatis;

import org.apache.ibatis.session.SqlSession;

public class RtcBillDataAccess {

    public static void insertDB(String appId, String roomId, Integer count,String profile, Long endTime, Long startTime) {
        RtcBillEntity rtcBillEntity = new RtcBillEntity();
        rtcBillEntity.setAppId(appId);
        rtcBillEntity.setRoomId(roomId);
        rtcBillEntity.setProfile(profile);
        rtcBillEntity.setCount(count);
        rtcBillEntity.setEndTime(endTime);
        rtcBillEntity.setStartTime(startTime);

        SqlSession sqlSession = MybatisUtil.getSqlSession();
        RtcBillMapper rtcBillMapper = sqlSession.getMapper(RtcBillMapper.class);

        int i = rtcBillMapper.insertBill(rtcBillEntity);

        System.out.println("insert : " + i);

        sqlSession.commit();
        sqlSession.close();
    }
}