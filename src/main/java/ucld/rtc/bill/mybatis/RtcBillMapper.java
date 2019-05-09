package ucld.rtc.bill.mybatis;

public interface RtcBillMapper {
    RtcBillEntity selectBill(int i);
    int insertBill(RtcBillEntity rb);
}
