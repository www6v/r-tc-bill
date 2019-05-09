package ucloud.utrc.bill.mybatis;

public interface RtcBillMapper {
    RtcBillEntity selectBill(int i);
    int insertBill(RtcBillEntity rb);
}
