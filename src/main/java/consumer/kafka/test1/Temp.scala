//package consumer.kafka.test1
//
//import com.alibaba.fastjson.{JSONException, JSON}
//
///**
//  * Created by www6v on 2019/4/4.
//  */
//object Temp {
//  def main(args:Array[String]) : Unit = {
//    try {
////      val value: String = "{\"appId\":\"URtc-h\",\"audio\":0,\"bitrate\":29131,\"byteCount\":130267,\"delay\":-1,\"fractionLost\":0,\"interval\":3917,\"jitter\":1,\"level\":\"info\",\"msg\":\"close\",\"mstag\":\"STAT\",\"profile\":\"none\",\"roomId\":\"123\",\"rtt\":-1,\"server\":\"log\",\"session\":\"\",\"streamId\":\"a1b\",\"ts\":\"2019-04-03T15:41:41.919+0800\",\"type\":\"PUSH\",\"userId\":\"android_99efbf74-99cd-4dde-9a90-d51f89066c86\",\"video\":1}>\n"
//      val value: String = "";
//      val jsonObject = JSON.parseObject(value)
//      if(jsonObject==null) {
//        System.out.println("jsonObject null")
//        false
//      }
//      val mstag = jsonObject.getString("mstag")
//      if(mstag==null) {
//        System.out.println("mstag null")
//        false
//      }
//
//      mstag.equals("STAT")
//    } catch {
//      case ex: JSONException =>{
//        System.out.println("JSONException")
//        false
//      }
//    }
//   }
//}
