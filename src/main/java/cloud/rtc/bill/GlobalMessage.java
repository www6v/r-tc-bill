package cloud.rtc.bill;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by www6v on 2019/5/10.
 * for test
 */
public class GlobalMessage {

    public static void main(String args[]){
        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());

//        JSONObject jsonObj = new JSONObject();
//        jsonObj.put("endpoint", "app1" );
//        jsonObj.put("bill", "urtc.audio.normal" );
//        jsonObj.put("timestamp", 1557459814);
//        jsonObj.put("step", 60);
//        jsonObj.put("value", 1);
//        jsonObj.put("counterType", "GAUGE");
//        jsonObj.put("tags", "location=beijing,service=falcon");

        Bill bill = new Bill();
        bill.setEndpoint("app1");
        bill.setMetric("urtc.audio.normal");
        bill.setTimestamp(1557459814L);
        bill.setStep(60);
        bill.setValue(1L);
        bill.setCounterType("GAUGE");
        bill.setTags("location=beijing,service=falcon");

        Bill bill1 = new Bill();
        bill1.setEndpoint("app1");
        bill1.setMetric("urtc.audio.normal");
        bill1.setTimestamp(1557459814L);
        bill1.setStep(60);
        bill1.setValue(1L);
        bill1.setCounterType("GAUGE");
        bill1.setTags("location=beijing,service=falcon");

        List<Bill> ts = new ArrayList<>();
        ts.add(bill);
        ts.add(bill1);

        System.out.print( JSON.toJSONString(ts) );
//        System.out.print(jsonObj.toJSONString());

        HttpEntity<String> formEntity = new HttpEntity<String>(JSON.toJSONString(ts), headers);

        JSONObject body = restTemplate.postForEntity("http://10.25.27.40:1988/v1/push", formEntity,
                JSONObject.class).getBody();


    }

}


