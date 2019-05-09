package ucloud.utrc.bill;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Deprecated
public class DbStore {

    public static void main(String[] args) {
        insertDB("123", 579);
    }

    public static void insertDB(String roomId,Integer count) {
        Connection con;
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.25.29.26:3306/test";
        String user = "root";
        String password = "1qaZxsw23edcvfr4";

        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url,user,password);
            Statement statement = con.createStatement();
            String sql = "INSERT INTO rtc_bill (room_id, count) VALUES ('"+ roomId +"', "+ count +")";
            int i = statement.executeUpdate(sql);

            System.out.println("updated " + i + " record.");

            con.close();
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
        }
    }
}