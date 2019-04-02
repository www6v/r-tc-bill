package consumer.kafka.client

import java.util.Properties

import consumer.kafka.MessageAndMetadata
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

class DataAccess {

//  def insertdb(rdd : RDD[MessageAndMetadata[Array[Byte]]], key:String, value:String, sqlContext:SQLContext ): Unit = {
  def insertdb(rows : java.util.List[Row], key:String, value:Integer, sqlContext:SQLContext ): Unit = {
    val connectionProperties = new Properties();
    connectionProperties.put("user","root");
    connectionProperties.put("password","1qaZxsw23edcvfr4");
    connectionProperties.put("driver","com.mysql.jdbc.Driver");

    val url = "jdbc:mysql://10.25.29.26:3306/test";

    rows.add(Row( key , value ));

    val schame= StructType(
      StructField("name",StringType,true)::
        StructField("age",IntegerType,true)::Nil
    )

    val df: DataFrame = sqlContext.createDataFrame(rows, schame);

    df.write.mode("append").jdbc( url,"person",connectionProperties );
  }
}
