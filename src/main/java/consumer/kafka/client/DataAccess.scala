package consumer.kafka.client

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

class DataAccess {

//  def insert(): Unit = {
//
//    val sc = new SparkContext();
//    val sql = new SQLContext(sc)
//    val people = sc.textFile("people.txt").map(lines => lines.split(","))
//    val peopleRow = sc.map(p => Row(p(0),p(1),p(2)))
//    val schema = StructType(StructFile("id",IntegerType,true)::
//      StructFile("name",StringType,true)::
//      StructFile("age",IntegerType,true)::Nil)
//    val peopleInfo = sql.createDataFrame(peopleRow,schema)
//    peopleInfo.registerTempTable("tempTable")

//    sql.sql.sql("""
//                  |insert overwrite table tagetTable
//                  |select
//                  | id,
//                  | name,
//                  | age
//                  |from tempTable
//                """.stripMargin)
//
//  }

//  def insert1( key:String, value:String ): Unit = {
//val l = List((key,value))\
//val rdd = sc.parallelize(l,2)
//val rowRDD: RDD[Row] = rdd.map(p=>Row(  ))

//  def insert1( rdd:RDD[MessageAndMetadata[Array[Byte]]],  conf:SparkConf ): Unit = {
//def insert1( rdd:RDD[MessageAndMetadata[Array[Byte]]],  sc:SparkContext ): Unit = {
//  def insert1(key:String,  value:String, sc:SparkContext ): Unit = {
//  def insert1(rows : java.util.List[Row] , key:String,  value:String,sc:SparkContext ): Unit = {
  def insert1(rows : java.util.List[Row] , key:String,  value:String, sqlContext:SQLContext ): Unit = {
    val connectionProperties = new Properties();
    connectionProperties.put("user","root");
    connectionProperties.put("password","1qaZxsw23edcvfr4");
    connectionProperties.put("driver","com.mysql.jdbc.Driver");

    val url = "jdbc:mysql://10.25.29.26:3306/test";

//    val conf=new SparkConf()
//    conf.setAppName(s"${this.getClass.getSimpleName}").setMaster("local")
//    val sc=new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)

//    val rdd=sc.textFile("C:\\Users\\wangyongxiang\\Desktop\\plan\\person.txt")
//    val rowRDD: RDD[Row] = rdd.map(_.split(",")).map(p=>Row(p(0),p(1).trim.toInt))
//    val pair = rdd.collect();
//    val rowRDD: RDD[Row] = {
//       rdd.map(p => Row( pair(0) , 11 ))
//    }

//  val rowRDD: RDD[Row] = {
//     Row( 12 , 11 )
//  }


//  val l : List[Row] = List(  Row( 12 , 11 )) ;
//  val l : util.List[org.apache.spark.sql.Row] = util.List[Row];
//  new util.List();
//  l.add(Row( key , 11 ));

    rows.add(Row( key , 11 ));

    val schame= StructType(
      StructField("name",StringType,true)::
        StructField("age",IntegerType,true)::Nil
    )

  val df: DataFrame = sqlContext.createDataFrame(rows, schame);
//    val df: DataFrame = sqlContext.createDataFrame(rowRDD,schame)

//    personsDF.write().mode("append").jdbc(url,"person",connectionProperties);

    df.write.mode("append").jdbc( url,"person",connectionProperties );
  }
}
