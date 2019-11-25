package org.sdominguez

// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._

// define main method (Spark entry point)
object HiveToMongoDB {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("HiveToMongoDB").setMaster("local")
    val spark: SparkSession = SparkSession.builder
      .config(conf)
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .config("spark.mongodb.input.uri", "mongodb://172.17.0.2:27017/")
      .config("spark.mongodb.input.database", "test")
      .config("spark.mongodb.input.collection", "client_balance")
      .config("spark.mongodb.output.uri", "mongodb://172.17.0.2:27017/")
      .config("spark.mongodb.output.database", "test")
      .enableHiveSupport()
      .getOrCreate()

    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    val rdd = spark.sparkContext.parallelize(Array(1 to 10))
    rdd.count()
    println("************")
    println("************")
    val df = spark.sql("select * from test.client_mov").withColumnRenamed("id","_id").groupBy("_id","name").sum("mov").alias("balance")
    df.show()
    MongoSpark.write(df).option("collection", "client_balance").mode(SaveMode.Append).save()
    println("************")
    println("************")
    val mongoRdd = MongoSpark.load(spark.sparkContext)
    println(mongoRdd.count)
    mongoRdd.foreach(println)

    // terminate spark context
    spark.stop()
  }
}

