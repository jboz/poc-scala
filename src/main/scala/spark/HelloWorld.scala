package spark

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    // initialise spark context
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // do stuff
    println("************")
    println("************")
    println("Hello, world! spark.version=" + sc.version)
    println("************")
    println("************")

    // terminate spark context
    sc.stop()
  }
}
