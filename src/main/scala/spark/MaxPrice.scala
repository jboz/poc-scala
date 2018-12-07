package spark

import org.apache.spark.{SparkConf, SparkContext}

object MaxPrice {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Max Price").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val fileName = "src/main/resources/table.csv"
    sc.textFile(fileName)
      .map(_.split(","))
      .map(rec => (rec(0).split("-")(0).toInt, rec(1).toFloat))
      .reduceByKey((a, b) => Math.max(a, b))
      .saveAsTextFile("target/output")
  }
}
