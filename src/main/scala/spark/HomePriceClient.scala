package spark

import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.{SparkConf, SparkContext}

object HomePriceClient {
  def main(args: Array[String]): Unit = {
    val master = "local[*]"
    //    val master = "spark://192.168.1.2:7077"
    val fileName = "src/main/resources/homeprice.data"
    //    val fileName = "hdfs://192.168.1.2:8120/user/root/homeprice.data"
    val path = "target/"
    //    val path = "hdfs://192.168.1.2:8120/user/root/"

    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName("Home Price CLI"))

    val linRegModel = sc.objectFile[LinearRegressionModel](path + "linReg.model").first()
    val scalerModel = sc.objectFile[StandardScalerModel](path + "scaler.model").first()

    // home.age, home.bathrooms, home.bedrooms, home.garage, home.sqF
    println(linRegModel.predict(scalerModel.transform(Vectors.dense(11.0, 2.0, 2.0, 1.0, 2200.0))))

    sc.stop()
  }
}
