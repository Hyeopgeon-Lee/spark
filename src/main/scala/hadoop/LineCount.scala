package hadoop

import org.apache.spark.sql.SparkSession

object LineCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HDFS File Read!")
      .master("local[*]") // Spark 설치된 위치
      .getOrCreate()

    val txt = spark.read.text("hdfs://192.168.2.136:9000/comedies")

    println("하둡에 저장된 comedies 파일의 전체 라인 수 : " + txt.count)

    spark.stop() // 스파크 종료

  }

}

