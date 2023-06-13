package file

import org.apache.spark.sql.SparkSession

object LineCount {

  def main(args: Array[String]): Unit = {

    // Spark 객체 생성
    val spark = SparkSession
      .builder()
      .appName("Local File Read!")
      .master("local[*]") // Spark 설치된 위치
      .getOrCreate()

    val txt = spark.read.textFile("src/resources/comedies.txt") // 읽은 데이터

    println("전체 라인 수 : " + txt.count)

    spark.stop()
  }

}

