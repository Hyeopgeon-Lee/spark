package file

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    // Spark 객체 생성
    val spark = SparkSession
      .builder()
      .appName("Local File Read!")
      .master("local[*]") // Spark 설치된 위치
      .getOrCreate()

    // 분석할 파일
    val txt = spark.read.textFile("src/resources/comedies.txt") // 읽은 데이터

    import spark.implicits._

    val words = txt.flatMap(value => value.split("\\s+")) // 파일을 공백 기준 나누기
    val groupedWords = words.groupByKey(_.toLowerCase) // 단어를 소문자로 변환하고, 그풉핑하기
    val result = groupedWords.count() // 그룹핑된 단어 숫자 세기

    result.show() // 워드 카운트 결과 상위 20개 출력

    result.foreach(println) // 결과 출력

    println("Total words: " + result.count()); // 결과 단어수 출력
  }

}
