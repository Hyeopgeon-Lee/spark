package df

import org.apache.spark.sql.SparkSession

/**
 * MariaDB에 저장된 데이터 조회하기
 * */
object ExamMariaDBSelect {

  def main(args: Array[String]): Unit = {

    // Spark 객체 생성
    val spark = SparkSession
      .builder()
      .appName("MariaDB Example")
      .master("local[*]")
      .getOrCreate()

    // DB 테이블의 내용을 데이터프레임으로 변환하기
    val df = spark
      .read
      .format("jdbc") // DB 접속 방법
      .option("url", "jdbc:mariadb://localhost:3306/myDB") // DB 접속 URL
      .option("user", "poly") // DB 아이디
      .option("password", "1234") // DB 비밀번호
      .option("dbtable", "MEMBER") // 데이터를 가져올 테이블명(쿼리 가능)
      .load() // 테이블의 데이터 가져오기

    // 테이블 스키마 구조 출력하기
    println("DB 테이블 스키마 구조 확인하기")
    df.printSchema()

    println("DB 테이블 데이터 보기")
    df.show();

    df.select("NAME").show();

  }

}
