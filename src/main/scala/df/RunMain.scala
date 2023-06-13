package df

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RunMain {

  def main(args: Array[String]): Unit = {

    // Spark 객체 생성
    val spark = SparkSession
      .builder()
      .appName("Data Frame Example")
      .master("local[*]") // Spark 설치된 위치
      .getOrCreate()

    // 저장할 데이터 구조
    val dataSchema = StructType(Array(
      StructField("NAME", StringType, true),
      StructField("AGE", IntegerType, true),
      StructField("BIRTHDAY", StringType, true),
      StructField("JOB", StringType, true),
      StructField("GENDER", StringType, true),
      StructField("SALARY", DoubleType, true)
    ))

    //    저장할 데이터
    val data = Seq(
      Row("박이사", 40, "1984-01-01", "이사", "M", 8800.60),
      Row("김부장", 36, "1988-01-10", "부장", "M", 7300.80),
      Row("박과장", 36, "1988-03-10", "과장", "M", 6300.80),
      Row("전대리", 29, "1995-03-30", "대리", "F", 3800.50),
      Row("나대리", 29, "1995-05-31", "대리", "F", 3500.50),
      Row("최대리", 29, "1995-11-31", "대리", "M", 4100.50),
      Row("김사원", 30, "1994-03-31", "사원", "M", 3300.10),
      Row("이주임", 30, "1994-08-31", "주임", "F", 370.10)

    )

    // 데이터 프레임에 데이터 구조 정의 및 데이터 저장하기
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), dataSchema)

    df.printSchema() // 데이터 프레임 구조 보기

    df.show() // 데이터 프레임에 저장된 데이터 보기

    //    ExamBasic.doExam(spark, df) // 데이터 프레임 기초 실습 진행

    //    ExamSQL.doExam(spark, df) // SparkSQL 실습

    //    ExamMariaDBInsert.doExam(df) // MariaDB Insert 예제

    // Spark 종료
    spark.stop()
  }
}

