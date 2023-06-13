package df

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL 실습
 * DataFrame 구조의 데이터는 그리드 형태의 데이터로 SQL로 쉽게 데이터 처리 및 분석이 가능함
 * */
object ExamSQL {

  def doExam(spark: SparkSession, df: DataFrame): Unit = {

    // Spark SQL 데이터 처리가 가능하도록 Member 이름의 뷰 생성
    df.createOrReplaceTempView("Member")

    // SQL 쿼리 실행
    spark.sql("SELECT * FROM Member").show()

    // SQL 쿼리 실행, Where 추가
    spark.sql("SELECT * FROM Member WHERE Job ='대리'").show()

    // SQL 쿼리 실행, Group by 추가
    spark.sql("SELECT Job, MAX(Salary) FROM Member WHERE GROUP BY Job").show()

  }

}


