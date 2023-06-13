package df

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * 데이터 프레임을 MariaDB에 저장하기
 * */
object ExamMariaDBInsert {

  def doExam(df: DataFrame): Unit = {

    // 테이블 스키마 구조 출력하기
    println("DB 테이블 스키마 구조 확인하기")
    df.printSchema()

    df.show()

    df.write
      .format("jdbc") // DB 접속 방법
      .option("url", "jdbc:mariadb://localhost:3306/myDB") // DB 접속 URL
      .option("user", "poly") // DB 아이디
      .option("password", "1234") // DB 비밀번호
      .option("dbtable", "MEMBER") // 데이터를 가져올 테이블명(쿼리 가능)
      .mode(SaveMode.Append)
      .save()

    //    df.write
    //      .format("jdbc") // DB 접속 방법
    //      .option("url", "jdbc:mariadb://localhost:3306/myDB") // DB 접속 URL
    //      .option("user", "poly") // DB 아이디
    //      .option("password", "1234") // DB 비밀번호
    //      .option("dbtable", "MEMBER") // 데이터를 가져올 테이블명(쿼리 가능)
    //      .mode(SaveMode.Append)
    //      .save()


    //    df.write.mode(SaveMode.Append).jdbc(jdbcUrl, "MEMBER", properties)

    //    df.writeTo("")
  }

}
