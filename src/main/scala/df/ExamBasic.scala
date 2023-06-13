package df

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 데이터 프레임에 데이터 구조 및 데이터 저장하기 예제
 */
object ExamBasic {

  def doExam(spark: SparkSession, df: DataFrame): Unit = {

    df.printSchema() // 데이터 구조 보여주기
    df.show(false) // 전체 데이터 보여주기

    // 저장된 데이터 수 확인하기
    println("저장된 데이터 수 : " + df.count())

    println("SQL의 WHERE 절과 유사한 Filter 실습!")
    // 조건 1개 데이터 조회하기
    df.filter(df("Name") === "박이사").show()

    // 조건 2개 데이터 조회하기
    // AND 연산 : &&, OR 연산 ||, Not 연산 =!=
    df.filter(df("Job") === "대리" && df("Gender") === "F").show()

    // 조건 2개 데이터 조회하기
    // AND 연산 : &&, OR 연산 ||, Not 연산 =!=
    df.filter(df("Job") === "대리" || df("Gender") === "F").show()

    println("SQL의 SELETE 절과 유사한 select 실습!")

    // 일부분 조회(Name 필드 조회하기)
    df.select("Name").show()

    df.select("Name", "Age", "Gender").show()

    // 데이터 프레임 컬럼값 편하게 사용하기 => $ 명령어 사용
    import spark.implicits._

    // 컬럼 값 계산하여 출력하기(나이 한살씩 더하기)
    df.select($"Name", $"Age", $"Age" + 1, $"Gender").show()

    // 컬럼에 조건 추가하기(30살 이상 조회하기)
    // select 문의 조건 <> 은 조건에 대한 결과(True, False) 출력
    df.select($"Name", $"Age" > 30).show()

    // 조건에 대한 출력은 Filter
    df.filter($"Age" > 30).show()

    // Group By 구문
    // SQL과 동일하게 MAX, MIN, COUNT, AVG 사용
    df.groupBy(df("Job")).max("Salary").show()

    // Group By 구문
    df.groupBy(df("Job")).min("Salary").show()

  }
}
