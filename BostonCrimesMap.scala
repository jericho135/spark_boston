package com.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row


object BostonCrimesMap extends App {

  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  spark.sparkContext.setLogLevel("WARN")

 /* С помощью Spark соберите агрегат по районам (поле district) со следующими метриками:
    crimes_total - общее количество преступлений в этом районе
      crimes_monthly - медиана числа преступлений в месяц в этом районе
      frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую с одним пробелом “, ” ,
          расположенных в порядке убывания частоты
          crime_type - первая часть NAME из таблицы offense_codes, разбитого по разделителю “-” (например, если NAME “BURGLARY - COMMERICAL - ATTEMPT”, то crime_type “BURGLARY”)
  lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
  lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов*/

  //val jsonFile = sc.textFile(s"${args(0)}")
  //val jsonFile = sc.textFile(s"${args(1)}")


  val offenceCodesCSV = 'file///C:/temp/offense_codes.csv'

  val dsCrimes = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load(s"${args(0)}")

  //dsCrimes.printSchema()
  dsCrimes.createOrReplaceTempView("dfTable")

  // высчитаем константы lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
  //  lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
  // судя по всему, требуется вычислить константу исходя из всех пруступлений, а не по каждому району

  val latVal = dsCrimes.select(avg("lat")).first()(0)
  val lngVal = dsCrimes.select(avg("long")).first()(0)

  //высчитываем crimes_total - общее количество преступлений в этом районе
  //   и   crimes_monthly - медиана числа преступлений в месяц в этом районе
  //для медианы сначала выведем группировку по районам и месяцам, а потом через окно возьмем медиану

  val dsCrimesCount = dsCrimes.groupBy("DISTRICT")
                              .agg(count("*").alias("crimes_total"))
                              .na.fill("null")

  val windowSpec = Window.partitionBy("DISTRICT")
  val mean_percentile = expr("percentile_approx(count, 0.5)")

  val dsCrimesMonth =  dsCrimes.groupBy("DISTRICT","MONTH").count()
  val dsCrimesMonthMedian = dsCrimesMonth.groupBy("DISTRICT").agg(mean_percentile.alias("crimes_monthly")).na.fill("null")

  var joinType = "inner"

  //dsCrimesCount.show()
  //dsCrimesMonthMedian.show(40)

  // датафрейм содержит колонки DISTRICT, crimes_total и crimes_monthly
  val dsCrimesTotMonth = dsCrimesCount.join(dsCrimesMonthMedian,"DISTRICT")
  // dsCrimesJoin1.show(40)


  //высчитываем  frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую с одним пробелом “, ” , расположенных в порядке убывания частоты
  //  crime_type - первая часть NAME из таблицы offense_codes, разбитого по разделителю “-” (например, если NAME “BURGLARY - COMMERICAL - ATTEMPT”, то crime_type “BURGLARY”)
  // соберем окно с ROW_NUMBER <= 3 с сортировкой по числу преступлений в обратном порядке => получим топ 3
  val preJoinCrimes = spark.sql("""select *
                                  |from (
		|select DISTRICT,
		|OFFENSE_CODE as code,
		|cnt,
		|ROW_NUMBER() over (partition by DISTRICT order by cnt desc) as rn
		|from  (
			|select DISTRICT, OFFENSE_CODE, COUNT(*) as cnt  from dfTable  group by DISTRICT, OFFENSE_CODE
			  |)
		|)
		|where rn <= 3 order by district """.stripMargin)

  //читаем второй файл с кодами преступлений
  val dsCodes = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load(s"${args(1)}")

  dsCodes.createOrReplaceTempView("dfCodes")

  //т.к. есть дубли по кодам, то группируем и берем максимальное значение name (в задании не указано, что делать при дублях)

  val preJoinCodes =  spark.sql("""select code, split(name,'-')[0] as name from (select code, max(name) as name from dfCodes group by code)""")

  val joinCodesCrimes = preJoinCrimes.join(broadcast(preJoinCodes),"CODE")

  joinCodesCrimes.createOrReplaceTempView("codesCrimesTable")

  //вывод всех колонок для проверки
  /*spark.sql("""select COUNT(*),
       |DISTRICT,
       |concat_ws(', ', collect_list(CODE)),
           |concat_ws(', ', collect_list(name)),
       |concat_ws(', ', collect_list(cnt))
          | from codesCrimesTable
       |group by DISTRICT
       |order by district""".stripMargin).show(false)*/

//получим три самых частых crime_type за всю историю наблюдений по районам
// кологнки DISTRICT и crime_type
  val dsCrimesFreq = spark.sql("""select
		 |DISTRICT,
      |concat_ws(', ', collect_list(name)) as crime_type
      | from codesCrimesTable
		 |group by DISTRICT
		 |order by district""".stripMargin).na.fill("null")

  //joinCodesCrimes.show(40, false)

  //джойним все вместе

  val fullCrimeStats =  dsCrimesTotMonth.join(dsCrimesFreq,"DISTRICT")
    .withColumn("lat",lit(latVal))
    .withColumn("lng",lit(lngVal))
    .repartition(1)
    .write.parquet(s"${args(2)}")
  //.show(false)
  println("File written")




}
