import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object Main  {
  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("Please enter a file source for the taxi data, zone data and output destination :)")
      System.exit(1)}

    // need args for main method.
    // first arg is source to the big data set in parquet form
    // second arg is to zone data set

    val spark: SparkSession = SparkSession.builder()
      .appName("BigTaxi")
      .config("spark.master", "local")
      .getOrCreate()


    val bigDF: DataFrame = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .load(args(0))

    val zones: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))
    //zones.printSchema()
    //zones.show(4)




    //
    //  // Which zones haves the most pickups?
    //
    val PUbyZONE: DataFrame = bigDF
      .groupBy(col("pickup_taxizone_id")) // Grouping the dataset based on Pick-Up location
      .agg(functions.count("*").as("totaltrips")) // counting the number of pickups per zone
      .join(zones, col("pickup_taxizone_id") === col("LocationID")) // joining with taxi zone csv via Pick up location ID
      .drop("LocationID", "service_zone") // dropping unnecessary (duplicate) columns
      .orderBy(col("totaltrips").desc_nulls_last) // order by total trips with null values at bottom.


    //
    //
    //  // Which borough has the most pickups/
    //
    val PUbyB: DataFrame = PUbyZONE.groupBy(col("Borough"))
      .agg(functions.sum(col("totaltrips")).as("totaltrips"))
      .orderBy(col("totaltrips").desc_nulls_last)

    //

    // Which hours of the day are peak pick-up times?
    val PUbyhour: DataFrame = bigDF
      .withColumn("hour", hour(col("pickup_datetime")))
      .groupBy("hour")
      .agg(functions.count("*").as("total"))
      .orderBy(col("total").desc_nulls_last)


    // How are trips distributed?

    //  val tripsdf: DataFrame = bigDF
    //    .select(col("trip_distance")
    //      .as("distance"))

    val longthreshold = 50 // miles
    val distStats: DataFrame = bigDF
      .filter(bigDF("trip_distance") > 0)
      .filter(bigDF("trip_distance") < 70)
      .select(
        count("*").as("count"),
        mean("trip_distance").as("mean"),
        stddev("trip_distance").as("StDev"),
        min("trip_distance").as("min"),
        max("trip_distance").as("max")
      )

    val num: Long = bigDF.count()

    println("Pickups by zone")
    PUbyZONE.show(10)
    println("Pickups by borough")
    PUbyB.show(5)
    println("Pickups by hour")
    PUbyhour.show(10)
    println("Distribution of trips")
    distStats.show()
    println(s"Total number of rows in dataset: $num")

    distStats.write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv(args(2))


    //bigDF.printSchema()

    // End
  }
}









