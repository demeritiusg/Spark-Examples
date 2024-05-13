import org.apache.spark.sql{SparkSession, DataFrame}

object TransformToParquet{
    def main(args: Array[String]): Unit = {

        val sc = SparkSession.builder()
            .builder("TransformToParquet")
            .getOrCreate()

        val filePath = "file/path/file.csv"

        val df = sc.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(filePath)

        val destinationFilePath = "parquet/path/file.parquet"

        df.write
            .mode("overwrite")
            .parquet(destinationFilePath)

        sc.stop()
    }
}