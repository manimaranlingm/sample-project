package pak
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class CustomPartitioner(val numPartitions: Int) extends org.apache.spark.Partitioner {
  override def getPartition(key: Any): Int = key match {

    case k: Int => k % numPartitions
  
    //case _ => throw new IllegalArgumentException("Unsupported key type")
  }
}
object objj {
   def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop") // Put your drive accordingly

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).getOrCreate() // DataFrame

    import spark.implicits._

    val df = spark.read.format("csv").option("header","true").load("file:///home/mani/allc.csv")

    df.show()

    // Assuming you have a DataFrame with a column named "key" that you want to partition by
    val numPartitions = 1
    val customPartitioner = new CustomPartitioner(numPartitions)
    
val partitionedDF = df.repartition(numPartitions)


   
    partitionedDF.write.format("csv").mode("overwrite").save("file:///home/mani/sparkdata/jarjob/file.csv")
  }
  
}