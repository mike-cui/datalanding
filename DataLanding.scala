import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{RDD, NewHadoopRDD}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.{FileSystem}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object DataLanding {

  val dataSep = '|'

  val logger = LoggerFactory.getLogger(this.getClass().getName)

  def main(args: Array[String]) {

    if (args.length < 1) {
      logger.error("Missing parameters")
      System.exit(1)
    }

    // Get config values for source files and destination Hive DB (HDFS)
    val configPath = args(0)

    val config = ConfigFactory.parseFile(new File(configPath))

    val customerFileFolder = config.getString("customerFileFolder")
	val addressFileFolder = config.getString("addressFileFolder")
	val transactionFileFolder = config.getString("transactionFileFolder")

    val hiveDB = config.getString("hiveDB")


    /* First, below we are setting initial Spark variables and configurations */
    val conf = new SparkConf().setAppName("RBC Coding Test")
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    try{

      // Create tables
      hc.sql(s"CREATE TABLE IF NOT EXISTS $hiveDB.customer (first_name string,last_name string,account_id string,income string, address_id string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'")
      hc.sql(s"CREATE TABLE IF NOT EXISTS $hiveDB.address (city string,postal_code string, address_id string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'")
      hc.sql(s"CREATE TABLE IF NOT EXISTS $hiveDB.transaction (account_id string, address_id string, transaction_amount string, transaction_date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'")

      val fc = classOf[TextInputFormat]
      val kc = classOf[LongWritable]
      val vc = classOf[Text]

      // Import Customer files
      logger.info("Import customer file started")
      val customerFileList = fs.listFiles(new org.apache.hadoop.fs.Path(customerFileFolder), true)
      if (customerFileList.hasNext()) {

        val customerFile = sc.newAPIHadoopFile(s"$customerFileFolder/*/*", fc, kc, vc, sc.hadoopConfiguration)

        val customerFileWithFileNames: RDD[String] = customerFile.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
          .mapPartitionsWithInputSplit((inputSplit, iterator) => {

            // Extract file name
            val fileName = inputSplit.asInstanceOf[FileSplit].getPath.getName()

            // Build records with fields separated and file names
            iterator.map(tup => (tup._2.toString() + dataSep + fileName))
          }
          )

        // Remove Header rows and empty lines
        val customerFileFiltered = customerFileWithFileNames.map(line => line.split(dataSep))
          .filter(x => x(0) != "first_name")
          .filter(x => x.length == 6 )
          .map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))

        // Create the dataframe with the correct column names and save the results in hive
        val customerFileDF = hc.createDataFrame(customerFileFiltered)
          .withColumnRenamed("_1", "first_name")
          .withColumnRenamed("_2", "last_name")
          .withColumnRenamed("_3", "account_id")
          .withColumnRenamed("_4", "income")
          .withColumnRenamed("_5", "address_id")
          .withColumnRenamed("_6", "input_file")

        customerFileDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB.customer")
      }
      logger.info("Import customer file completed")

      // Import Address files
      logger.info("Import address file started")
      val addressFileList = fs.listFiles(new org.apache.hadoop.fs.Path(addressFileFolder), true)
      if (addressFileList.hasNext()) {

        val addressFile = sc.newAPIHadoopFile(s"$addressFileFolder/*/*", fc, kc, vc, sc.hadoopConfiguration)

        val addressFileWithFileNames: RDD[String] = addressFile.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
          .mapPartitionsWithInputSplit((inputSplit, iterator) => {

            // Extract file name
            val fileName = inputSplit.asInstanceOf[FileSplit].getPath.getName()

            // Build records with fields separated and file names
            iterator.map(tup => (tup._2.toString() + dataSep + fileName))
          }
          )

        // Remove Header rows and empty lines
        val addressFileFiltered = addressFileWithFileNames.map(line => line.split(dataSep))
          .filter(x => x(0) != "city")
          .filter(x => x.length == 4 )
          .map(x => (x(0), x(1), x(2), x(3)))

        // Create the dataframe with the correct column names and save the results in hive
        val addressFileDF = hc.createDataFrame(addressFileFiltered)
          .withColumnRenamed("_1", "city")
          .withColumnRenamed("_2", "postal_code")
          .withColumnRenamed("_3", "address_id")
          .withColumnRenamed("_4", "input_file")

        addressFileDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB.address")
      }
      logger.info("Import address file completed")

	  // Import Transaction files
      logger.info("Import transaction file started")
      val transactionFileList = fs.listFiles(new org.apache.hadoop.fs.Path(transactionFileFolder), true)
      if (transactionFileList.hasNext()) {

        val transactionFile = sc.newAPIHadoopFile(s"$transactionFileFolder/*/*", fc, kc, vc, sc.hadoopConfiguration)

        val transactionFileWithFileNames: RDD[String] = transactionFile.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
          .mapPartitionsWithInputSplit((inputSplit, iterator) => {

            // Extract file name
            val fileName = inputSplit.asInstanceOf[FileSplit].getPath.getName()

            // Build records with fields separated and file names
            iterator.map(tup => (tup._2.toString() + dataSep + fileName))
          }
          )

        // Remove Header rows and empty lines
        val transactionFileFiltered = transactionFileWithFileNames.map(line => line.split(dataSep))
          .filter(x => x(0) != "account_id")
          .filter(x => x.length == 5 )
          .map(x => (x(0), x(1), x(2), x(3), x(4)))

        // Create the dataframe with the correct column names and save the results in hive
        val transactionFileDF = hc.createDataFrame(transactionFileFiltered)
          .withColumnRenamed("_1", "account_id")
          .withColumnRenamed("_2", "address_id")
          .withColumnRenamed("_3", "transaction_amount")
          .withColumnRenamed("_4", "transaction_date")
          .withColumnRenamed("_5", "input_file")

        transactionFileDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB.transaction")
      }
      logger.info("Import transaction file completed")
	  
      // Query sample 
      val unionQuery =
        s"""   SELECT
            |    A.first_name,
			|	 A.last_name,
			|	 A.income,
            |    sum(C.transaction_amount) AS total_amount,
            |    B.city,
			|	 month(B.transaction_date) AS transaction_month
            |  FROM
            |    $hiveDB.customer AS A
            |    INNER JOIN $hiveDB.address AS B ON A.address_id = B.address_id
            |    INNER JOIN $hiveDB.transaction AS C ON A.account_id = C.account_id and A.address_id = C.address_id
			|  GROUP BY A.first_name, A.last_name, A.income, B.city, month(B.transaction_date)
			|  HAVING sum(C.transaction_amount) >= 100000 AND sum(C.transaction_amount) <= 150000
            |      """.stripMargin


      val unionDF = hc.sql(unionQuery)
      logger.info(unionQuery)
      unionDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$hiveDB.temp_union_records")

      logger.info("Done")

    } catch {
      case e: Exception => logger.error("Error during processing")
    }

  }
}
