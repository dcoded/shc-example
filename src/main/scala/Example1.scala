import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


case class Employee(key:String,
                    firstName:String,
                    lastName:String,
                    middleName:String,
                    addressLine:String,
                    city:String,
                    state:String,
                    zipCode:String)



object Example1 {

  def main(args: Array[String] ): Unit = {

    def catalog=
      s"""  {
         |    "table"  :{"namespace":"hbasepoc","name":"employee"},
         |    "rowkey" :"key",
         |    "columns":{
         |      "key"         :{"cf":"rowkey",  "col":"key",        "type":"string"},
         |      "fName"       :{"cf":"person",  "col":"firstName",  "type":"string"},
         |      "lName"       :{"cf":"person",  "col":"lastName",   "type":"string"},
         |      "mName"       :{"cf":"person",  "col":"middleName", "type":"string"},
         |      "addressLine" :{"cf":"address", "col":"addressLine","type":"string"},
         |      "city"        :{"cf":"address", "col":"city",       "type":"string"},
         |      "state"       :{"cf":"address", "col":"state",      "type":"string"},
         |      "zipCode"     :{"cf":"address", "col":"zipCode",    "type":"string"}
         |    }
         |  }
         |  """
        .stripMargin

    val data=Seq(
      Employee("1", "Abby",     "Smith",      "K",  "3456main",   "Orlando",  "FL", "45235"),
      Employee("2", "Amaya",    "Williams",   "L",  "123Orange",  "Newark",   "NJ", "27656"),
      Employee("3", "Alchemy",  "Davis",      "P",  "Warners",    "Sanjose",  "CA", "34789"))

    val spark = SparkSession.builder()
      .appName("HBase Insert")
      .master("local[*]")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    import spark.implicits._

    val rdd = sparkContext.parallelize(data)
    val df  = rdd.toDF("key", "fName", "lName", "mName", "addressLine", "city", "state", "zipCode")

    df.show()

    df.write
      .options(Map(
        HBaseTableCatalog.tableCatalog  -> catalog,
        HBaseTableCatalog.newTable      -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

}