package pack

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import scala.util._
import org.apache.spark.sql.SparkSession

case class Order(OrderID: String, UserName: String, OrderTime: String, OrderType: String, Quantity: String, Price: String)

object Lseg {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop")


			println("===started===")
			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

			.set("spark.driver.allowMultipleContexts", "true")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession
			                .builder
			                .getOrCreate()

    import spark.implicits._
    //Note: In my local the file path was "file:///C:/Users/Personal/Desktop/tcs-coding-excercise/exampleOrders.csv"
    val ordersRDD = sc.textFile("file:///C:/Users/Personal/Desktop/tcs-coding-excercise/exampleOrders.csv")
    val finalRDD = ordersRDD.map( x => x.split(",")).map(x => Order(x(0),x(1),x(2),x(3),x(4),x(5)))
    val ordersDF = finalRDD.toDF()

    ordersDF.createOrReplaceTempView("orders")

    val matchesDF = spark.sql("""
           SELECT o1.OrderID AS Order_ID_1, o2.OrderID AS Order_ID_2,
           o1.UserName AS User_Name_1, o2.UserName AS User_Name_2,
           o1.OrderTime AS Order_Time_1, o2.OrderTime AS Order_Time_2,
           o1.OrderType AS Order_Type_1, o2.OrderType AS Order_Type_2,
           o1.Quantity AS Quantity_1, o2.Quantity AS Quantity_2,
           o1.Price AS Price_1, o2.Price AS Price_2
           FROM orders o1 join orders o2 on o1.Quantity = o2.Quantity
           WHERE (o1.OrderID < o2.OrderID)
           AND (o1.OrderType != o2.OrderType)
           AND ((o1.OrderType = 'BUY' AND o1.Price >= o2.Price)
           OR (o1.OrderType = 'SELL' AND o1.Price <= o2.Price))
                              """)
    matchesDF.createOrReplaceTempView("matches")

    val finalopDF = spark.sql("""select Order_ID_2,Order_Id_1,Order_Time_2,Quantity_2,Price_1 from matches order by Order_ID_2 desc""")
    finalopDF.show()
  }
}