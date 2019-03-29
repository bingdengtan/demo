import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object Main extends App {
	println("Hello, World!")
  
	val conf = new SparkConf(true)
	conf.set("spark.cassandra.connection.host", "172.19.37.14")
	conf.set("spark.cassandra.auth.username", "cassandra")
	conf.set("spark.cassandra.auth.password", "cassandra")
	println("1")	
	val sc = new SparkContext("local", "example", conf)
	println("2")
	val rdd = sc.cassandraTable("ks01", "tb01")
	println("3")
	println(rdd.count)	
}