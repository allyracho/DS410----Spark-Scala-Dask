import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q2{
   def main(args:Array[String]) = {
     val conf = new SparkConf().setAppName("q2")
     val sc = new SparkContext(conf)
     val customer = sc.textFile("hdfs:///ds410/retailclean")
     val header = customer.first() //get header
     val no_header = customer.filter(_ != header) //remove header
     val fields = customer.map{x => x.split("\t")}
     val id_country = fields.map{i => (i(6), i(7))} //subset of fields only with id, country

     val distinct_countries = id_country.distinct.groupByKey //get distinct countries per key
     val count_country = distinct_countries.map{i => (i._1, i._2.size)} //count the number of distinct countries per key
     count_country.saveAsTextFile("hdfs:///user/axr402/q2")

     }
}

