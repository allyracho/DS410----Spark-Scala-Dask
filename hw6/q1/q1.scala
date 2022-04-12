import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q1{
   def main(args:Array[String]) = {

      val conf = new SparkConf().setAppName("q1")  
      val sc = new SparkContext(conf)
      val customer = sc.textFile("hdfs:///ds410/retailclean")
      val header = customer.first()
      val no_header = customer.filter(_ != header)
      val fields = no_header.map{x => x.split("\t")}
      val id_kv = fields.map{i => (i(6), (i(5).toDouble* i(3).toInt))} //subset of fields only with id, price, quantity
      val agg = id_kv.aggregateByKey((0, 0))({case ((num, total), nextitem) => (num+1, (total + nextitem).toInt)}, {case ((num, total), (nextnum, nexttotal)) => (num+nextnum, (total + nexttotal))})
      val result =  agg.mapValues(i =>1.0* i._2 / i._1)
      result.saveAsTextFile("hdfs:///user/axr402/q1")
   }
}
