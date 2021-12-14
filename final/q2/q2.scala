import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q3{
     def main(args: Array[String]) = {
          val conf = new SparkConf().setAppName("q3")
          val sc = new SparkContext(conf)
          val fb = sc.textFile("hdfs:///ds410/facebook").map{x => x.split("\t")}
          val kvrdd = fb.map{x => (x(0).toInt, x(1).toInt.filter(_ > 1000))}
          val grouped = kvrdd.groupByKey
          val count = grouped.map{i => i._1, i._2.size.filter(_ > 5)}
          result.saveAsTextFile("hdfs:///user/axr402/q2")
     }
}

