import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q2{
     def main(args: Array[String]) = {
          val conf = new SparkConf().setAppName("q2")
          val sc = new SparkContext(conf)
          val fb = sc.textFile("hdfs:///ds410/facebook").map{x => x.split("\t")}
          val kvrdd = fb.map{x => (x(0), x(1).toInt))}
          val grouped = kvrdd.groupByKey
          val filtered = grouped.map{i => (i._1, i._2.filter(_ > 1000))}
          val counts = filtered.map{i => (i._1, i._2.size)}
          result.saveAsTextFile("hdfs:///user/axr402/q2")
     }
}

