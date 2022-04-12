import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q3{
     def main(args: Array[String]) = {
          val conf = new SparkConf().setAppName("q3")
          val sc = new SparkContext(conf)
          val fb = sc.textFile("hdfs:///ds410/facebook").map{x => x.split("\t")}
          val kvrdd = fb.map{x => (x(0).toInt, x(1).toInt)}
          val kvrdd2 = kvrdd
          val joined = kvrdd2.join(kvrdd2)
          val flipped_join = joined.map{case (c, (a,b)) => ((a,b),c)}
          val friends_friends = flipped_join.distinct.groupByKey
          val sum_friends = friends_friends.map{i => (i._1._1, i._2.sum)}
          sum_friends.saveAsTextFile("hdfs:///user/axr402/q3")
     }
}
