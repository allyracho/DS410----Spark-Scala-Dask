import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q3{
     def main(args: Array[String]) = {
          val conf = new SparkConf().setAppName("q3")
          val sc = new SparkContext(conf)
          val fb = sc.textFile("hdfs:///ds410/facebook").map{x => x.split("\t")}
          val kvrdd = fb.map{x => (x(0).toInt, x(1).toInt)}
          val neighbors = kvrdd.aggregateByKey( (0, 0) )({case ((num, total), nextitem) => (num+1, total + nextitem)},{case ((num, total), (nextnum, nexttotal)) => (num + nextnum, total + nexttotal)}) //count of neighbors and total sum
          val filteredx = kvrdd.filter(i(1) > i(0)).count
          val result = .... map{i => (neighbors._2._1, neighbors._2._2, filteredx)}
          result.saveAsTextFile("hdfs:///user/axr402/q3")
     }
}

