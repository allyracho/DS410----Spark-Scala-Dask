 import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Q3{
     def main(args: Array[String]) = {
          val conf = new SparkConf().setAppName("q3")
          val sc = new SparkContext(conf)
          val fb = sc.textFile("hdfs:///ds410/facebook").map{x => x.split("\t")}
          val kvrdd = fb.map{x => (x(0).toInt, x(1).toInt)}
          val neighbors = fb.aggregateByKey( (0, 0) )({ // init state for aggregating 
values with same key
                    case ((num, total), nextitem) => (num+1, total + nextitem)
                },{
                    case ((num, total), (nextnum, nexttotal)) => (num + nextnum, 
total + nexttotal)
                })
          val grouped = kvrdd.groupByKey
          val filteredx = kvrdd.filter(i(1) > i(0)).count
          val result = .... map{i => neighbors, grouped._2.size, filteredx}
          result.saveAsTextFile("hdfs:///user/axr402/q3")
     }
}

