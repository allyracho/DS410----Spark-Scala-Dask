case class Neumaier(sum: Double, c: Double)

object HW {

   def q1_middle(x: Int, y: Int, z:Int) : Int = {
      if ((x>y && x<z) || (x<y && x>z)){
         x
      }
      
      else if ((y>x && y<z) || (y<x && y>z)){
         y
      }
      
      else{
         z
      }

   }

   def q2_interpolation(name: String, age: Int) : String = {
      val name_lower = name.toLowerCase()
      if(age >= 21){
         s"hello, $name_lower"
      }
      else{
         s"howdy, $name_lower"
      }
   }

   def q3_polynomial(arr: Seq[Double]) : Double = {
      val result = arr.foldLeft(0.0,0)((x:Tuple2[Double, Int], y) => (x._1+(y*scala.math.pow(2, x._2)),x._2+1))
      result._1      
   }

   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int) : Int = {
      f(f(x, y), f(y, z))
   }

   def q5_stringy(start: Int, n: Int) : Vector[String] = {
      val result = Vector.tabulate(n)(_ + start)
      result.map(_.toString)
   }

   def q6_modab(a: Int, b: Int, c: Vector[Int]) : Vector[Int] = {
      val result_filter_a = c.filter(_ >= a)
      val result_filter_b = result_filter_a.filter(_ % b != 0)
      result_filter_b.toVector

   }

   def q7_find(x: Vector[Int])(f: (Int) => Boolean) : Int = {
      val true_lst = x.filter(f(_))
      val last_ele = true_lst.last

      if (true_lst.isEmpty){
      -1
      }
      
      x.indexOf(last_ele)

   }
   /*

   @annotation.tailrec
   def q8_find_tail(x: Seq[Int])(f: (Int) => Boolean) : Int = {
      if (f(_)) x.indexOf(_) else q8_find_tail(x.filter(_)) 

      if (x.isEmpty){
      -1
      }  

   }

   def q9_neumaier(a: Seq[Double]) : Double = {
      val t = a.foldLeft(0.0)((x, y) => sum+(x+y))
      if (sum.abs >= x.abs) {
         c += a.foldLeft(0.0)((x,y) => (sum - t) + (x+y))
     }
      else {
         c += a.foldLeft(0.0)((x,y) => ((x+y) - t)+ sum)
      }
      sum+c

   }
   */



}
