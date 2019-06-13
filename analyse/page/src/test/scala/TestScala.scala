/**
  * Created by 38636 on 2019/6/12.
  */
object TestScala {
  def main(args: Array[String]): Unit = {
    val list1 = List("1", "2", "3")
    val list2 = List(4, 5)
    var z1 = list1 zip list2
    println(z1)

  }
}
