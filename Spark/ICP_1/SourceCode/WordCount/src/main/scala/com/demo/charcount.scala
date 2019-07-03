package com.demo

object charcount {
  def main(args: Array[String]): Unit ={
    val charinput: String = "This is Character count program".toLowerCase
    val map = scala.collection.mutable.HashMap.empty[Char,Int]
    for (chars <- charinput){
      if(map.contains(chars))
      map(chars) +=  1
      else
        map.+= ((chars,1))
    }
    println(map)
  }

}
