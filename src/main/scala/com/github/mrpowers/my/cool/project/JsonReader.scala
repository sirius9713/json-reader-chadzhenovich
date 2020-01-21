package com.github.mrpowers.my.cool.project

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

object JsonReader extends App {

   case class Winemag(id: Option[Int] = None,
                      country: Option[String] = None,
                      points: Option[Int] = None,
                      price: Option[Double] = None,
                      title: Option[String] = None,
                      variety: Option[String] = None,
                      winery: Option[String] = None)

  implicit val formats: AnyRef with Formats = {
    Serialization.formats(FullTypeHints(List()))
  }
  val spark = SparkSession.builder().master( master = "local").getOrCreate()

  val sc = spark.sparkContext

  val filename = args (0)

//  val js = sc.textFile(filename)
//    .map(s => parse(s).extract[Winemag])
//      .collect()

  val js = sc.textFile(filename)
    .map(s => parse(s).extract[Winemag])
    .collect()
  println("before")
  println(js.toList mkString "\n")
  println("after")
//  val js = parse(json).extract[Winemag]
}
