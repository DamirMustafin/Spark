package org.example

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.tartarus.snowball.ext.russianStemmer

object LabTwo {
  val PATH: String = "src/main/data"
  val NODES: Int = 3

  def main(args: Array[String]): Unit = {
    // This line to avoid an info message in log
    BasicConfigurator.configure(new NullAppender)

    val spark: SparkSession = SparkSession.builder()
      .master(s"local[$NODES]")
      .appName("LabTwo")
      .getOrCreate()

    val stopWords: Array[String] = spark.sparkContext.textFile(s"$PATH/stop_words.txt").collect()
    val rdd: RDD[String] = spark.sparkContext.textFile(s"$PATH/var_03.txt")
    // To write all file
    // rdd.collect().foreach(println)
    // Only num of line
    // rdd.take(17).foreach(println)

    val wordCounts: RDD[(String, Int)] = wordCounter(rdd, stopWords)
    // All list of words
    // wordCounts.foreach(println)
    println("\n\tNumber of words: ")
    println(wordCounts.count())

    println("\n\tTop50 most common words: ")
    val mostCommon: Array[(String, Int)] = TopN(wordCounts, ascending = false)
    mostCommon.foreach(println)

    println("\n\tTop50 least common words: ")
    val leastCommon: Array[(String, Int)] = TopN(wordCounts, ascending=true)
    leastCommon.foreach(println)

    val stemmed: RDD[((String, Iterable[String]), Int)] = wordCounts
      .mapPartitions(stemming)
      .groupBy(word => word._2)
      .map(word => (word._1, word._2.map(i => i._1._1)) -> word._2.map(i => i._1._2).sum)

    println("\n\tTop50 most common stems: ")
    val mostStemmed: Array[((String, Iterable[String]), Int)] = stemmed
      .sortBy(word => word._2, ascending = false)
      .take(50)
    mostStemmed.foreach(println)

    println("\n\tTop50 least common stems: ")
    val leastStemmed: Array[((String, Iterable[String]), Int)] = stemmed
      .sortBy(word => word._2, ascending = true)
      .take(50)
    leastStemmed.foreach(println)
    }

  def wordCounter(text: RDD[String], stopWords: Array[String]): RDD[(String, Int)] = {
    text.flatMap(line => line.toLowerCase.split(" "))
      .map(word => word.replaceAll("""[;:,.!?"«» ()“–]""", ""))
      .filter(word => word.length > 1 && !stopWords.contains(word))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def TopN(text: RDD[(String, Int)], ascending: Boolean, count: Int = 50): Array[(String, Int)] = {
    text.sortBy(f => f._2, ascending = ascending).take(count)
  }

  def stemming(iter: Iterator[(String, Int)]): Iterator[((String, Int), String)] = {
    val stemmer: russianStemmer = new russianStemmer
    iter.map(word => (word._1, word._2) -> {
      stemmer.setCurrent(word._1)
      stemmer.stem
      stemmer.getCurrent
    })
  }
}
