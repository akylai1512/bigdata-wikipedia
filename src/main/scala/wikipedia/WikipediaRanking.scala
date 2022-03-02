package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.util.Properties.isWin

case class WikipediaArticle(title: String, text: String):
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)

object WikipediaRanking extends WikipediaRankingInterface:
  // Reduce Spark logging verbosity

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")


  val conf: SparkConf = new SparkConf().setAppName("wikipedia").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines).map(WikipediaData.parse)

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.filter(_.text.toLowerCase.split(" ").contains(lang.toLowerCase)).count().toInt


  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =  {
    rdd.cache()
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(_._2).reverse
  }

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val articleLanguagePairs = rdd.flatMap(article => {
      val langsMentioned = langs.filter(lang => article.text.split(" ").contains(lang))
      langsMentioned.map(lang => (lang, article))
    })
    articleLanguagePairs.groupByKey
  }

  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues(_.size).sortBy(-_._2).collect().toList
  }

  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap(article => {
      langs.filter(lang => article.text.split(" ").contains(lang)).map((_, 1))
    }).reduceByKey(_ + _).sortBy(_._2).collect().toList.reverse
  }

  def main(args: Array[String]): Unit =

    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    println(timing)
    sc.stop()

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
