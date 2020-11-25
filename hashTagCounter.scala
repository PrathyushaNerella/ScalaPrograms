/* Program to count the occurrences of hashtag and sort*/
package xi.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object HelloSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Hashtag WordCount Sorted")

    val sc = new SparkContext(conf)

    val tweetsPath = args(0)
    val outputDataset = args(1)

    val tweetsRaw: RDD[String] = sc.textFile(tweetsPath)

    val wordCounts = tweetsRaw.
      flatMap(line => line.split("\\s+"))
      .filter(_.startsWith("#"))
        .groupBy(identity)
        .map { case (word, words) => (word, words.size) }

    val sortedCounts = wordCounts
      .sortBy {case (_,count) => -count}
        .map {case (word,count) => s"$word\t$count"}

    sortedCounts.saveAsTextFile(outputDataset)


  } //main

}//HelloSpark


