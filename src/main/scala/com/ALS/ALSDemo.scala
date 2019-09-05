package com.ALS

import java.io.File

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity

object ALSDemo {
  def main(args: Array[String]): Unit = {
    val filePath = "md/data3/input/data.csv"

    val dataModel = new FileDataModel(new File(filePath))

    val userSimilarity = new PearsonCorrelationSimilarity(dataModel)

    val userNeighborhood = new NearestNUserNeighborhood(2, userSimilarity, dataModel)

    val recommender = new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity)

    val userIterator = dataModel.getUserIDs
    while (userIterator.hasNext) {
      println("===============")

      val userId = userIterator.nextLong()

      val otherUserIterator = dataModel.getUserIDs

      while (otherUserIterator.hasNext) {
        val otherUserId = otherUserIterator.nextLong()
        println("用户 " + userId + " 与用户 " + otherUserId + " 的相似度为："
          + userSimilarity.userSimilarity(userId, otherUserId))
      }
      val userN = userNeighborhood.getUserNeighborhood(userId)

      import scala.collection.JavaConverters._
      val recommendedItems = recommender.recommend(userId, 2).asScala
      println("用户 " + userId + " 的2-最近邻是 " + userN.toList);

      if (recommendedItems.size > 0) {
        for (item <- recommendedItems) {
          println("推荐的物品"+ item.getItemID()+"预测评分是 "+ item.getValue());
        }
      } else {
        println("无商品推荐")
      }
    }
  }
}
