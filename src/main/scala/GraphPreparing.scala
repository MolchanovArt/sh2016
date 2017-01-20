import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.slf4j.{Logger, LoggerFactory}

import scala.math.log

/**
  * Created by mart on 10.01.17.
  */
object GraphPreparing {

  val Log: Logger = LoggerFactory.getLogger(GraphPreparing.getClass)

  val NumPartitions = 200
  val NumPartitionsGraph = 107

  def graphPrepare(sc: SparkContext, graphPath: String): RDD[UserFriends] = {
    sc.textFile(graphPath)
      .map(line => {
        val lineSplit = line.split("\t")
        val user = lineSplit(0).toInt
        val friends = {
          lineSplit(1)
            .replace("{(", "")
            .replace(")}", "")
            .split("\\),\\(")
            .map(t => t.split(","))
            .map(splitStr => OneWayFriendship(splitStr(0).toInt,
              FriendshipHelpers.normalizeFriendshipType(splitStr(1).toInt)))
        }
        UserFriends(user, friends)
      })
  }

  def reversedGraphPrepare(reversedGraphPath: String,
                           graph: RDD[UserFriends],
                           sqlc: SQLContext): Any = {
    import sqlc.implicits._

    if (new File(reversedGraphPath).exists()) {
      Log.warn("Reversed Graph SKIPPED cause Exists!")
    } else {
      // flat and reverse graph
      // step 1.a from description
      graph
        .filter(userFriends => userFriends.friends.length >= 2 &&
          userFriends.friends.length <= 1500)
        .flatMap(userFriends => userFriends.friends.map(
          x => (x.anotherUser, OneWayFriendship(userFriends.user,
            FriendshipHelpers.invertFriendshipType(x.fType)))))
        .groupByKey(NumPartitions)
        .map({ case (userFromList, oneWayFriendshipSeq) => oneWayFriendshipSeq.toArray })
        .filter(userFriends => userFriends.length >= 2 && userFriends.length <= 2000)
        .map(userFriends => userFriends.sortBy({
          case oneWayFriendship => oneWayFriendship.anotherUser
        }))
        .map(friends => new Tuple1(friends))
        .toDF
        .write.parquet(reversedGraphPath)
    }
  }

  def commonFriendsPrepare(commonFriendsPath: String,
                           reversedGraphPath: String,
                           sqlc: SQLContext): Unit = {
    import sqlc.implicits._

    if (new File(commonFriendsPath).exists()) {
      Log.warn("Commons Friend Exists - SKIPPED")
    } else {
      // for each pair of ppl count the amount of their common friends
      // amount of shared friends for pair (A, B) and for pair (B, A) is the same
      // so order pair: A < B and count common friends for pairs unique up to permutation
      // step 1.b
      for (partition <- 0 until NumPartitionsGraph) {
        val commonFriendsCounts = {
          sqlc.read.parquet(reversedGraphPath)
            .map(t => DataPreparingHelpers.generatePairs(
              t.getSeq[GenericRowWithSchema](0).map(t =>
                new OneWayFriendship(t.getAs[Int](0), t.getAs[Int](1))),
              NumPartitionsGraph, partition))
            .flatMap(pairs => pairs.map(
              friendship => (friendship.user1, friendship.user2) ->
                SquashedFriendship(1.0 / log(friendship.middleUserCommonFriendSize),
                  FriendshipHelpers.transformedFriendTypeToFAcc(friendship.granulatedMergedFType)))
            )
            .reduceByKey({ case (SquashedFriendship(weight1, fAccumulator1),
            SquashedFriendship(weight2, fAccumulator2)) =>
              SquashedFriendship(weight1 + weight2,
                FriendshipHelpers.mergeAccs(fAccumulator1, fAccumulator2))
            })
            .map({
              case ((user1, user2), SquashedFriendship(fScore, fAccumulator)) =>
                PairWithCommonFriends(user1, user2, fScore, fAccumulator)
            })
            .filter(pair => pair.commonFriendsCount >= 2)
        }
        commonFriendsCounts.toDF.repartition(4).write.parquet(commonFriendsPath + "/part_" + partition)
      }
    }
  }
}
