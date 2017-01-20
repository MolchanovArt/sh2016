import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.math._
/**
  * Created by mart on 10.01.17.
  */
object DataPreparingHelpers {
  val JAN_1_2017: Long = new Date(2017, 0, 1).getTime

  // step 4
  def prepareAgeSexBroadcast(sc: SparkContext,
                             demographyPath: String): Broadcast[Map[Int, Profile]] = {
    val ageSex =
      sc.textFile(demographyPath)
        .map(line => {
          val lineSplit = line.trim().split("\t")
          defaultInt(lineSplit, 0) -> Profile(
            defaultLong(lineSplit, 1),
            defaultInt(lineSplit, 2),
            defaultInt(lineSplit, 3),
            defaultLong(lineSplit, 4),
            defaultLong(lineSplit, 5),
            defaultLong(lineSplit, 6))
        })
    sc.broadcast(ageSex.collectAsMap())
  }

  def generatePairs(pplWithCommonFriends: Seq[OneWayFriendship],
                    numPartitions: Int, k: Int): ArrayBuffer[MiddleFriendship] = {
    val pairs = ArrayBuffer.empty[MiddleFriendship]
    for (i <- pplWithCommonFriends.indices) {
      val f1 = pplWithCommonFriends(i)
      if (f1.anotherUser % numPartitions == k) {
        for (j <- i + 1 until pplWithCommonFriends.length) {
          val f2 = pplWithCommonFriends(j)
          pairs.append(MiddleFriendship(f1.anotherUser, f2.anotherUser,
            pplWithCommonFriends.length,
            FriendshipHelpers.combineFTypesToTransFType(f1.fType, f2.fType)))
        }
      }
    }
    pairs
  }

  def prepareData(commonFriendsCounts: RDD[PairWithCommonFriends],
                  positives: RDD[((Int, Int), Double)],
                  ageSexBC: Broadcast[scala.collection.Map[Int, Profile]]):
  RDD[((Int, Int), (Vector, Option[Double]))] = {

    commonFriendsCounts
      .map(pair => (pair.person1, pair.person2) -> {
        val user1 = ageSexBC.value.getOrElse(pair.person1, Profile(0, 0, 0, 0, 0, 0))
        val user2 = ageSexBC.value.getOrElse(pair.person2, Profile(0, 0, 0, 0, 0, 0))
        val fType = pair.fAccumulator
        Vectors.dense(
          pair.commonFriendsCount,
          ageToYears(user1.age),
          difInYears(user1.age, user2.age),
          combineSex(user1.sex, user2.sex),
          profileTooCloseCreationsDates(user1.create_date, user2.create_date),
          min(profileLogAge(user1.create_date), profileLogAge(user2.create_date)),

          if (user1.country != 0 && user2.country != 0)
            if (user1.country == user2.country) 1.0 else 0.0 else -1.0,
          if (user1.location != 0 && user2.location != 0)
            if (user1.location == user2.location) 1.0 else 0.0 else -1.0,
          if (user1.loginRegion != 0 && user2.loginRegion != 0)
            if (user1.loginRegion == user2.loginRegion) 1.0 else 0.0 else -1.0,

          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.BEST_FRIENDS_CNT_MASK,
            FriendshipHelpers.BEST_FRIENDS_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.FAMILY_CNT_MASK,
            FriendshipHelpers.FAMILY_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.BEST_FAMILY_CNT_MASK,
            FriendshipHelpers.BEST_FAMILY_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.SCHOOL_CNT_MASK,
            FriendshipHelpers.SCHOOL_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.WORK_CNT_MASK,
            FriendshipHelpers.WORK_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.UNIVERSITY_CNT_MASK,
            FriendshipHelpers.UNIVERSITY_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.ARMY_CNT_MASK,
            FriendshipHelpers.ARMY_CNT_SHIFT).toDouble,
          FriendshipHelpers.getFriendTypeCountAcc(
            fType,
            FriendshipHelpers.PLAY_CNT_MASK,
            FriendshipHelpers.PLAY_CNT_SHIFT).toDouble
        )
      }
      )
      .leftOuterJoin(positives)
  }

  def combineSex(sex1: Int, sex2: Int): Double = {
    if (sex1 == 0 || sex2 == 0) {
      -1.0
    } else if (sex1 == sex2) {
      if (sex1 == 1) 1.0 else 2.0
    } else {
      if (sex1 == 1) 3.0 else 4.0
    }
  }

  def ageToYears(ageInDay: Int): Int = {
    ageInDay / 365
  }

  def difInYears(ageInYears1: Int, ageInYears2: Int): Double = {
    1.0 * (ageToYears(ageInYears1) - ageToYears(ageInYears2))
  }

  def defaultInt(strArray: Array[String], index: Int): Int = {
    if (strArray.length <= index) {
      0
    } else {
      val str = strArray(index)
      if (str == null || str.isEmpty) 0 else str.toInt
    }
  }

  def defaultLong(strArray: Array[String], index: Int): Long = {
    if (strArray.length <= index) {
      0L
    } else {
      val str = strArray(index)
      if (str == null || str.isEmpty) 0L else str.toLong
    }
  }

  def profileTooCloseCreationsDates(creationDate1: Long,
                                    creationDate2: Long): Double = {
    if (abs(creationDate1 - creationDate2) <= TimeUnit.DAYS.toMillis(7)) 1.0 else 0.0
  }

  def profileLogAge(creationDate: Long): Double = {
    log(JAN_1_2017 - creationDate)
  }
}
