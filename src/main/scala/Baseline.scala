import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

case class OneWayFriendship(anotherUser: Int, fType: Int)

//describe pair of user with common friend between them
case class MiddleFriendship(user1: Int, user2: Int,
                            middleUserCommonFriendSize: Int,
                            granulatedMergedFType: Int)

case class SquashedFriendship(weighedFLink: Double, fAccumulator: Int)

case class PairWithCommonFriends(person1: Int, person2: Int,
                                 commonFriendsCount: Double,
                                 fAccumulator: Int)

case class UserFriends(user: Int, friends: Array[OneWayFriendship])

case class Profile(create_date: Long, age: Int, sex: Int,
                   country: Long, location: Long, loginRegion: Long)
/**
  * Created by mart on 10.01.17.
  */
object Baseline {
  val Log: Logger = LoggerFactory.getLogger(Baseline.getClass)
  val random = new scala.util.Random

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Baseline")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._

    val dataDir = if (args.length == 1) args(0) else "./"

    val graphPath = dataDir + "trainGraph"
    val reversedGraphPath = dataDir + "trainSubReversedGraph"
    val commonFriendsPath = dataDir + "commonFriendsCountsPartitioned"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "prediction"

    // read graph
    val graph = GraphPreparing.graphPrepare(sc, graphPath)
    GraphPreparing.reversedGraphPrepare(reversedGraphPath, graph, sqlc)
    GraphPreparing.commonFriendsPrepare(commonFriendsPath, reversedGraphPath, sqlc)

    // prepare data for training model
    // step 2
    val commonFriends = sqlc
      .read.parquet(commonFriendsPath + "/part_3/", commonFriendsPath + "/part_13/",
      commonFriendsPath + "/part_33/", commonFriendsPath + "/part_57/",
      commonFriendsPath + "/part_51/", commonFriendsPath + "/part_67/",
      commonFriendsPath + "/part_23/", commonFriendsPath + "/part_45/",
      commonFriendsPath + "/part_10/", commonFriendsPath + "/part_60/")
      .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1),
        t.getAs[Double](2), t.getAs[Int](3))).cache()

    val trainCommonFriendsCounts = commonFriends.filter(pair => pair.person1 % 11 != 7
      && pair.person2 % 11 != 7)

    // step 3
    val usersBC = sc.broadcast(graph.map(userFriends => userFriends.user).collect().toSet)

    val positives = {
      graph
        .flatMap(
          userFriends => userFriends.friends
            .filter(oneWayFriendship => usersBC.value.contains(oneWayFriendship.anotherUser)
              && oneWayFriendship.anotherUser > userFriends.user)
            .map(x => (userFriends.user, x.anotherUser) -> 1.0)
        )
    }

    val ageSexBC = DataPreparingHelpers.prepareAgeSexBroadcast(sc, demographyPath)

    // step 5
    val trainData = {
      DataPreparingHelpers.prepareData(trainCommonFriendsCounts, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }


    // split data into training (70%) and validation (30%)
    // step 6
    val splits = trainData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0)
    val validation = splits(1)

    // run training algorithm to build the model
    val model = ModelHelpers.randomForestModel(training, sqlc)

    val validationWithKey = validation
      .map({
        case LabeledPoint(label, features) => (random.nextLong(), label, features)
      }).cache()
    val predictedRDD = model.predict[Long](
      validationWithKey.toDF(DataFrameColumns.KEY,
        DataFrameColumns.LABEL,
        DataFrameColumns.FEATURES))
    val predictionAndLabels = validationWithKey.map({
      case (key, label, features) => (key, label)
    })
      .join(predictedRDD).map({
      case (key, (label, predictedProbability)) => (predictedProbability, label)
    })

    // estimate model quality
    @transient val metricsLogReg = new BinaryClassificationMetrics(predictionAndLabels, 100)
    val threshold = metricsLogReg.fMeasureByThreshold(2.0).sortBy(-_._2).take(1)(0)._1
    println("Use threshold = " + threshold)

    val rocLogReg = metricsLogReg.areaUnderROC()
    println("model ROC = " + rocLogReg.toString)

    // compute scores on the test set
    // step 7
    val testCommonFriendsCounts = {
      sqlc
        .read.parquet(commonFriendsPath + "/part_*/")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1),
          t.getAs[Double](2), t.getAs[Int](3)))
        .filter(pair => pair.person1 % 11 == 7 || pair.person2 % 11 == 7)
    }

    val testData = {
      DataPreparingHelpers.prepareData(testCommonFriendsCounts, positives, ageSexBC)
        .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
        .filter(t => t._2.label == 0.0)
    }
    ModelHelpers.buildPrediction(testData, model, threshold, predictionPath, sqlc)
  }
}
