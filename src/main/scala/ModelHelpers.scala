import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

object ModelHelpers {
  val Log: Logger = LoggerFactory.getLogger(Baseline.getClass)

  val NumPartitions = 70

  def randomForestModel(training: RDD[LabeledPoint], sqlc: SQLContext): UnifiedClassifier = {
    import sqlc.implicits._
    val trainingDF = addMetadata(training.toDF())

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol(DataFrameColumns.FEATURES)
      .setOutputCol("indexedFeatures")
      .setMaxCategories(8)
      .fit(trainingDF)

    val dt = new RandomForestClassifier()
      .setLabelCol(DataFrameColumns.LABEL)
      .setFeaturesCol("indexedFeatures")
      .setImpurity("gini")
      .setMaxBins(64)
      .setMaxDepth(27)
      .setMinInstancesPerNode(60)

    // Chain indexers and tree in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(trainingDF)

    new DTClassifier(model)
  }

  // step 8
  def buildPrediction(testData: RDD[((Int, Int), LabeledPoint)],
                      model: UnifiedClassifier,
                      threshold: Double,
                      predictionPath: String,
                      sqlc: SQLContext): Unit = {
    import sqlc.implicits._

    val labeledPoints = testData.map({
      case (userPair, LabeledPoint(label, features)) =>
        (serializeTuple(userPair), label, features)
    })
    val predictedRDD = model.predict[String](
      labeledPoints.toDF(
        DataFrameColumns.KEY,
        DataFrameColumns.LABEL,
        DataFrameColumns.FEATURES)
    ).cache()

    val testPrediction = {
      predictedRDD
        .flatMap { case (pairStr, predictedProbability) =>
          val (user1, user2) = deserializeTuple(pairStr)
          Seq(user1 -> (user2, predictedProbability), user2 -> (user1, predictedProbability))
        }
        .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
        .groupByKey(NumPartitions)
        .map(t => {
          val user = t._1
          val friendsWithRatings = t._2
          val topBestFriends = friendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
          (user, topBestFriends)
        })
        .sortByKey(true, 1)
        .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(predictionPath, classOf[GzipCodec])
  }

  def addMetadata(df: DataFrame): DataFrame = {
    val meta = NominalAttribute
      .defaultAttr
      .withName(DataFrameColumns.LABEL)
      .withValues("0.0", "1.0")
      .toMetadata

    df.withColumn(
      DataFrameColumns.LABEL,
      df.col(DataFrameColumns.LABEL).as(DataFrameColumns.LABEL, meta))
  }

  def serializeTuple(tuple: (Int, Int)): String = {
    tuple._1.toString + "," + tuple._2.toString
  }

  def deserializeTuple(str: String): (Int, Int) = {
    val splits = str.split(',')
    (splits(0).toInt, splits(1).toInt)
  }
}
