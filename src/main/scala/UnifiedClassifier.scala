import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by mart on 11.01.17.
  */
trait UnifiedClassifier {
  def predict[T](data: DataFrame): RDD[(T, Double)]
}
