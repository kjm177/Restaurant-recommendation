
// ALS Collaborative Filtering

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

val path:String = "project/data/"

val masterRDD = sc.textFile("MasterRDD/").map(_.split(',') )
//user_id, business_id, restaurant_name, latitude, longitude, postal_code, review_count, restaurant_rating, cuisines, review_id, review_stars, user_name

val ratings = masterRDD.map(line =>Rating(line(1).toInt, line(3).toInt, line(5).toString.toDouble))
//Rating -> user, item, rate

val rank = 10
val numIterations = 10
val model = ALS.train(ratings, rank, numIterations, 0.01)

val usersProducts = ratings.map { case Rating(user_id, business_id, stars) =>
  (user_id, business_id)
}
val predictions =
  model.predict(usersProducts).map { case Rating(user_id, business_id, stars) =>
    ((user_id, business_id), stars)
  }
val ratesAndPreds = ratings.map { case Rating(user_id, business_id, stars) =>
  ((user_id, business_id), stars)
}.join(predictions)
val MSE = ratesAndPreds.map { case ((business_id), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}.mean()
println(s"Mean Squared Error = $MSE")

model.save(sc, "myCollaborativeFilter2")

