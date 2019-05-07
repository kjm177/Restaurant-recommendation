import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

val masterRDD = sc.textFile("MasterRDD/").map(l => l.substring(1,l.length-1)).map(_.split(','))

val restaurantRDD = sc.textFile("RestaurantRDD/").map(l => l.substring(1,l.length-1)).map(_.split(','))

val model = MatrixFactorizationModel.load(sc, "myCollaborativeFilter")

val searchQuery_df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").load("searchList.csv")
val userID = searchQuery_df.first()(0).toString.toInt
val zip = searchQuery_df.first()(1).toString.toInt


val numOfRecommendations = 10

val getRecommendations = (userID: Int, n: Int) => {
	model.recommendProducts(userID, n)
}

val recommendations = getRecommendations(userID, numOfRecommendations)

case class reviews(user_id: String, user_id_hash: Int, business_id: String, restaurant_id_hash: Int, review_id: String, review_stars: Float, longitude: Double, latitude: Double, postal_code: String, review_count: Long, restaurant_rating: Double, cuisines: String, user_name: String, restaurant_name: String, address: String)
val masterDF = masterRDD.map{case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14) => reviews(s0, s1.toInt, s2, s3.toInt, s4, s5.toFloat, s6.toDouble, s7.toDouble, s8, s9.toLong, s10.toDouble, s11, s12, s13, s14)}.toDF()

case class restaurants(business_id: String, restaurant_name: String, address: String, latitude: Double, longitude: Double, postal_code: String, review_count: Long,  restaurant_rating: Double, cuisines: String, restaurant_id_hash: Int)
val restaurantDF = restaurantRDD.map{case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9) => restaurants(s0, s1, s2, s3.toDouble, s4.toDouble, s5, s6.toLong, s7.toDouble, s8, s9.toInt)}.toDF()

// Recommendations from Collaborative Filtering
val RDD1 = sc.parallelize(recommendations)
val RDD2 = RDD1.map(l => (l.product, l.rating))
case class recommends(restaurant_id_hash: Int, rating: Double)
val recommendDF = RDD2.map{case (s0, s1) => recommends(s0, s1)}.toDF()
val recommendDF2 = recommendDF.join(restaurantDF, "restaurant_id_hash")
recommendDF2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("r1")

val toprated_zipDF = masterDF.filter($"postal_code" === zip && $"review_count" >= 10).groupBy("restaurant_id_hash").agg(avg("review_stars")).toDF("restaurant_id_hash", "avg_rating") 
val toprated_zipDF2 = toprated_zipDF.sort(toprated_zipDF("avg_rating").desc).limit(numOfRecommendations)
val toprated_zipDF3 = toprated_zipDF2.join(restaurantDF, "restaurant_id_hash")
// toprated_zipDF3.rdd.saveAsTextFile("toprated")


val cuisine_ratingDF = masterDF.filter($"postal_code" === zip).groupBy("cuisines").agg(avg("review_stars")).toDF("cuisine", "avg_rating")
// val top_rated_cuisine = cuisine_ratingDF.sort(cuisine_ratingDF("avg_rating").desc).take(1)
// val topcuisine:String = top_rated_cuisine(0)(0).toString
// val cuisine_ratingDF2 = masterDF.filter($"cuisines" ===  topcuisine && $"review_count" >= 10).groupBy("restaurant_id_hash").agg(avg("review_stars")).toDF("restaurant_id_hash", "avg_rating") 
// val cuisine_ratingDF3 = cuisine_ratingDF2.sort(cuisine_ratingDF2("avg_rating").desc).limit(numOfRecommendations)
// val cuisine_ratingDF4 = cuisine_ratingDF3.join(restaurantDF, "restaurant_id_hash")
// cuisine_ratingDF4.rdd.saveAsTextFile("cuisinerated")
