
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

val masterRDD = sc.textFile("MasterRDD/").map(_.split(','))
val model = MatrixFactorizationModel.load(sc, "myCollaborativeFilter2")

val numOfRecommendations = 5
val userID = 283421950


val getRecommendations = (userID: Int, n: Int) => {
	model.recommendProducts(userID, n)
}

val recommendations = getRecommendations(userID, numOfRecommendations)


