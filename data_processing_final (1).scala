import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf


val path:String = "project/data/"

//USER_TABLE
val sqlCtx = new SQLContext(sc)
import sqlCtx._
import sqlCtx.implicits._

val userDF = sqlCtx.jsonFile(path + "yelp_academic_dataset_user.json")
val userDF2 = userDF.where("review_count > 4").select("user_id", "name").withColumnRenamed("name", "user_name")
//val userDF3 = userDF2.select($"*", ($"useful" + $"funny" + $"cool").as("votes"))
//val userDF4 = userDF3.drop("useful").drop("cool").drop("funny")
//val userDF5 = userDF4.select($"*", ($"compliment_hot" + $"compliment_more" + $"compliment_profile" + $"compliment_cute" + $"compliment_list" + $"compliment_note" + $"compliment_plain" + $"compliment_cool" + $"compliment_funny" + $"compliment_writer" + $"compliment_photos").as("compliments"))
//val userDF6 = userDF5.drop("compliment_hot").drop("compliment_more").drop("compliment_profile").drop("compliment_cute").drop("compliment_list").drop("compliment_note").drop("compliment_plain").drop("compliment_cool").drop("compliment_funny").drop("compliment_writer").drop("compliment_photos")
//val userDF7 = userDF6.drop("friends").drop("yelping_since").drop("elite").filter($"user_id".isNotNull)


//REVIEW TABLE

val reviewDF = sqlCtx.jsonFile(path + "yelp_academic_dataset_review.json")
val reviewDF2 = reviewDF.select($"*", ($"useful" + $"funny" + $"cool").as("votes"))
val reviewDF3 = reviewDF2.drop("text").drop("date").filter($"user_id".isNotNull).drop("useful").drop("funny").drop("cool").filter($"business_id".isNotNull).drop("votes").withColumnRenamed("stars", "review_stars")
//val reviewDF4 = reviewDF3.withColumn("num_id",monotonicallyIncreasingId)


// BUISENESS TABLE
val busdf = sqlCtx.jsonFile(path + "yelp_academic_dataset_business.json")

//Convert the string columns to lower case and then filter only Las Vegas (Restaurants, Pubs, Bars or Food places)
val lowered = busdf.withColumn("city", lower(col("city"))).withColumn("categories", lower(col("categories")))
val filtered = lowered.filter((($"categories".contains("restaurant"))||($"categories".contains("food"))||($"categories".contains("pubs"))||($"categories".contains("bars")))&&(!(($"categories".contains("plann"))||($"categories".contains("travel"))))).filter($"city".contains("vegas"))
val valid_bus = filtered.filter($"business_id".isNotNull)

val droppeddf = valid_bus.drop("state").drop("hours").drop("city").drop("attributes")

val flattened = droppeddf.select("business_id", "name", "address", "categories", "latitude", "longitude", "postal_code","review_count", "stars")
// Add address

//Mapping different cuisines from keywords
import org.apache.spark.sql.functions._
val cuisines = udf((categories:String) => {
     categories match{
     case x if categories.contains("italian") => "Italian"
     case x if categories.contains("mexican") || categories.contains("taco") => "Mexican"
     case x if categories.contains("french") => "French"
     case x if categories.contains("burgers") || categories.contains("american") => "American"
     case x if categories.contains("thai") => "Thai"
     case x if categories.contains("chinese") => "Chinese"
     case x if categories.contains("korean") => "Korean"
     case x if categories.contains("indian") => "Indian"
     case x if categories.contains("pizza") => "Pizza"
     case x if categories.contains("nightlife") => "Nightlife"
     case x if categories.contains("middle eastern") => "Middle eastern"
     case x if categories.contains("fast food") => "Fast food"
     case x if categories.contains("tapas") => "Spanish"
     case x if categories.contains("steak") => "Steakhouse"
     case x if categories.contains("buffet") => "Buffet"
     case x if categories.contains("dessert") || categories.contains("donut") || categories.contains("ice cream") => "Desserts"
     case x if categories.contains("baker") => "Bakery"
     case x if categories.contains("japanese") || categories.contains("sushi") => "Japanese"
     case x if categories.contains("asian") => "Asian"
     case x if categories.contains("cafe") || categories.contains("coffee") || categories.contains("sandwich")  => "Cafe"
     case _ => "Others"
     }
	 }
     )

val replaceCommas = udf((input: String) => {
	input.replace(',', ';')
})


val hash = udf((input: String) => {
     input.hashCode()
})

val cuisines_sorted = flattened.withColumn("cuisines", cuisines(flattened("categories"))).drop("categories")     
cuisines_sorted.dtypes

println("Initial User count: " + userDF.count())             // 1637138
println("Final User count: " + userDF2.count())              // 861695
println("Initial Review count: " + reviewDF.count())         // 6685900
println("Final Review count: " + reviewDF3.count())          // 6685900
println("Initial Business count: " + busdf.count())          // 6685900
println("Final Business count: " + cuisines_sorted.count())  // 6685900


val businessDF = cuisines_sorted.withColumnRenamed("name", "restaurant_name").withColumnRenamed("stars", "restaurant_rating")
val businessDF2 = businessDF.withColumn("restaurant_name", replaceCommas(businessDF("restaurant_name")))
val businessDF3 = businessDF2.withColumn("address", replaceCommas(businessDF2("address")))
val businessDF4 = businessDF3.withColumn("restaurant_id_hash", hash(businessDF3("business_id")))


val userDF3 = userDF2.withColumn("user_name", replaceCommas(userDF2("user_name")))
val userDF4 = userDF3.withColumn("user_id_hash", hash(userDF3("user_id")))


val businessAndReview = businessDF4.join(reviewDF3, Seq("business_id"),"inner")

val masterDF = businessAndReview.join(userDF4, Seq("user_id") ,"inner")


// val masterDF2 = masterDF.withColumn("user_id_hash", hash(masterDF("user_id")))
// val masterDF3 = masterDF2.withColumn("restaurant_id_hash", hash(masterDF("business_id")))

val masterDF2 = masterDF.select("user_id", "user_id_hash", "business_id", "restaurant_id_hash", "review_id", "review_stars", "longitude", "latitude", "postal_code", "review_count", "restaurant_rating", "cuisines", "user_name", "restaurant_name", "address")


//val reviewRDD = reviewDF3.rdd
//val userRDD = userDF2.rdd
//val businessRDD = businessDF.rdd

//reviewRDD.saveAsTextFile("ReviewRDD/")
//userRDD.saveAsTextFile("UserRDD/")
//businessRDDRDD.saveAsTextFile("RDD/")


masterDF2.rdd.saveAsTextFile("MasterRDD")
businessDF4.rdd.saveAsTextFile("RestaurantRDD")

