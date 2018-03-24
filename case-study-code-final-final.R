
#### Data preparation
# Initialize Spark
library(SparkR)
sc <- sparkR.session(appName = "reviews")

# First read data from S3
reviews_moviestv <- read.df("s3n://spark-data/reviews_Movies_and_TV_5.json", 
                            source = "json", inferSchema = "true", header = "true")
reviews_cdsvinyl <- read.df("s3n://spark-data/reviews_CDs_and_Vinyl_5.json", 
                            source = "json", inferSchema = "true", header = "true")
reviews_kindle <- read.df("s3n://spark-data/reviews_Kindle_Store_5.json", 
                            source = "json", inferSchema = "true", header = "true")



#### Exploratory Data Analysis

#Important note: There is no one correct answer to this case study. 
# You can compare any metric or groyp of metrics that you deem fit for your business logic.
# First, let's examine the data. Total number of reviews in each of the 3 categories:
nrow(reviews_moviestv)
nrow(reviews_cdsvinyl)
nrow(reviews_kindle)

# To run sql queries, we need to create views out of the dataframes
createOrReplaceTempView(reviews_moviestv, "reviews_moviestv_view")

# Use groupBy to find count of each product
productCount_moviestv <- summarize(groupBy(reviews_moviestv, reviews_moviestv$asin), 
               count = n(reviews_moviestv$asin))
productCount_cdsvinyl <- summarize(groupBy(reviews_cdsvinyl, reviews_cdsvinyl$asin), 
                                   count = n(reviews_cdsvinyl$asin))
productCount_kindle <- summarize(groupBy(reviews_kindle, reviews_kindle$asin), 
                                   count = n(reviews_kindle$asin))


head(productCount_moviestv)
head(productCount_cdsvinyl)
head(productCount_kindle)




# Find products with the most number of reviews.
# We will use a SQL query for this - it gives the best performance, and as data analysts, you are comfortable with SQL

reviews_moviestv_reviewsperproduct <- 
  SparkR::sql("SELECT COUNT(reviewTime) AS reviews_count, asin FROM reviews_moviestv_view 
              GROUP BY asin ORDER BY reviews_count DESC")
take(reviews_moviestv_productcount, 10)

reviews_cdsvinyl_reviewsperproduct <- 
  SparkR::sql("SELECT COUNT(reviewTime) AS reviews_count, asin FROM reviews_cdsvinyl_view 
              GROUP BY asin ORDER BY reviews_count DESC")
take(reviews_cdsvinyl_productcount, 10)

reviews_kindle_reviewsperproduct <- 
  SparkR::sql("SELECT COUNT(reviewTime) AS reviews_count, asin FROM reviews_kindle_view 
              GROUP BY asin ORDER BY reviews_count DESC")
take(reviews_kindle_productcount, 10)


# Instead of this, the "collect" command is also valid

###### Now we'll work with long reviews and helpfulness.
###### Based on the nature of your business problem, you can mix around the order of these analyses

## LONG REVIEWS

# One way to filter for long reviews is as follows:
reviews_moviestv_long <- SparkR::sql("SELECT * FROM reviews_moviestv_view WHERE LENGTH(reviewText) >= 1000")
head(reviews_moviestv_long)
nrow(reviews_moviestv_long)

reviews_cdsvinyl_long <- SparkR::sql("SELECT * FROM reviews_cdsvinyl_view WHERE LENGTH(reviewText) >= 1000")
head(reviews_cdsvinyl_long)
nrow(reviews_cdsvinyl_long)

reviews_moviestv_long <- SparkR::sql("SELECT * FROM reviews_kindle_view WHERE LENGTH(reviewText) >= 1000")
head(reviews_kindle_long)
nrow(reviews_kindle_long)


# Create view of long reviews DF
createOrReplaceTempView(reviews_moviestv_long, "reviewsmoviestv_long_view")

# Also, you could do binning as follows. First, create a variable for legngth called reviewLength
reviews_moviestv_long$reviewLength <- length(reviews_moviestv$reviewText)
createOrReplaceTempView(reviews_moviestv, "reviews_moviestv_view")

# For binning, you can use the ifelse statement as demonstrated below
reviews_moviestv$bin_number <-  ifelse(reviews_moviestv$reviewLength > 0 & reviews_moviestv$reviewLength <= 500 ,1,
                                       ifelse(reviews_moviestv$reviewLength > 501 & reviews_moviestv$reviewLength <= 1000,2,
                                              ifelse(reviews_moviestv$reviewLength > 1001 & reviews_moviestv$reviewLength <= 1500,3,
                                                     ifelse(reviews_moviestv$reviewLength > 1501 & reviews_moviestv$reviewLength <= 2000,4,5))))


take(reviews_moviestv, 10)

# This is an alternative to the binning process. You can also perform it through a SQL query

# First, create view again
createOrReplaceTempView(reviews_moviestv, "reviews_moviestv_view")

# Then, run SQL query on the view and create new object with bin numbers
reviews_moviestv_binned <- 
                  sql("select reviewLength, reviewTime, asin, overall, helpful, \
                   CASE  WHEN reviewLength <= 1000  THEN 1\
                   WHEN (reviewLength > 1000  and reviewLength <= 2000) THEN 2\
                   WHEN (reviewLength > 2000 and reviewLength <= 3000) THEN 3\
                   WHEN (reviewLength > 3000 and reviewLength <= 4000) THEN 4\
                   ELSE 5 END  as bin_number FROM reviews_moviestv_view")




reviews_cdsvinyl_binned <- 
  sql("select reviewLength, reviewTime, asin, overall, helpful, \
                   CASE  WHEN reviewLength <= 1000  THEN 1\
                   WHEN (reviewLength > 1000  and reviewLength <= 2000) THEN 2\
                   WHEN (reviewLength > 2000 and reviewLength <= 3000) THEN 3\
                   WHEN (reviewLength > 3000 and reviewLength <= 4000) THEN 4\
                   ELSE 5 END  as bin_number FROM reviews_moviestv_view")


reviews_kindle_binned <- 
  sql("select reviewLength, reviewTime, asin, overall, helpful, 
      CASE  WHEN reviewLength <= 1000  THEN 1
      WHEN (reviewLength > 1000  and reviewLength <= 2000) THEN 2
      WHEN (reviewLength > 2000 and reviewLength <= 3000) THEN 3
      WHEN (reviewLength > 3000 and reviewLength <= 4000) THEN 4
      ELSE 5 END  as bin_number FROM reviews_kindle_view")



reviews_moviestv_bincount <- sql("SELECT bin_number, COUNT(*) FROM reviews_moviestv GROUP BY bin_number order by bin_number")
head(reviews_moviestv_bincount)

reviews_cdsvinyl_bincount <- sql("SELECT bin_number, COUNT(*) FROM reviews_cdsvinyl GROUP BY bin_number order by bin_number")
head(reviews_moviestv_bincount)

reviews_kindle_bincount <- sql("SELECT bin_number, COUNT(*) FROM reviews_kindle GROUP BY bin_number order by bin_number")
head(reviews_moviestv_bincount)


## HELPFULNESS

# First, threshold out values where helpfulness is less than 10
reviews_moviestv_helpful_threshold <- sql("SELECT *  FROM reviews_moviestv_view WHERE helpful[1] >= 10")
createOrReplaceTempView(reviews_moviestv_helpful_threshold, "reviews_moviestv_helpful_threshold_view")

reviews_cdsvinyl_helpful_threshold <- sql("SELECT *  FROM reviews_cdsvinyl_view WHERE helpful[1] >= 10")
createOrReplaceTempView(reviews_cdsvinyl_helpful_threshold, "reviews_cdsvinyl_helpful_threshold_view")

reviews_kindle_helpful_threshold <- sql("SELECT *  FROM reviews_kindle_view WHERE helpful[1] >= 10")
createOrReplaceTempView(reviews_kindle_helpful_threshold, "reviews_kindle_helpful_threshold_view")

# Now, calculate the helpfulness score as the ratio of helpful[0] and helpful[1]

reviews_moviestv_helpful_scores <- sql("select asin, overall,reviewText, reviewTime,reviewerID, reviewerName,
helpful[0] as helpful_yes, helpful[1] as helpful_total,
length(reviewText) AS reviewLength, helpful[0]/helpful[1] AS helpful_score 
from reviews_moviestv_helpful")

reviews_cdsvinyl_helpful_scores <- sql("select asin, overall,reviewText, reviewTime,reviewerID, reviewerName,
helpful[0] as helpful_yes, helpful[1] as helpful_total,
length(reviewText) AS reviewLength, helpful[0]/helpful[1] AS helpful_score 
from reviews_cdsvinyl_helpful")

reviews_kindle_helpful_scores <- sql("select asin, overall,reviewText, reviewTime,reviewerID, reviewerName,
helpful[0] as helpful_yes, helpful[1] as helpful_total,
length(reviewText) AS reviewLength, helpful[0]/helpful[1] AS helpful_score 
from reviews_kindle_helpful")

# Now, let's find the distribution of average helpfulness over different bins

helpfulness_distribution_moviestv <- 
  SparkR::sql("SELECT bin, AVG(helpful_score) FROM reviews_moviestv_helpful GROUP BY bin ORDER BY 1")


helpfulness_distribution_moviestv <- 
  SparkR::sql("SELECT bin, AVG(helpful_score) FROM reviews_moviestv_helpful GROUP BY bin ORDER BY 1")


helpfulness_distribution_moviestv <- 
  SparkR::sql("SELECT bin, AVG(helpful_score) FROM reviews_moviestv_helpful GROUP BY bin ORDER BY 1")


##### This is the very basic required out of the case study analysis
##### Any extra analyses will be awarded marks based on whether the code runs, and whether the metrics are meaningful.




