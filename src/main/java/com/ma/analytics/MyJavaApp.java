package com.ma.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConversions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static java.util.Arrays.asList;

public class MyJavaApp {

    public static StructType ratingsSchema = new StructType(new StructField[] {
            new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("movie_Id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("movie_rating", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("voting_timestamp", DataTypes.LongType, false, Metadata.empty()),
    });

    public static StructType moviesSchema = new StructType(new StructField[] {
            new StructField("movie_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("movie_runtime", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("movie_title", DataTypes.StringType, false, Metadata.empty())
    });

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
          .master("local[2]")
          .appName("java-code-challenge")
          .getOrCreate();

        if (args.length < 1) {
            throw new Exception("Missing input date parameters. Please enter a date in the format of yyyyMMdd in order to start the process.");
        }

        //inputDate in the format of yyyyMMdd will be passed as a parameter
        String inputDate = args[0];

        //check to see if input directory exists
        File ratingsDirectory = new File("src/main/resources/ratings/" + inputDate);
        if (!ratingsDirectory.exists()) {
           throw new Exception("Ratings with date : [" + inputDate + "] does not exist.");
        }

        //removes the output directory if it exists - so we can run the job again
        Path outputDirectory = Paths.get("src/main/resources/output/" + inputDate);
        if (Files.exists(outputDirectory)) {
            Files.walk(outputDirectory)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

        }

        Dataset<Row> movieRatings = spark.read()
                          .format("org.apache.spark.csv")
                          .option("header", false)
                          .schema(ratingsSchema)
                          .csv("src/main/resources/ratings/" + inputDate + "/*");

        //only consider a user's latest ratings for a movie
        Dataset<Row> maxUserRatings = movieRatings.groupBy("user_id","movie_id").agg(functions.max("voting_timestamp"));

        //joins maxUserRatings back to movieRatingsWithTimestamp in order to get the ratings column
        Dataset<Row> recentUserRatings = movieRatings.join(maxUserRatings,
                JavaConversions.asScalaBuffer(asList("user_id","movie_Id"))).where(
                movieRatings.col("voting_timestamp").equalTo(maxUserRatings.col("max(voting_timestamp)"))).
                drop("max(voting_timestamp)");

        //averageMovieRatings returns the average ratings and number of votes for a movie
        Dataset<Row> averageMovieRatings = recentUserRatings.groupBy("movie_id").agg(functions.avg("movie_rating").cast(DataTypes.createDecimalType(4,2)).alias("average rating"),functions.count("user_id").alias("number of votes"));

        Dataset<Row> movies = spark.read()
            .format("org.apache.spark.csv")
            .option("header", false)
            .schema(moviesSchema)
            .csv("src/main/resources/movies_metadata/*");

        Dataset<Row> averageMovieRatingsWithTitleAndRuntime = movies.join(averageMovieRatings, JavaConversions.asScalaBuffer(asList("movie_id")),"right_outer");

        averageMovieRatingsWithTitleAndRuntime.repartition(3).write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("delimiter",",")
                .save("src/main/resources/output/" + inputDate);

        spark.stop();
    }
}
