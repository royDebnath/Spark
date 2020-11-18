/**
 * Starting master node:
 * <p>
 * C:\Work\Softwares\spark\spark-2.4.7-bin-hadoop2.7\bin>spark-class org.apache.spark.deploy.master.Master
 * <p>
 * Master: Starting Spark master at spark://172.25.160.1:7077
 * <p>
 * Starting Worker node:
 * <p>
 * C:\Work\Softwares\spark\spark-2.4.7-bin-hadoop2.7\bin>spark-class org.apache.spark.deploy.worker.Worker spark://172.25.160.1:7077
 * <p>
 * <p>
 * Opening master node shell:
 * <p>
 * C:\Work\Softwares\spark\spark-2.4.7-bin-hadoop2.7\bin>spark-shell --master spark://172.25.160.1:7077
 */

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import spire.random.rng.Serial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkDatasetOperations implements Serializable {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
/*

        */
/**Creating Spark Context**//*

        JavaSparkContext sparkContext = createJavaSparkContext();

        */
/**=========================================================================Java RDDs=========================================================================*//*

        */
/**Reducing*//*

        reduceExample(buildDoublesDataset(), sparkContext);

        */
/**Mapping*//*

        JavaRDD<Double> squareRoots = mapExample(buildIntegerDataset(), sparkContext);

        */
/**Write outputs*//*

        writingOutputs(squareRoots);

        */
/**Java Pair RDD*//*

        JavaPairRDD<String, Long> pairRDD = createPairRdd(sparkContext);

        */
/**Reduce by key*//*

        reducingByKey(sparkContext, pairRDD);

        */
/**Group By Key*//*

        groupingByKey(sparkContext);

        */
/**Flat Mapping
         * map converts one element to one element
         * flatmap converts one element to 0 or multiple element hence returns an iterator
         * *//*

        JavaRDD<String> words = flatMapping(sparkContext);

        */
/**Filtering*//*

        filtering(words);

        */
/**Reading from file*//*

        readingFromFile(sparkContext);
*/

        /**=========================================================================Spark Datasets=========================================================================*/

        SparkSession sparkSession = createSession();

        /**Reading from file to dataset**/
        Dataset<Row> dataset = sparkSession.read().format("csv").option("header", "true").load("C:\\Work\\Coding\\support\\students.csv");
        dataset.show();

        /**Dataset count**/
        System.out.println("dataset count : " + dataset.count());


        /**Dataset operations**/

        System.out.println("dataset first row : " + dataset.first().toString());

        System.out.println("dataset column projection by fieldname : " + dataset.first().getAs("first_name").toString());

        System.out.println("dataset column projection by column number : " + dataset.first().getAs(2).toString());

        dataset.filter("ethnicity = 'Hispanic' AND gender='M'").show();

        dataset.filter(row -> row.getAs("status").equals("TRANSFER") && row.getAs("ethnicity").equals("Asian")).show();

        dataset.filter(col("entry_academic_period").equalTo("Fall 2008").and(col("gender").equalTo("M"))).show();

        dataset.createOrReplaceTempView("student_table");
        sparkSession.sql(" select ethnicity, count(*) from student_table group by ethnicity").show();

        /**Writing to parquet**/
        dataset.write().parquet("C:\\Work\\Coding\\support\\students.parquet");


    }

    private static SparkSession createSession() {
        return SparkSession
                    .builder()
                    //.master("spark://172.25.160.1:7077")
                    .appName("Java Spark SQL Example")
                    .config("spark.master", "local")
                    .getOrCreate();
    }

    private static void readingFromFile(JavaSparkContext sparkContext) {
        JavaRDD<String> fileInputRdd = sparkContext.textFile("C:\\Work\\Coding\\support\\sample.txt");
        fileInputRdd.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator()).collect().forEach(System.out::println);
        sparkContext.close();
    }

    private static void filtering(JavaRDD<String> words) {
        System.out.println("===============Filtering only the numbers=========================");
        words.filter(word -> NumberUtils.isCreatable(word)).collect().forEach(System.out::println);
    }

    private static JavaRDD<String> flatMapping(JavaSparkContext sparkContext) {
        System.out.println("===============Flatmap execution=========================");
        JavaRDD<String> words = sparkContext.parallelize(buildSampleDataSet()).flatMap(sentence -> Arrays.asList(sentence.split(":")).iterator());
        words.collect().forEach(System.out::println);
        return words;
    }

    private static void groupingByKey(JavaSparkContext sparkContext) {
        sparkContext.parallelize(buildSampleDataSet())
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[1], 1L))
                .groupByKey()
                .foreach(row -> System.out.println("GBK: Number of movies in Genre " + row._1 + " is : " + Iterables.size(row._2))); // Iterables is a google api to operate on Iterable variables
    }

    private static void reducingByKey(JavaSparkContext sparkContext, JavaPairRDD<String, Long> pairRDD) {
        JavaPairRDD<String, Long> movieByGenre = pairRDD.reduceByKey((val1, val2) -> val1 + val2);
        movieByGenre.foreach(row -> System.out.println("Number of movies in Genre " + row._1 + " is : " + row._2));
        //Optimized Lines of Code
        sparkContext.parallelize(buildSampleDataSet())
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[1], 1L))
                .reduceByKey((val1, val2) -> val1 + val2)
                .foreach(row -> System.out.println("RBK : Number of movies in Genre " + row._1 + " is : " + row._2));
    }

    private static JavaPairRDD<String, Long> createPairRdd(JavaSparkContext sparkContext) {
        JavaRDD<String> inputRDD = sparkContext.parallelize(buildSampleDataSet());
        return inputRDD.mapToPair(rawValue -> {
            String[] columns = rawValue.split(":");
            String genre = columns[1];
            return new Tuple2<>(genre, 1L);
        });
    }

    private static List<String> buildSampleDataSet() {
        List<String> inputDataSet = new ArrayList<>();
        inputDataSet.add("1:Comedy:Toy Story (1995):01-Jan-1995");
        inputDataSet.add("2:Thriller:GoldenEye (1995):01-Jan-1995");
        inputDataSet.add("3:Thriller:Four Rooms (1995):01-Jan-1995");
        inputDataSet.add("4:Comedy:Get Shorty (1995):01-Jan-1995");
        inputDataSet.add("5:Comedy:Copycat (1995):01-Jan-1995");
        return inputDataSet;
    }

    private static JavaSparkContext createJavaSparkContext() {
        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        return new JavaSparkContext(sparkConf);
    }

    private static void writingOutputs(JavaRDD<Double> squareRoots) {
        // squareRoots.foreach(System.out::println); This gives not serializable exception for multi core cpus
        squareRoots.collect().forEach(System.out::println);
        System.out.println("Count from count function : " + squareRoots.count());
        System.out.println("Map Reduce Count : " + squareRoots.map(value -> 1L).reduce((val1, val2) -> val1 + val2));
    }

    private static JavaRDD<Double> mapExample(List<Integer> inputDataSet, JavaSparkContext sparkContext) {
        JavaRDD<Integer> inputRDD1 = sparkContext.parallelize(inputDataSet);
        return inputRDD1.map(value -> Math.sqrt(value));
    }

    private static List<Integer> buildIntegerDataset() {
        List<Integer> inputDataSet = new ArrayList<>();
        inputDataSet.add(125);
        inputDataSet.add(138);
        inputDataSet.add(200);
        inputDataSet.add(3252);
        inputDataSet.add(1912);
        inputDataSet.add(723);
        return inputDataSet;
    }

    private static void reduceExample(List<Double> inputDataSet, JavaSparkContext sparkContext) {
        JavaRDD<Double> inputRDD = sparkContext.parallelize(inputDataSet);
        Double sum = inputRDD.reduce((value1, value2) -> value1 + value2);
        System.out.println("Reduced Sum : " + sum);
    }

    private static List<Double> buildDoublesDataset() {
        List<Double> inputDataSet = new ArrayList<>();
        inputDataSet.add(12.5);
        inputDataSet.add(13.8);
        inputDataSet.add(20.0);
        inputDataSet.add(328.52);
        inputDataSet.add(19.12);
        inputDataSet.add(72.3);
        return inputDataSet;
    }
}
