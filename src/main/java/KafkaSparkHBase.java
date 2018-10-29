import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.spark.streaming.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import scala.Tuple2;

public class KafkaSparkHBase {
    public static void main(String[] args) {
        // Try to read properties from file
        Properties propConfig = new Properties();
        try {
            propConfig.load(new FileReader("config.properties"));
        } catch (FileNotFoundException e) {
            System.out.println("Config properties file not found, Use default properties");
            propConfig.put("spark.app.name", "test-app");
            propConfig.put("spark.master", "yarn");
            propConfig.put("kafka.group.id", "test-group");
            propConfig.put("kafka.bootstrap.servers", "aio:6667");
            propConfig.put("kafka.topic", "test-topic");
            propConfig.put("hbase.table", "test-table");
            propConfig.put("hbase.zookeeper.quorum", "localhost:2181");
            propConfig.put("hbase.rootdir", "file:///home/testuser/hbase");
            propConfig.put("hbase.columnfamily", "word-count");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Spark settings
        SparkConf sparkConf = new SparkConf().setAppName(propConfig.getProperty("spark.app.name")).setMaster(propConfig.getProperty("spark.master"));
        // Create spark streaming context with 5 second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        JavaSparkContext jsc = jssc.sparkContext();
        // Set the logging level to reduce log message spam
        jsc.setLogLevel("ERROR");
        /*=================================================================================*/
        // Kafka settings
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", propConfig.get("kafka.bootstrap.servers"));
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", propConfig.get("kafka.group.id"));
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(propConfig.get("kafka.topic").toString());
        /*=================================================================================*/
        // create DStream
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
        // Do the word count process
        Pattern SPACE = Pattern.compile(" ");
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        /*=================================================================================*/
        // HBase settings
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableOutputFormat.OUTPUT_TABLE, propConfig.get("hbase.table").toString());
        conf.set("hbase.zookeeper.quorum", propConfig.get("hbase.zookeeper.quorum").toString());
        conf.set("hbase.rootdir", propConfig.getProperty("hbase.rootdir"));

//        Job newAPIJobConfiguration = null;
//        try {
//            newAPIJobConfiguration = Job.getInstance(conf);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "tableName");
//        newAPIJobConfiguration.setOutputKeyClass(ImmutableBytesWritable.class);
//        newAPIJobConfiguration.setOutputValueClass(Result.class);
//        newAPIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);

        Configuration jobConf = new Configuration(conf);
        jobConf.set("mapreduce.job.outputkey.class", ImmutableBytesWritable.class.getName());
        jobConf.set("mapreduce.job.outputvalue.class", Result.class.getName());
        jobConf.set("mapreduce.job.outputformat.class", TableOutputFormat.class.getName());
        /*=================================================================================*/
        // Save result to HBase
        wordCounts.foreachRDD( pairRDD -> {
            pairRDD.mapToPair( tuple -> {
                long rowKey = new Date().getTime();
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(propConfig.get("hbase.columnfamily").toString()), Bytes.toBytes(tuple._1), Bytes.toBytes(tuple._2));
                return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
            }).saveAsNewAPIHadoopDataset(jobConf); //newAPIJobConfiguration.getConfiguration()
        });
        /*=================================================================================*/
        // start the streaming context
        jssc.start();
        try {
            jssc.awaitTermination(); // block while the context is running
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}