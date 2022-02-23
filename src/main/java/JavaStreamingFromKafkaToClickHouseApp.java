import avro.shaded.com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class JavaStreamingFromKafkaToClickHouseApp {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String USER_COOKIE_ID = "user_cookie_id";
    public static final String IMPRESSION = "impression";
    public static final String CLICKED = "clicked";
    public static final String DATE = "date";
    public static final String REFERRER = "referrer";
    public static final String SOURCE = "source";
    public static final String IP = "ip";
    public static final String CONSUMER_GROUP_ID = "group1";

    public static void main(String[] args) throws InterruptedException {
        // Firstly, we'll begin by initializing the JavaStreamingContext which is the entry point for all Spark Streaming applications:
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        System.setProperty("hadoop.home.dir", "C:\\hadoop");


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.setAppName("App");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // Now, we can connect to the Kafka topic from the JavaStreamingContext.

        Collection<String> topics = Arrays.asList("fornrt");

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, getProps()));
        JavaDStream<String> data = messages.map(v -> {
            return v.value();
        });


        Properties ckProperties = new Properties();
        ckProperties.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver");
        ckProperties.setProperty("batchsize", "100000");
        ckProperties.setProperty("socket_timeout", "300000");
        ckProperties.setProperty("numPartitions", "8");
        ckProperties.setProperty("rewriteBatchedStatements", "true");
        String url = "jdbc:clickhouse://localhost:8123/default";


        data.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) {
                JavaRDD<Row> rowRDD = rdd.map(new Function<>() {
                    @Override
                    public Row call(String msg) {
                        JsonParser parser = new JsonParser();
                        JsonObject object = (JsonObject) parser.parse(msg);// response will be the json String
                        Row s = RowFactory.create(
                                !Objects.isNull(object.get(DATE)) ? object.get(DATE).getAsString() : null,
                                !Objects.isNull(object.get(CLICKED)) ? object.get(CLICKED).getAsInt() : null,
                                !Objects.isNull(object.get(USER_COOKIE_ID)) ? object.get(USER_COOKIE_ID).getAsLong() : null,
                                !Objects.isNull(object.get(IMPRESSION)) ? object.get(IMPRESSION).getAsInt() : null,
                                !Objects.isNull(object.get(SOURCE)) ? object.get(SOURCE).getAsString() : null,
                                !Objects.isNull(object.get(REFERRER)) ? object.get(REFERRER).getAsString() : null,
                                !Objects.isNull(object.get(IP)) ? object.get(IP).getAsString() : null
                        );
                        return s;
                    }
                });
                //Create Schema
                StructType schema = DataTypes.createStructType(Lists.newArrayList(
                        DataTypes.createStructField(DATE, DataTypes.StringType, true),
                        DataTypes.createStructField(CLICKED, DataTypes.IntegerType, true),
                        DataTypes.createStructField(USER_COOKIE_ID, DataTypes.LongType, true),
                        DataTypes.createStructField(IMPRESSION, DataTypes.IntegerType, true),
                        DataTypes.createStructField(REFERRER, DataTypes.StringType, true),
                        DataTypes.createStructField(SOURCE, DataTypes.StringType, true),
                        DataTypes.createStructField(IP, DataTypes.StringType, true)
                ));
                //Get Spark session

                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
                msgDataFrame.write().mode(SaveMode.Append).option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), 100000).jdbc(url, "for_nrt", ckProperties);

                if (!Boolean.TRUE.equals(msgDataFrame.isEmpty())) {
                    msgDataFrame.show();
                }
            }
        });
        streamingContext.start();
        streamingContext.awaitTermination();


    }

    private static Map<String, Object> getProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //  Default: latest
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Default: true
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // Default: 500
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); // Default: 5000
        return props;
    }
}
