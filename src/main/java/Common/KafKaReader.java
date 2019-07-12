package Common;

import Config.ConfigurationImpl;
import com.alibaba.fastjson.JSONObject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.*;
import org.apache.log4j.Logger;
import utils.DateTime;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class KafKaReader implements Serializable {

    /**
     * KafKa的Topic
     */
    private String topic;

    /**
     * kafka的配置属性
     */
    private Map<String, Object> kafkaParams;

    /**
     * 配置文件
     */
    private ConfigurationImpl config;

    /**
     * 日志输出信息
     */
    private static Logger logger = Logger.getLogger(KafKaReader.class);


    /**
     * 构造函数
     */
    public KafKaReader(){
        this.config = ConfigurationImpl.getInstance();
        this.config.init();
        this.kafkaParams = createConsumerConfig(config.getBrokerList(), config.getGroupId());
        this.topic = config.getTopic();

    }

    /**
     * 配置KafKa Consumer
     */
    private static Map<String, Object> createConsumerConfig(String brokers, String groupId) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("fetch.min.bytes", "8192");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return kafkaParams;
    }


    /**
     * 解析kafka日志，过滤出拼多多的点击和曝光日志
     */
    public void process(){
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topic.split(",")));
        SparkConf sparkConf = new SparkConf();
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.minutes(60));
        JavaInputDStream<ConsumerRecord<String, byte[]>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topicsSet, kafkaParams)
                );
        JavaDStream<byte[]> lines = stream.map(new Function<ConsumerRecord<String, byte[]>, byte[]>() {
            @Override
            public byte[] call(ConsumerRecord<String, byte[]> record){
                return record.value();
            }
        });
        JavaDStream<byte[]> clickOrExposure = lines.filter(new Function<byte[], Boolean>() {
            @Override
            public Boolean call(byte[] record) throws Exception {
                String data = new String(record);
                JSONObject jsonObject = JSONObject.parseObject(data);
                boolean result = false;
                String event = jsonObject.getString("event");
                JSONObject activity = jsonObject.getJSONObject("activity");
                if (activity != null) {
                    String activityId = activity.getString("activity_id");
                    if (activityId.equals("pinduoduo_list") || activityId.equals("pinduoduo_list_b")){
                        if (event.equals("51")) {
                            result = true;
                        }
                        else if (event.equals("24") && jsonObject.getString("click_element").equals("213")) {
                            result = true;
                        }
                    }
                }
                return result;
            }
        });
        JavaDStream<String> result = clickOrExposure.map(new Function<byte[], String>() {
            @Override
            public String call(byte[] line) throws Exception {
                return new String(line);
            }
        });
        result.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                Path hdfsDir;
                DateTime dateTime = new DateTime();
                hdfsDir = Paths.get("gaochen","PddGoodsClickExposure", dateTime.getNowDate(), String.valueOf(dateTime.getDateHour()));
                rdd.saveAsTextFile(hdfsDir.toString());
            }
        });
        jsc.start();
        try {
            jsc.awaitTermination();
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    public static void main(String[] args){
        KafKaReader kafKaReader = new KafKaReader();
        kafKaReader.process();
    }

}
