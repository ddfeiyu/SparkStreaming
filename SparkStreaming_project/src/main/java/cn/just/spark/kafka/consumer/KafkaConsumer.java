package cn.just.spark.kafka.consumer;

import cn.just.spark.kafka.producer.KafkaProperties;
import cn.just.spark.utils.JsonUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import javax.swing.text.DateFormatter;
import java.util.*;

public class KafkaConsumer extends Thread{

    public String topic;
    public KafkaConsumer(String topic){
        this.topic=topic;
    }

    public ConsumerConnector getConnection(){

        Properties properties=new Properties();
        properties.put("group.id", KafkaProperties.GROUP_ID);
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }


    @Override
    public void run() {
        ConsumerConnector consumer=getConnection();

        Map<String, Integer> topicMap=new HashMap<String, Integer>();
        topicMap.put(topic,1);     //从一个KafkaStream消费数据

        //String:topic
        //List<KafkaStream<byte[], byte[]>>对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream= consumer.createMessageStreams(topicMap);

        //获取我们每次接收到的数据
        KafkaStream<byte[], byte[]> stream=messageStream.get(topic).get(0);        //get(0)对应于上面的一个KafkaStream

        ConsumerIterator<byte[], byte[]> it=stream.iterator();
        while(it.hasNext()){
            String message=new String(it.next().message());
            System.out.println("resever message: "+message);
        }


    }

    public static void main(String[] args) {


        String msgConfigTemplate = "[\n" +
                "    {\n" +
                "        \"date\":\"{today} 10:30\",\n" +
                "        \"msg\":\"【参访】今日11点有重要参访团队，请知悉。\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"date\":\"{yesterday} 12:00\",\n" +
                "        \"msg\":\"【服务】机器人已将您的餐饮已送达，请查收。\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"date\":\"{yesterday} 18:30\",\n" +
                "        \"msg\":\"【会议】您的会议即将在15分钟后召开。\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"date\":\"\",\n" +
                "        \"msg\":\"【管理】园区今日告警已处理完毕。\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"date\":\"{beforeYesterday} 12:30\",\n" +
                "        \"msg\":\"【服务】您的餐饮已送达，请及时查收。\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"date\":\"{beforeYesterday} 12:00\",\n" +
                "        \"msg\":\"【服务】机器人已将您的餐饮已送达，请查收。\"\n" +
                "    }\n" +
                "]";

        List<LinkedHashMap> msgConfigs = JsonUtil.jsonToList(msgConfigTemplate, LinkedHashMap.class);

        Date date = new Date();
        String today = "2022-06-11";
        String yesterday = "2022-06-10";
        String beforeYesterday = "2022-06-09";

        msgConfigs.forEach(config -> {
            String dt = String.valueOf(config.get("date"));
            String msg = String.valueOf(config.get("msg"));
            dt = dt.replace("{today}", today).
                    replace("{yesterday}", yesterday).
                    replace("{beforeYesterday}", beforeYesterday);

            System.out.println("dt: "+dt+" ,msg : "+msg);

        });
    }
}

