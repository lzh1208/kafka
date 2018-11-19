import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by liuzehui on 2018/9/13.
 */
public class KafkaConsumer {

    private final ConsumerConnector consumer;
    private KafkaStream<String, String> stream;

    private Map<String, List<KafkaStream<String, String>>> consumerMap;

    public  KafkaConsumer(String zkList,String topic,String consumerGroup){
        Properties props=new Properties();
        //zookeeper
        props.put("zookeeper.connect",zkList);
        //topic
        props.put("group.id",consumerGroup);

        //Zookeeper 超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");


        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config=new ConsumerConfig(props);

        consumer= kafka.consumer.Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
         stream = consumerMap.get(topic).get(0);
    }

    public  KafkaStream<String, String> getStream(){
        return this.stream;
    }



}
