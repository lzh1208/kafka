import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by liuzehui on 2018/9/12.
 */
public class KafkaProducer {
    private static Logger logger = Logger.getLogger(KafkaProducer.class);

    public final Producer<String, String> producer;
    //public final static String TOPIC = properties.getProperty("kafka.return.topic");

    public KafkaProducer(String brokerList){

        Properties props = new Properties();

        // 此处配置的是kafka的broker地址:端口列表
        props.put("metadata.broker.list", brokerList);

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    public void produce(String topic,String key,String data) throws Exception {
        try {
            producer.send(new KeyedMessage<String, String>(topic, key, data));
        }catch (Exception e){
            logger.error("fail to put kafka:\t"+e);
            throw e;
        }

    }
}
