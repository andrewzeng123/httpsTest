import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop102:9092");
        props.put("acks","all");
        props.put("retries","1");
        props.put("batch.size",16384);
        props.put("linger.ms",100);
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //添加拦截器链
        List<String> interceptors=new ArrayList<>();
        interceptors.add("MyTimeInterceptor");
        interceptors.add("MyCounterInterceptor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            //异步发送
           // producer.send(new ProducerRecord<String, String>("first",null,null,"andrew: "+Integer.toString(i)));
            //同步发送
            //producer.send(new ProducerRecord<String, String>("first",null,null,"andrew: "+Integer.toString(i))).get();

            //异步+回调函数
            producer.send(new ProducerRecord<String, String>("hello3", null, Integer.toString(i), "andrew: " + Integer.toString(i)), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e==null){
                        System.out.println(metadata.partition()+"--"+metadata.offset());
                    }else{
                        e.printStackTrace();
                    }
                }
            });

        }
        producer.close();

    }
}
