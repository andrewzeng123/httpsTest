import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyCounterInterceptor implements ProducerInterceptor {
    private int errorCounter=0;
    private int successCounter=0;
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord;//原样返回
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e!=null){
            errorCounter++;
        }else {
            successCounter++;
        }
    }

    @Override
    public void close() {
        System.out.println("successful sent:"+successCounter);
        System.out.println("failed sent:"+errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
