package comsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers","hadoop102:9092");
        props.put("group.id","test");

       // props.put("enable.auto.commit","true");//自动提交
        props.put("enable.auto.commit","true");//手动提交

        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("hello3"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //d 表示十进制, s表示字符串, n表示行
                System.out.printf("offset = %d, key = %s, value = %s%n",record.offset(),record.key(),record.value());
            }
            //同步手动提交需要下面的代码, 自动提交则不需要
            //consumer.commitSync();

            //异步手动提交需要以下代码, 自动提交则不需要
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception!=null){
                        System.out.println("Commit failed for"+offsets);
                    }
                }
            });
        }



    }
}
