package pl.nbd.consumer;

import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import pl.nbd.consumer.db.AbstractMongoRepository;
import pl.nbd.consumer.db.Record;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Consumer extends AbstractMongoRepository {

    public static KafkaConsumer<UUID, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

    private static KafkaConsumer<UUID, String> kafkaConsumer;

    public static List<KafkaConsumer<UUID, String>> getConsumerGroup() {
        return consumerGroup;
    }

    private static List<KafkaConsumer<UUID, String>> consumerGroup;

    private static MongoCollection<Record> recordCollection;

    public void initConsumer() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, Topics.CONSUMER_GROUP_NAME);
        //consumerConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "clientconsumer");
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9192, kafka2:9292, kafka3:9392");

        super.initDbConnection();
        recordCollection = mongoDatabase.getCollection("consumerRecords", Record.class);
        recordCollection.drop();
        kafkaConsumer = new KafkaConsumer<UUID, String>(consumerConfig);
        kafkaConsumer.subscribe(List.of(Topics.CLIENT_TOPIC));
    }

    public static void initConsumerGroup() {
        consumerGroup = new ArrayList<>();

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, Topics.CONSUMER_GROUP_NAME);//dynamiczny przydział
        consumerConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "clientconsumer");//statyczny przydział
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9192,kafka2:9292,kafka3:9392");

        for (int i = 0; i < 2; i++) {
            KafkaConsumer<UUID, String> kafkaConsumer = new KafkaConsumer(consumerConfig);
            kafkaConsumer.subscribe(List.of(Topics.CLIENT_TOPIC));
            consumerGroup.add(kafkaConsumer);
        }
    }


    private void deleteConsumerGroup() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9192,kafka2:9292,kafka3:9392");
        try (Admin admin = Admin.create(properties)) {
            DescribeConsumerGroupsResult describeConsumerGroupsResult = admin.describeConsumerGroups(List.of(Topics.CONSUMER_GROUP_NAME));
            Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups = describeConsumerGroupsResult.describedGroups();
            for (Future<ConsumerGroupDescription> group : describedGroups.values()) {
                ConsumerGroupDescription consumerGroupDescription = group.get();
                System.out.println(consumerGroupDescription);
            }
            admin.deleteConsumerGroups(List.of(Topics.CONSUMER_GROUP_NAME));
        }
    }



    public void consume(KafkaConsumer<UUID, String> consumer) {
        try {
            consumer.poll(0);
            Set<TopicPartition> consumerAssignment = consumer.assignment();
            System.out.println(consumer.groupMetadata().memberId() + " " + consumerAssignment);
            consumer.seekToBeginning(consumerAssignment);

            Duration timeout = Duration.of(100, ChronoUnit.MILLIS);
            MessageFormat formatter = new MessageFormat("Konsument {5},Temat {0}, partycja {1}, offset {2, number, integer}, klucz {3}, wartość {4}");
            while (true) {
                ConsumerRecords<UUID, String> records = consumer.poll(timeout);
                for (ConsumerRecord<UUID, String> record : records) {
                    String result = formatter.format(new Object[]{
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value(),
                            consumer.groupMetadata().memberId()
                    });
                    System.out.println(result);
                    Record recordMongo = new Record(UUID.randomUUID(),
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key().toString(),
                            record.value(),
                            consumer.groupMetadata().toString());
                    recordCollection.insertOne(recordMongo);
                }
            }
        } catch (WakeupException we) {
            System.out.println("Job Finished");
        }
    }

    @Override
    public void close() throws Exception {

    }
}
