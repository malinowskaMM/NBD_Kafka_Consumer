package pl.nbd.consumer.db;

import lombok.Getter;
import lombok.Setter;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.UUID;

@Getter
@Setter
public class Record  implements Serializable {

    @BsonId
    UUID id;

    @BsonProperty("topic")
    String topic;

    @BsonProperty("partition")
    int partition;

    @BsonProperty("offset")
    long offset;

    @BsonProperty("key")
    String key;

    @BsonProperty("value")
    String value;

    @BsonProperty("consumer")
    String consumer;



    @BsonCreator
    public Record(@BsonId UUID id,
                  @BsonProperty("topic")String topic,
                  @BsonProperty("partition")int partition,
                  @BsonProperty("offset")long offset,
                  @BsonProperty("key")String key,
                  @BsonProperty("value")String value,
                  @BsonProperty("consumer")String consumer) {
        this.id = id;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.consumer = consumer;
    }
}
