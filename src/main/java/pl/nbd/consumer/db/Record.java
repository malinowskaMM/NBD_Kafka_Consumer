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

    @BsonProperty("record")
    String record;


    @BsonCreator
    public Record(@BsonId UUID id, @BsonProperty("record") String record) {
        this.id = id;
        this.record = record;
    }
}
