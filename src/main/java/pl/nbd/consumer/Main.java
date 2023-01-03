package pl.nbd.consumer;


public class Main {
    public static void main(String[] args) throws InterruptedException {
       Topics.createTopic();
        Consumer consumer = new Consumer();
        Consumer.initConsumer();
        consumer.consume(Consumer.getKafkaConsumer());
    }
}