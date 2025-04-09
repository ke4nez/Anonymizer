package CDN;

import CDN.Anonymizer.KafkaListener;

public class Main {
    public static void main(String[] args) {
        String kafkaTopicURL = System.getenv("BOOTSTRAPSERVERS");
        String kafkaTopicName = System.getenv("TOPIC");
        KafkaListener listener = new KafkaListener(kafkaTopicURL, "http_log");
        listener.listen();
    }
}

