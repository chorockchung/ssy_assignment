package org.example;//import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class VideoRewatchProducer {
    private final String TOPIC = "video-rewatch-logs";


    public void sendRewatchLog(Producer<String, String> sharedProducer, String userId, String videoId, String eventType, int currentPos, int lastPos) {
        // JSON 형태로 로그 생성
        String message = String.format(
                "{\"user_id\":\"%s\", \"video_id\":\"%s\", \"eventType\":\"%s\", \"rewatchPos\":%d, \"PrevPos\":%d}",
                userId, videoId, eventType, currentPos, lastPos
        );

        sharedProducer.send(new ProducerRecord<>(TOPIC, videoId, message));
        System.out.println("⏪⏪ Kafka로 rewatch 로그 전송: " + message);
    }

}
