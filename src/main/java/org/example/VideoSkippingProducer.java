package org.example;//import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class VideoSkippingProducer {
    private final String TOPIC = "video-skip-logs";

    public void sendJumpingLog(Producer<String, String> sharedProducer, String userId, String videoId, String eventType, int lastPos, int currentPos) {
        // JSON 형태로 로그 생성
        String message = String.format(
                "{\"user_id\":\"%s\", \"video_id\":\"%s\", \"eventType\":\"%s\", \"skipStart\":%d, \"currentPos\":%d}",
                userId, videoId, eventType, lastPos, currentPos
        );

        sharedProducer.send(new ProducerRecord<>(TOPIC, videoId, message));
        System.out.println("⏩⏩ Kafka로 skip 로그 전송: " + message);
    }

}
