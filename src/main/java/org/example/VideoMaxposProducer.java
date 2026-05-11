package org.example;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class VideoMaxposProducer {
    private final String TOPIC = "video-Maxpos-logs";

    public void sendMaxposLog(Producer<String, String> sharedProducer, String userId, String videoId, int maxpos) {
        // JSON 형태로 로그 생성
        String message = String.format(
                "{\"user_id\":\"%s\", \"video_id\":\"%s\", \"max_pos\":%d}",
                userId, videoId, maxpos
        );

        sharedProducer.send(new ProducerRecord<>(TOPIC, videoId, message));

        System.out.println("🌐🌐 Kafka로 Maxpos 로그 전송: " + message);
    }

}
