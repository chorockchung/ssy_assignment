package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

public class VideoLastposProducer {
    private final String TOPIC = "video-lastpos-logs";

    public void sendLastposLog(Producer<String, String> sharedProducer, String userId, String videoId, int lastPos) {
        // JSON 형태로 로그 생성
        String message = String.format(
                "{\"user_id\":\"%s\", \"video_id\":\"%s\", \"last_pos\":%d}",
                userId, videoId, lastPos
        );

        sharedProducer.send(new ProducerRecord<>(TOPIC, videoId, message));

        System.out.println("🌐🌐 Kafka로 Lastpos 로그 전송: " + message);
    }

}
