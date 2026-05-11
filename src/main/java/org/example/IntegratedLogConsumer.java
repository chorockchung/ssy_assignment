package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class IntegratedLogConsumer {
    public static final String DB_URL = "jdbc:mysql://db:3306/kafka_log_db";
    public static final String DB_USER = "dev_user";       // init.sql에 쓴 유저
    public static final String DB_PW = "dev_pass123!";    // init.sql에 쓴 비번


    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "db:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "video-analysis-group-v2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("video-lastpos-logs", "video-rewatch-logs", "video-skip-logs", "video-Maxpos-logs"));

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PW)) {
            System.out.println("✅ MySQL 연결 성공! 로그 데이터 적재를 시작합니다...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String topic = record.topic();
                        JsonNode json = objectMapper.readTree(record.value());

                        // 토픽별로 매칭되는 메서드 실행
                        if (topic.equals("video-lastpos-logs")) {
                            insertLastPos(conn, json);
                        } else if (topic.equals("video-rewatch-logs")) {
                            insertRewatch(conn, json);
                        } else if (topic.equals("video-skip-logs")) {
                            insertSkipping(conn, json);
                        } else {
                            insertMaxpos(conn, json);
                        }
                    } catch (Exception e) {
                        System.err.println("❌ 데이터 처리 중 오류 발생: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 1. Lastpos 매칭: user_id, video_id, last_pos
    private static void insertLastPos(Connection conn, JsonNode json) throws Exception {
        String sql = "INSERT INTO video_lastpos_logs (user_id, video_id, last_pos) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, json.get("user_id").asText());
            pstmt.setString(2, json.get("video_id").asText());
            pstmt.setInt(3, json.get("last_pos").asInt());
            pstmt.executeUpdate();
        }
    }

    // 2. Rewatch 매칭: user_id, video_id, eventType, rewatchPos, PrevPos
    private static void insertRewatch(Connection conn, JsonNode json) throws Exception {
        String sql = "INSERT INTO video_rewatch_logs (user_id, video_id, event_type, prev_pos, current_pos) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, json.get("user_id").asText());
            pstmt.setString(2, json.get("video_id").asText());
            pstmt.setString(3, json.get("eventType").asText()); // Producer 명칭: eventType
            pstmt.setInt(4, json.get("PrevPos").asInt());       // Producer 명칭: PrevPos
            pstmt.setInt(5, json.get("rewatchPos").asInt());    // Producer 명칭: rewatchPos
            pstmt.executeUpdate();
        }
    }

    // 3. Skipping 매칭: user_id, video_id, eventType, skipStart, currentPos
    private static void insertSkipping(Connection conn, JsonNode json) throws Exception {
        String sql = "INSERT INTO video_skipping_logs (user_id, video_id, event_type, prev_pos, current_pos) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, json.get("user_id").asText());
            pstmt.setString(2, json.get("video_id").asText());
            pstmt.setString(3, json.get("eventType").asText()); // Producer 명칭: eventType
            pstmt.setInt(4, json.get("skipStart").asInt());     // Producer 명칭: skipStart
            pstmt.setInt(5, json.get("currentPos").asInt());    // Producer 명칭: currentPos
            pstmt.executeUpdate();
        }
    }

    private static void insertMaxpos(Connection conn, JsonNode json) throws Exception {
        String sql = "INSERT INTO video_lastpos_logs (user_id, video_id, max_pos) " +
                "VALUES (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "max_pos = IF(? > max_pos, ?, max_pos)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            String userId = json.get("user_id").asText();
            String videoId = json.get("video_id").asText();
            int newMaxPos = json.get("max_pos").asInt();

            pstmt.setString(1, userId);
            pstmt.setString(2, videoId);
            pstmt.setInt(3, newMaxPos);

            pstmt.setInt(4, newMaxPos);
            pstmt.setInt(5, newMaxPos);

            pstmt.executeUpdate();
        }
    }

}