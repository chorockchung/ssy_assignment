package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.*;

public class AutoLogSimulator {

    // 유저-비디오별 상태를 저장하기 위한 Record
    record UserVideoKey(String userId, String videoId) {}

    public static void main(String[] args) {
        // 이벤트 객체 생성
        VideoLastposProducer videoLastposProducer = new VideoLastposProducer();
        VideoRewatchProducer videoRewatchProducer = new VideoRewatchProducer();
        VideoSkippingProducer videoSkippingProducer = new VideoSkippingProducer();
        VideoMaxposProducer videoMaxposProducer = new VideoMaxposProducer();

        Random random = new Random();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // 유저의 현재 시청 위치와 최대 도달 위치를 관리할 Map
        Map<UserVideoKey, Integer> userVideoLastPosMap = new HashMap<>();
        Map<UserVideoKey, Integer> userVideoMaxPosMap = new HashMap<>();

        //kafka 설정
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> sharedKafkaProducer = new KafkaProducer<>(props);

        System.out.println("🚀 데이터 기반 시청 분석 시뮬레이터를 시작합니다...");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 1. 대상 유저 및 비디오 선정
                String userId = "user_" + (random.nextInt(100) + 1); // 100명의 유저
                String videoId = "vid_" + (random.nextInt(10) + 1);  // 10개의 강의
                int videoLength = 600; // 10분 영상

                UserVideoKey key = new UserVideoKey(userId, videoId);

                // 2. 이전 위치 가져오기 (처음이면 0초부터 시작)
                int lastPos = userVideoLastPosMap.getOrDefault(key, 0);
                int maxPos = userVideoMaxPosMap.getOrDefault(key, 0);
                int currentPos;

                // 3. 확률 기반 행동 시뮬레이션
                double p = random.nextDouble();

                if (p < 0.5 || maxPos == 0) {
                    // [정상 시청] 50% 확률로 5초씩 전진
                    currentPos = lastPos + 5;
                } else if (p < 0.6) {
                    // [다시보기 - SEEK_BACK] 10% 확률로 이전 지점 중 무작위로 돌아감
                    currentPos = random.nextInt(lastPos + 1);
                    videoRewatchProducer.sendRewatchLog(sharedKafkaProducer, userId, videoId, "SEEK_BACK", currentPos, lastPos);
                } else {
                    // [건너뛰기 - SEEK_FORWARD] 40% 확률로 15~50초 사이를 뛰어넘음
                    currentPos = lastPos + 15 + random.nextInt(35);
                    videoSkippingProducer.sendJumpingLog(sharedKafkaProducer, userId, videoId, "SEEK_FORWARD", lastPos, currentPos);
                }

                // 영상 길이를 초과하지 않도록 보정
                if (currentPos > videoLength) currentPos = videoLength;

                // 4. 데이터 업데이트 및 Kafka 전송
                // 최대 시청 지점 갱신 (True Completion Rate 분석용)
                if (currentPos > maxPos) {
                    userVideoMaxPosMap.put(key, currentPos);
                }

                // 현재 위치 저장 (Heartbeat 대용)
                userVideoLastPosMap.put(key, currentPos);

                // 분석 로그 전송 (현재 위치는 항상 전송하여 리텐션 측정)
                videoLastposProducer.sendLastposLog(sharedKafkaProducer, userId, videoId, currentPos);

                // 영상 끝까지 다 봤을 경우 초기화 (무한 시뮬레이션을 위해)
                if (currentPos >= videoLength) {
                    userVideoLastPosMap.put(key, 0);
                    userVideoMaxPosMap.put(key, 0);
                }

            } catch (Exception e) {
                System.err.println("로그 생성 오류: " + e.getMessage());
            }
        }, 0, 200, TimeUnit.MILLISECONDS); // 0.2초마다 이벤트 발생

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 종료 신호 감지! 자원을 정리합니다...");

            // 메모리에 쌓인 최종 maxPos 데이터를 Kafka로 전송
            userVideoMaxPosMap.forEach((key, maxPos) -> {
                videoMaxposProducer.sendMaxposLog(sharedKafkaProducer, key.userId(), key.videoId(), maxPos);
            });


            System.out.println("⏳ 메시지 전송 완료를 기다리는 중 (Flush)...");
            sharedKafkaProducer.flush();
            scheduler.shutdown();

            sharedKafkaProducer.close();

        }));
    }
}