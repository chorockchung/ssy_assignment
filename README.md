##플랫폼 / 데이터 엔지니어링 인턴 채용 과제

## 🛠 Tech Stack
* **Language:** Java 17
* **Framework:** Maven / Gradle
* **Message Broker:** Apache Kafka 
* **Database:** MySQL 8.0 

---

🚀 실행 방법 (Getting Started)
1️⃣ Docker 미사용 시 (Local)
1. Kafka 설정 및 토픽 생성

Bash
# Kafka 실행 (윈도우 터미널)
bin/kafka-server-start.sh config/server.properties

# 필수 토픽 생성
bin/kafka-topics.sh --create --topic video-lastpos-logs --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic video-rewatch-logs --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic video-skip-logs --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic video-maxpos-logs --bootstrap-server localhost:9092

2. 데이터베이스 초기화

mysql/init/init.sql 경로의 SQL 파일을 실행하여 DB 및 테이블을 생성합니다.

3. 애플리케이션 실행 (순서 주의 ‼)

시작 시: IntegratedLogConsumer 실행 → AutoLogSimulator 실행

종료 시: AutoLogSimulator 종료 → IntegratedLogConsumer 종료

2️⃣ Docker 사용 시
1. 소스코드 환경 설정 (IP 변경)
Docker 네트워크 환경에 맞춰 아래 코드의 주석을 교체합니다.

AutoLogSimulator.java: 35번 줄 주석 처리 후 36번 줄 주석 해제

IntegratedLogConsumer.java:

15번 줄 주석 처리 후 16번 줄 주석 해제
25번 줄 주석 처리 후 26번 줄 주석 해제

2. 인프라 실행

# Kafka 및 DB 컨테이너 실행
docker-compose up -d


## 스키마 설명
하단 2번에 작성 해두었습니다!

## 구현하면서 고민한 점
1. 데이터 부하 분산 
고민: 0.2초마다 발생하는 대량의 시청 로그를 매번 DB에 UPDATE하면 시스템 부하가 커질 것이라 판단했습니다.
해결: 애플리케이션 계층에서 HashMap을 사용해 유저별 max_pos를 관리하고, 프로그램이 종료되는 시점(Shutdown Hook)에만 최종 데이터를 Kafka로 전송하는 Write-back 전략을 선택했습니다.
향후 보완점: 현재는 종료 시점에만 전송하도록 구현되어 있어 비정상 종료 시 데이터 유실 가능성이 있어 현업에서 많이 사용하는 데이터 복원 시간 간격을 도입해 코드를 보완하고 싶습니다.

2. 유저 행동 시뮬레이션
고민: 실제 유저는 영상을 순차적으로만 보지 않습니다. 다시보기(Rewatch)나 건너뛰기(Skip) 같은 불규칙한 행동 패턴을 어떻게 데이터화할지 고민했습니다.
해결: 실제 유저들이 영상을 보는 방법을 검색 후, 확률을 도입하여 50%는 정상 시청, 10%는 다시보기, 40%는 건너뛰기 이벤트를 발생시키도록 시뮬레이터를 만들었습니다.

3. 강사와 회사에 모두 도움되는 데이터 분석
고민: 시청률 외에 '콘텐츠의 질'을 평가하기 위해 어떤 유저 행동을 데이터화해야 할지 고민했습니다. 단순히 '본다'는 행위만으로는 유저가 실제로 내용을 이해했는지, 혹은 특정 구간이 지루한지 판단하기 어렵기 때문입니다.
해결: 유저의 의도를 반영할 수 있는 세 가지 핵심 이벤트를 정의하고 시뮬레이션에 도입했습니다.
   - NORMAL_WATCH (정상 시청): 기본 잔존율 및 완강률 계산을 위한 기초 데이터로 활용.
   - SEEK_BACK (다시보기): 특정 구간의 반복 시청 횟수를 파악하여, 해당 강의의 '핵심 구간'이나 '이해하기 어려운 난이도 높은 구간'을 식별하는 지표로 설계.
   - SEEK_FORWARD (건너뛰기): 유저가 불필요하다고 느끼는 구간을 파악하여, 콘텐츠의 '지루함'이나 '불필요한 군더더기'를 찾아내는 지표로 설계.

이번 프로젝트를 통해 좋은 알고리즘을 작성하는 것을 넘어, 데이터가 흐르는 전체 파이프라인에서 발생할 수 있는 데이터 병목 현상과 예외 상황을 미리 예측하고 방지하는 설계의 중요성을 깨달았습니다.


1. 생성한 이벤트
동영상 이벤트
  - 실시간 시청 및 이탈 
  - 동영상 되감기(다시보기)
  - 동영상 스킵

라이브클래스의 주된 상품은 강의를 판매하는 플랫폼이고, 해당 플랫폼이 잘 유지되기 위해서는 강의자들이 충분한 수익을 내야할텐데,
수익을 계속해서 내기 위해서는 강의자용 대시보드가 필수라고 생각했습니다.
유튜브 채널 애널리틱스에서 주된 요소들을 참고했습니다.
영상에서 분석했을 때 가장 좋은 데이터는 동영상 이탈률, 가장 많이 다시 본 부분, 건너 뛴 부분이라고 생각했습니다.


2. 로그 저장
   mysql, kafka를 사용했습니다.
   mysql은 java 및 spring boot와 가장 많이 커플링으로 사용되는 rdbms고, 저의 프로젝트와 회사에서 사용해보았기 떄문에 선택했습니다.
   동영상 데이터 특성상 실시간 데이터를 분석하기 위해 수많은 로그가 빠르게 쌓이는데, mysql의 부담을 줄이기 위해 중간 브로커로 kafka를 선택했습니다.

   CREATE TABLE `video_lastpos_logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(50) NOT NULL,
  `video_id` varchar(50) NOT NULL,
  `last_pos` int NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `max_pos` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_video_lastposLog` (`video_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4191 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

이탈률을 분석하려면 유저가 현재 어디 남아 있는지 분석해야한다고 생각해 last_pos를 넣었고
max_pos로 어디까지 봤는지 확인하려고 넣었습니다.

CREATE TABLE `video_rewatch_logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(50) NOT NULL,
  `video_id` varchar(50) NOT NULL,
  `event_type` varchar(20) DEFAULT 'SEEK_BACK',
  `prev_pos` int NOT NULL,
  `current_pos` int NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_video_rewatch` (`video_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=265 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

유튜브의 '가장 많이 본 부분' 에서 차용하여, 되감기를 시작한 첫 부분(prev_pos)과, 되감기해서 보고있는 현재 부분(current_pos)을 넣었습니다.

CREATE TABLE `video_skipping_logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(50) NOT NULL,
  `video_id` varchar(50) NOT NULL,
  `event_type` varchar(20) DEFAULT 'SEEK_FORWARD',
  `prev_pos` int NOT NULL,
  `current_pos` int NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_video_skipping` (`video_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=987 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

동영상의 어느 부분이 가장 가치가 떨어지는지 판단하기 위해,
뛰어넘기를 시작한 부분 (prev_pos)와 현재 뛰어넘어서 시청중인 부분 current_pos를 넣었습니다.

3. 데이터 분석

구간별 잔존 유저 수
   SELECT 
    FLOOR(max_last_pos / 10) * 10 AS section_start, -- 10초 단위 구간
    COUNT(*) AS stay_user_count -- 해당 구간에서 시청을 멈춘 유저 수
    FROM (
        -- 각 유저별 해당 영상의 최종 시청 위치 추출
        SELECT user_id, MAX(last_pos) AS max_last_pos
        FROM video_lastpos_logs
        WHERE video_id = 'vid_1' -- 분석할 영상 ID
        GROUP BY user_id
    ) AS user_final_status
    GROUP BY section_start
    ORDER BY section_start;

스킵한 구간(gap)
SELECT 
    FLOOR(skipstart / 10) * 10 AS section_start,-- 10초 단위 구간
    AVG(currentPos - skipstart) AS avg_skip_duration, 
    COUNT(*) AS total_skips
FROM video_skip_logs
WHERE video_id = 'vid_1'
GROUP BY section_start
HAVING avg_skip_duration > 0
ORDER BY section_start;


가장 많이 다시본 구간
SELECT 
    FLOOR(current_pos / 10) * 10 AS rewatch_section, -- 10초 단위 구간
    COUNT(*) AS rewatch_count
FROM video_logs
WHERE event_type = 'SEEK_BACK' 
  AND video_id = 'vid_1'
GROUP BY rewatch_section
ORDER BY rewatch_count DESC
LIMIT 5;


4. 데이터 분석 차트는 git에 올려두었습니다. 
5. docker의 경우 컴퓨터에서 실행되지 않아서 실제로 돌려보지 못했지만 yml 파일은 작성해두었습니다.



