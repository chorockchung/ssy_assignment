## 플랫폼 / 데이터 엔지니어링 인턴 채용 과제
---
## 🛠 Tech Stack
* **Language:** Java 17
* **Framework:** Maven / Gradle
* **Message Broker:** Apache Kafka 
* **Database:** MySQL 8.0 

---

## 🚀 실행 방법 (Getting Started)
**1️⃣ Docker 미사용 시 (Local)** <br />

**1. Kafka 설정 및 토픽 생성** <br />

**Kafka 실행 (윈도우 터미널)** <br />
bin/kafka-server-start.sh config/server.properties

**필수 토픽 생성** <br />
bin/kafka-topics.sh --create --topic video-lastpos-logs --bootstrap-server localhost:9092<br />
bin/kafka-topics.sh --create --topic video-rewatch-logs --bootstrap-server localhost:9092<br />
bin/kafka-topics.sh --create --topic video-skip-logs --bootstrap-server localhost:9092<br />
bin/kafka-topics.sh --create --topic video-maxpos-logs --bootstrap-server localhost:9092<br />

**2. 데이터베이스 초기화**

mysql/init/init.sql 경로의 SQL 파일을 실행하여 DB 및 테이블을 생성합니다.

**3. 애플리케이션 실행 (순서 주의!!)**

시작 시: IntegratedLogConsumer 실행 → AutoLogSimulator 실행 <br />
종료 시: AutoLogSimulator 종료 → IntegratedLogConsumer 종료

**2️⃣ Docker 사용 시**

**1. 소스코드 환경 설정 (IP 변경)** 

Docker 네트워크 환경에 맞춰 아래 코드의 주석을 교체합니다. <br />

AutoLogSimulator.java: 35번 줄 주석 처리 후 36번 줄 주석 해제 <br />

IntegratedLogConsumer.java: <br />
15번 줄 주석 처리 후 16번 줄 주석 해제, 25번 줄 주석 처리 후 26번 줄 주석 해제

**2. 인프라 실행**

Kafka 및 DB 컨테이너 실행 <br />
docker-compose up -d

---

## 스키마 설명
하단 2번에 작성 해두었습니다!

---

## 구현하면서 고민한 점
**1. 데이터 부하 분산**<br />
고민: 0.2초마다 발생하는 대량의 시청 로그를 매번 DB에 UPDATE하면 시스템 부하가 커질 것이라 판단했습니다. <br />
해결: 애플리케이션 계층에서 HashMap을 사용해 유저별 max_pos를 관리하고, 프로그램이 종료되는 시점(Shutdown Hook)에만 최종 데이터를 Kafka로 전송하는 Write-back 전략을 선택했습니다.<br />
향후 보완점: 현재는 종료 시점에만 전송하도록 구현되어 있어 비정상 종료 시 데이터 유실 가능성이 있어 현업에서 많이 사용하는 데이터 복원 시간 간격을 도입해 코드를 보완하고 싶습니다.

**2. 유저 행동 시뮬레이션**<br />
고민: 실제 유저는 영상을 순차적으로만 보지 않습니다. 다시보기(Rewatch)나 건너뛰기(Skip) 같은 불규칙한 행동 패턴을 어떻게 데이터화할지 고민했습니다.<br />
해결: 실제 유저들이 영상을 보는 방법을 검색 후, 확률을 도입하여 50%는 정상 시청, 10%는 다시보기, 40%는 건너뛰기 이벤트를 발생시키도록 시뮬레이터를 만들었습니다.

**3. 강사와 회사에 모두 도움되는 데이터 분석**<br />
고민: 시청률 외에 '콘텐츠의 질'을 평가하기 위해 어떤 유저 행동을 데이터화해야 할지 고민했습니다. 단순히 '본다'는 행위만으로는 유저가 실제로 내용을 이해했는지, 혹은 특정 구간이 지루한지 판단하기 어렵기 때문입니다<br />
해결: 유저의 의도를 반영할 수 있는 세 가지 핵심 이벤트를 정의하고 시뮬레이션에 도입했습니다.<br />
   - NORMAL_WATCH (정상 시청): 기본 잔존율 및 완강률 계산을 위한 기초 데이터로 활용.<br />
   - SEEK_BACK (다시보기): 특정 구간의 반복 시청 횟수를 파악하여, 해당 강의의 '핵심 구간'이나 '이해하기 어려운 난이도 높은 구간'을 식별하는 지표로 설계.<br />
   - SEEK_FORWARD (건너뛰기): 유저가 불필요하다고 느끼는 구간을 파악하여, 콘텐츠의 '지루함'이나 '불필요한 부분'를 찾아내는 지표로 설계.<br /><br />

이번 프로젝트를 통해 좋은 알고리즘을 작성하는 것을 넘어, 데이터가 흐르는 전체 파이프라인에서 발생할 수 있는 데이터 병목 현상과 예외 상황을 미리 예측하고 방지하는 설계의 중요성을 깨달았습니다.

---

## 1. 생성한 이벤트
동영상 이벤트
  - 실시간 시청 및 이탈 
  - 동영상 되감기(다시보기)
  - 동영상 스킵
    

플랫폼의 지속 가능한 성장을 위해서는 공급자인 강사가 수익을 창출할 수 있는 환경이 필수적입니다. <br />
이를 위해 YouTube 분석 지표를 벤치마킹하여, 강사가 콘텐츠의 질을 개선할 수 있도록 돕는 강의 분석 대시보드를 기획했습니다. <br />
특히 시청 데이터 중 콘텐츠 개선에 가장 직관적인 인사이트를 제공하는 동영상 이탈률, 구간별 재시청 및 스킵 구간을 핵심 지표로 선정하여 데이터 파이프라인을 설계했습니다.<br />

---

## 2. 로그 저장
  - MySQL 8.0: 
   Java 및 Spring Boot 환경에서 가장 검증된 RDBMS로, 높은 데이터 일관성과 관리 편의성을 제공합니다.  <br />
   이전 프로젝트와 실무 환경에서의 운용 경험을 바탕으로, 데이터의 무결성을 보장하며 안정적으로 관리하기 위해 선택했습니다. <br /> <br />

- Apache Kafka: 
동영상 스트리밍 로그는 특성상 단시간에 대량의 트래픽이 발생합니다.   <br />
DB에 직접 쓰기 작업을 수행할 경우 발생하는 병목 현상을 방지하고, 시스템 간의 결합도를 낮추기 위해 Kafka를 중간 메시지 브로커로 도입했습니다.  <br />
이를 통해 실시간 로그 데이터를 비동기적으로 처리하여 데이터 파이프라인의 가용성을 극대화했습니다.  <br />  <br />


   CREATE TABLE `video_lastpos_logs` ( <br />
  `id` int NOT NULL AUTO_INCREMENT,<br />
  `user_id` varchar(50) NOT NULL,<br />
  `video_id` varchar(50) NOT NULL,<br />
  `last_pos` int NOT NULL,<br />
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,<br />
  `max_pos` int DEFAULT NULL,<br />
  PRIMARY KEY (`id`),<br />
  KEY `idx_video_lastposLog` (`video_id`,`user_id`)<br />
) ENGINE=InnoDB AUTO_INCREMENT=4191 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;<br /><br />

이탈률을 분석하려면 유저가 현재 어디 남아 있는지 분석해야한다고 생각해 last_pos를 넣었고 <br />
max_pos로 어디까지 봤는지 확인하려고 넣었습니다.<br />

CREATE TABLE `video_rewatch_logs` (<br />
  `id` int NOT NULL AUTO_INCREMENT,<br />
  `user_id` varchar(50) NOT NULL,<br />
  `video_id` varchar(50) NOT NULL,<br />
  `event_type` varchar(20) DEFAULT 'SEEK_BACK',<br />
  `prev_pos` int NOT NULL,<br />
  `current_pos` int NOT NULL,<br />
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,<br />
  PRIMARY KEY (`id`),<br />
  KEY `idx_video_rewatch` (`video_id`,`user_id`)<br />
) ENGINE=InnoDB AUTO_INCREMENT=265 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;<br /><br />

유튜브의 '가장 많이 본 부분' 에서 차용하여, 되감기를 시작한 첫 부분(prev_pos)과, 되감기해서 보고있는 현재 부분(current_pos)을 넣었습니다. <br />

CREATE TABLE `video_skipping_logs` ( <br />
  `id` int NOT NULL AUTO_INCREMENT,<br />
  `user_id` varchar(50) NOT NULL,<br />
  `video_id` varchar(50) NOT NULL,<br />
  `event_type` varchar(20) DEFAULT 'SEEK_FORWARD',<br />
  `prev_pos` int NOT NULL,<br />
  `current_pos` int NOT NULL,<br />
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,<br />
  PRIMARY KEY (`id`),<br />
  KEY `idx_video_skipping` (`video_id`,`user_id`)<br />
) ENGINE=InnoDB AUTO_INCREMENT=987 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;<br /><br />

동영상의 어느 부분이 가장 가치가 떨어지는지 판단하기 위해,<br />
뛰어넘기를 시작한 부분 (prev_pos)와 현재 뛰어넘어서 시청중인 부분 current_pos를 넣었습니다.<br /><br />

## 3. 데이터 분석

**구간별 잔존 유저 수**<br /><br />

   SELECT <br />
    FLOOR(max_last_pos / 10) * 10 AS section_start, -- 10초 단위 구간<br />
    COUNT(*) AS stay_user_count -- 해당 구간에서 시청을 멈춘 유저 수<br />
    FROM (<br />
        -- 각 유저별 해당 영상의 최종 시청 위치 추출<br />
        SELECT user_id, MAX(last_pos) AS max_last_pos<br />
        FROM video_lastpos_logs<br />
        WHERE video_id = 'vid_1' -- 분석할 영상 ID<br />
        GROUP BY user_id<br />
    ) AS user_final_status<br />
    GROUP BY section_start<br />
    ORDER BY section_start;<br /><br />

**스킵한 구간(gap)** <br /><br />
SELECT <br />
    FLOOR(skipstart / 10) * 10 AS section_start,-- 10초 단위 구간<br />
    AVG(currentPos - skipstart) AS avg_skip_duration, <br />
    COUNT(*) AS total_skips<br />
FROM video_skip_logs<br />
WHERE video_id = 'vid_1'<br />
GROUP BY section_start<br />
HAVING avg_skip_duration > 0<br />
ORDER BY section_start;<br /><br />


**가장 많이 다시본 구간**<br /><br />
SELECT <br />
    FLOOR(current_pos / 10) * 10 AS rewatch_section, -- 10초 단위 구간<br />
    COUNT(*) AS rewatch_count<br />
FROM video_logs<br />
WHERE event_type = 'SEEK_BACK' <br />
  AND video_id = 'vid_1'<br />
GROUP BY rewatch_section<br />
ORDER BY rewatch_count DESC<br />
LIMIT 5;<br /><br />


**4. 데이터 분석 차트는 git에 올려두었습니다.** <br />
5. docker의 경우 컴퓨터에서 실행되지 않아서 실제로 돌려보지 못했지만 yml 파일은 작성해두었습니다.



