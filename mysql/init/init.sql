-- DB 생성
CREATE DATABASE IF NOT EXISTS kafka_log_db;

-- 임시 사용자 생성 및 권한 부여
CREATE USER 'dev_user'@'%' IDENTIFIED BY 'dev_pass123!';
GRANT ALL PRIVILEGES ON kafka_log_db.* TO 'dev_user'@'%';
FLUSH PRIVILEGES;

USE kafka_log_db;

-- 테이블 생성 (질문자님이 작성하신 DDL
CREATE TABLE IF NOT EXISTS video_lastpos_logs (
                                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                                  user_id VARCHAR(50), video_id VARCHAR(50), last_pos INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS video_rewatch_logs (
                                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                                  user_id VARCHAR(50), video_id VARCHAR(50), event_type VARCHAR(20),
    prev_pos INT, current_pos INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS video_skipping_logs (
                                                   id INT AUTO_INCREMENT PRIMARY KEY,
                                                   user_id VARCHAR(50), video_id VARCHAR(50), event_type VARCHAR(20),
    prev_pos INT, current_pos INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );