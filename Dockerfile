FROM openjdk:17-jdk-slim
# 빌드된 jar 파일을 컨테이너 내부로 복사 (이름은 실제 빌드된 파일명에 맞춰 수정)
COPY target/*.jar app.jar
# 실행 (command는 docker-compose에서 덮어씌움)
ENTRYPOINT ["java", "-jar", "/app.jar"]