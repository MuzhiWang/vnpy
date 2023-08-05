FROM openjdk:8

LABEL maintainer="wmz669082@gmail.com"

WORKDIR /kafka

COPY . /kafka

# ENV DEBIAN_FRONTEND noninteractive
# RUN apt-get update && \
#     apt-get install -y wget

# RUN wget -q http://mirrors.hust.edu.cn/apache/kafka/1.0.0/kafka_2.11-1.0.0.tgz
# RUN tar -xzf /kafka/kafka_2.13-3.0.0.tgz -C .
# RUN mv /home/kafka_2.13-3.0.0/* /kafka


#CMD ["/bin/bash"]

ENTRYPOINT ["./kafka_2.11-2.3.0/bin/kafka-server-start.sh", "./kafka_2.11-2.3.0/config/server.properties"]
# ENTRYPOINT [ "/kafka/start.sh" ]