FROM ubuntu:18.04

EXPOSE 8080

ADD target/haxwell*.jar haxwell-kafka.jar

CMD java -jar -XX:+UseG1GC -Xms${JAVA_HEAP-1g} -Xmx${JAVA_HEAP-1g} haxwell-kafka.jar