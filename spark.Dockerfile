FROM bitnami/spark:3.5.6
USER root
RUN install_packages curl
USER 1001
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar
RUN curl https://jdbc.postgresql.org/download/postgresql-42.7.3.jar --output /opt/bitnami/spark/jars/postgresql-42.7.3.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar