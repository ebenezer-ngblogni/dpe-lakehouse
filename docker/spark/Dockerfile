# Image Spark pour l'exécution des jobs Scala.
#
# On part de l'image officielle Apache plutôt que d'installer Spark à la main :
# elle embarque déjà la bonne combinaison Spark/Hadoop/JVM, source classique
# d'incompatibilités de jars quand on assemble soi-même.
FROM apache/spark:3.5.3-scala2.12-java17-ubuntu

USER root

# Pilotes nécessaires aux jobs : accès S3/MinIO et écriture PostgreSQL.
# Les figer ici évite un téléchargement Maven à chaque `spark-submit`, qui
# rendrait les exécutions dépendantes du réseau.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4

RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSLO "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    curl -fsSLO "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Configuration S3A commune à tous les jobs, pour ne pas la répéter en ligne de
# commande à chaque soumission.
RUN printf '%s\n' \
    'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' \
    'spark.hadoop.fs.s3a.path.style.access=true' \
    'spark.hadoop.fs.s3a.connection.ssl.enabled=false' \
    'spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider' \
    'spark.sql.sources.partitionOverwriteMode=dynamic' \
    > /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/spark
