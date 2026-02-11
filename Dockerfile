FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu


USER root
# Install Python3, pip, pandas, and psycopg2 dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
RUN pip3 install --no-cache-dir \
    pandas \
    psutil \
    psycopg2-binary\
    prometheus_client


ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR /app