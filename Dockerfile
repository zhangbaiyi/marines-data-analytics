FROM python:3.12-bullseye

WORKDIR /marines-data-analytics

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        software-properties-common \
        openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Declare the volumes BEFORE any steps that might write to them
VOLUME /marines-data-analytics/db
VOLUME /marines-data-analytics/data_lake

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENV PYTHONPATH=/marines-data-analytics

# The following scripts will now operate on the Docker volumes
RUN python src/scripts/data_warehouse/init_db.py
RUN python src/scripts/data_warehouse/load_db.py

ENTRYPOINT ["streamlit", "run", "src/scripts/Home.py", \
            "--server.port=8501", "--server.address=0.0.0.0"]