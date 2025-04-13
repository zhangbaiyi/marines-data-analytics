FROM python:3.12

WORKDIR /marines-data-analytics

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 
ENV PATH="$JAVA_HOME/bin:${PATH}"

COPY requirements.txt ./
RUN pip3 install -r requirements.txt
COPY . .

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENV PYTHONPATH=/marines-data-analytics
RUN python3 src/scripts/data_warehouse/init_db.py
RUN python3 src/scripts/data_warehouse/load_db.py
ENTRYPOINT ["streamlit", "run", "src/scripts/Home.py", "--server.port=8501", "--server.address=0.0.0.0"]