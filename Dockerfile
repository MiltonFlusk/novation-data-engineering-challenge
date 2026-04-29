FROM nedbank-de-challenge/base:1.0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Fix PySpark path inside Docker
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Required when Docker runs with --network=none
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV PYSPARK_SUBMIT_ARGS="--conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 pyspark-shell"
ENV JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"

# Preload Delta jars during Docker build while network is available.
# This prevents Delta from trying to download dependencies at runtime.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; b=SparkSession.builder.appName('delta-preload').master('local[1]').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.local.hostname','localhost').config('spark.ui.enabled','false'); s=configure_spark_with_delta_pip(b).getOrCreate(); s.stop()"

COPY pipeline/ pipeline/
COPY config/ config/

CMD ["python", "pipeline/run_all.py"]