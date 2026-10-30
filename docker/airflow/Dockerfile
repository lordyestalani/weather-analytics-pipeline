FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt
# Keep Airflow pinned while adding the pipeline's dependencies.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt \
    && pip check

# New named volumes inherit this writable directory's ownership and permissions.
RUN mkdir -p /opt/airflow/auth && chmod 0770 /opt/airflow/auth
