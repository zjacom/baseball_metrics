FROM apache/airflow:2.4.1-python3.10
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/app"

USER airflow
RUN pip install selenium && \
    pip install bs4 && \
    pip install lxml && \
    pip install selenium-stealth \
    pip install beautifulsoup4

COPY requirements.txt /lib.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /lib.txt