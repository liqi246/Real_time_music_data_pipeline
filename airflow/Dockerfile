FROM apache/airflow:2.10.5

USER airflow

RUN pip install \
    --no-cache-dir \
    --default-timeout=300 \
    --retries 10 \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    setuptools \
    wheel \
    oss2 \
    pyarrow==19.0.1 \
    requests
