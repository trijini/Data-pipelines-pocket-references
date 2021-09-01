### 1. Installing Apache Airflow

https://airflow.apache.org/docs/apache-airflow/stable/start/local.html

```shell
# 가상환경에서 실행
$ python -m venv venv

# 여기서부터 설치 단계
$ export AIRFLOW_HOME=~/airflow

$ AIRFLOW_VERSION=2.1.3
$ PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
$ CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
$ pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 데이터베이스 생성 (기본 sqlite)
$ airflow db init
$ airflow users create \
    --username admin \
    --firstname Jiwoo \
    --lastname Choi \
    --role Admin \
    --email cho2wldn@gmail.com
# 비밀번호 설정

$ airflow webserver --port 8080
$ airflow scheduler
# localhost:8080 접속
```

### 2. Setting up PostgreSQL

Start postgresql server

```psql
cho2jiwoo=# CREATE USER airflow;
CREATE ROLE
cho2jiwoo=# ALTER USER airflow WITH PASSWORD 'my_password';
ALTER ROLE
cho2jiwoo=# CREATE DATABASE airflowdb;
CREATE DATABASE
cho2jiwoo=# GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflow;
GRANT
```

### 3. Setting airflow database to PostgreSQL

일반 psycopg2는 prerequisites이 필요하기 때문에 psycopg2-binary로 설치

참고: https://www.psycopg.org/docs/install.html#build-prerequisites

```shell
$ pip install psycopg2-binary
$ vim airflow/airflow.cfg # cd airflow 한 후에 vim airflow.cfg 도 가능

...
# DBMS 변경
# sql_alchemy_conn = sqlite:////Users/myuser/airflow/airflow.db
sql_alchemy_conn = postgresql+psycopg2://airflow:my_password@localhost:5432/airflowdb
...
```

