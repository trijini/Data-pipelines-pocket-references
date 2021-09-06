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

만약 홈디렉터리 말고 다른곳에 설치하고 싶다면 bash나 zsh 파일안에 path를 지정해주면 된다. (홈디렉토리를 의미하는 물결표시 ~ 는 경로에서 빼줘야 원하는 곳에 설치할 수 있다)

`vim ~/.zshrc`

```bash
export AIRFLOW_HOME=/Users/home/.../airflow
```

설치하려는 디렉터리의 경로는 터미널에서 해당 경로로 가서 `pwd` 명령어를 치면 쉽게 알 수 있다.

```bash
$ cd /Users/home/documents/whateverdirectory
$ pwd
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
```

airflow db init 명령어를 입력했을 때 airflow 라는 디렉터리가 생성되었을 것이고 그 안에 있는 airflow.cfg 파일에 약간의 수정이 필요하다.

Executor를 `SequentialExecutor`에서 `LocalExecutor` 변경해준다.

```
Executor = LocalExecutor
```

sql_alchemy_conn를 `sqlite:////Users/myuser/airflow/airflow.db` 에서 `postgresql+psycopg2://airflow:my_password@localhost:5432/airflowdb`

```
sql_alchemy_conn = postgresql+psycopg2://airflow:my_password@localhost:5432/airflowdb
```

그리고 이건 선택 사항인데 load_examples 를 True에서 False로 변경해주면 airflow 대쉬보드에 예제 DAGS 들이 나타나지 않는다.
