from abc import ABC, abstractmethod
import configparser
import csv
import boto3
import psycopg2


class Parser(ABC):

    def __init__(self, info: str) -> None:
        self.info = info

    @abstractmethod
    def get_config(self) -> dict:
        pass

    @abstractmethod
    def connect(self) -> None:
        pass


class Psycopg2(Parser):
    def __init__(self, info) -> None:
        super().__init__(info)

    def get_config(self) -> dict:
        parser = configparser.ConfigParser()
        parser.read('pipeline.conf')

        dbname = parser.get(self.info, 'database')
        user = parser.get(self.info, 'username')
        password = parser.get(self.info, 'password')
        host = parser.get(self.info, 'host')
        port = parser.get(self.info, 'port')
        iam_role = parser.get(self.info, 'iam_role')

        config = dict(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            iam_role=iam_role,
        )

        return config

    def connect(self) -> psycopg2.connection:
        config = self.get_config()
        connection_info = (f"dbname={config['dbname']} user={config['user']} "
                           f"password={config['password']} "
                           f"host={config['host']} port={config['port']}")
        conn = psycopg2.connect(connection_info)

        return conn


class S3(Parser):
    def __init__(self, info) -> None:
        super().__init__(info)

    def get_config(self) -> dict:
        parser = configparser.ConfigParser()
        parser.read('pipeline.conf')

        access_key = parser.get(self.info, 'access_key')
        secret_key = parser.get(self.info, 'secret_key')
        bucket_name = parser.get(self.info, 'bucket_name')
        account_id = parser.get(self.info, 'account_id')

        config = dict(
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name,
            account_id=account_id,
        )

        return config

    def connect(self) -> S3.Client:
        config = self.get_config()
        conn = boto3.client(
                's3',
                aws_access_key_id=config['access_key'],
                aws_secret_access_key=config['secret_key'],
            )

        return conn
