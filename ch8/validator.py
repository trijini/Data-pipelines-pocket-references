import sys
import psycopg2
import json
import requests
import configparser
from pathlib import Path


def connect_to_warehouse():
    parent_dir = str(Path(__file__).parents[1]) + '/'

    parser = configparser.ConfigParser()
    config_path = parent_dir + 'pipeline.conf'
    parser.read(config_path)
    dbname = parser.get('postgres_config', 'database')
    user = parser.get('postgres_config', 'username')
    password = parser.get('postgres_config', 'password')
    host = parser.get('postgres_config', 'host')
    port = parser.get('postgres_config', 'port')

    rs_conn = psycopg2.connect(
        'dbname=' + dbname +
        ' user=' + user +
        ' password=' + password +
        ' host=' + host +
        ' port=' + port
    )

    return rs_conn


def execute_test(db_conn, script_1, script_2, comp_operator):
    cursor = db_conn.cursor()
    sql_file = open(script_1, 'r')
    cursor.execute(sql_file.read())

    record = cursor.fetchone()
    result_1 = record[0]
    db_conn.commit()
    cursor.close()

    cursor = db_conn.cursor()
    sql_file = open(script_2, 'r')
    cursor.execute(sql_file.read())

    record = cursor.fetchone()
    result_2 = record[0]
    db_conn.commit()
    cursor.close()

    print(f'result 1 = {str(result_1)}')
    print(f'result 2 = {str(result_2)}')

    if comp_operator == 'equals':
        return result_1 == result_2
    elif comp_operator == 'greater_equals':
        return result_1 >= result_2
    elif comp_operator == 'greater':
        return result_1 > result_2
    elif comp_operator == 'less_equals':
        return result_1 <= result_2
    elif comp_operator == 'less':
        return result_1 < result_2
    elif comp_operator == 'not_equal':
        return result_1 != result_2


def send_slack_notification(webhook_url, script_1, script_2,
                            comp_operator, test_result):
    try:
        if test_result is True:
            message = ('Validation Test Passed!: '
                       f'{script_1} / {script_2} / {comp_operator}')
        else:
            message = ('Validation Test FAILED!: '
                       f'{script_1} / {script_2} / {comp_operator}')
        slack_data = {'text': message}
        response = requests.post(
            url=webhook_url,
            data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'},
        )
        if response.status_code != 200:
            print(response)
            return False

    except Exception as e:
        print('error sending slack notification')
        print(str(e))
        return False


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '-h':
        print(('Usage: python validator.py' +
               'script1.sql script2.sql ' +
               'comparison_operator'))
        print('valid comparison_operator values:')
        print('equals')
        print('greater_equals')
        print('greater')
        print('less_equals')
        print('less')
        print('not_equal')

        exit(0)

    if len(sys.argv) != 5:
        print(('Usage: python validator.py' +
               'script1.sql script2.sql' +
               'comparison_operator'))
        exit(-1)

    script_1 = sys.argv[1]
    script_2 = sys.argv[2]
    comp_operator = sys.argv[3]
    sev_level = sys.argv[4]

    db_conn = connect_to_warehouse()
    test_result = execute_test(db_conn, script_1, script_2, comp_operator)
    print(f'Result of test: {str(test_result)}')

    if test_result is True:
        exit(0)
    else:
        # send_slack_notification(webhook_url, script_1, script_2,
        #                         comp_operator, test_result)
        exit(-1)
