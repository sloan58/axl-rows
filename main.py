import math
import re
import sys

from lib import Axl


def format_sql_query(query, throttle_params):
    if throttle_params['throttling']:
        sub_query = re.search(r"SELECT (.*)", query).group(1)
        query = f"SELECT SKIP {throttle_params['skip']} FIRST {throttle_params['suggested_rows']} {sub_query}"\
            .replace('\r', '')\
            .replace('\n', '')\
            .replace('\t', '')

    return query

def call_axl(client, data, throttle_params):

    sql = format_sql_query('SELECT * FROM device', throttle_params)

    log = f"- Running query {sql}"

    if throttle_params['throttling']:
        log += f" ({throttle_params['loop'] + 1} of {throttle_params['iterations'] + 1})"

    print(log)

    message = {
        'method': 'executeSQLQuery',
        'body': {
            'sql': sql,
        }
    }

    try:
        response = client.call_api(message)

        if response is not None and 'row' in response:
            for row in response['row']:
                data.append(row)

    except Axl.AxlFaultException as err:
        if 'Query request too large' in str(err):
            print('- ****************************************')
            print('- ** Received AXL throttle notification **')
            print('- ****************************************')
            [total_rows, suggested_rows] = re.findall('\d+', str(err))
            throttle_params['throttling'] = True
            throttle_params['total_rows'] = total_rows
            throttle_params['suggested_rows'] = math.floor(int(suggested_rows) / 2)
            throttle_params['iterations'] = math.floor(int(throttle_params['total_rows']) / int(throttle_params['suggested_rows'])) + 1
            while int(throttle_params['loop']) <= throttle_params['iterations']:
                call_axl(client, data, throttle_params)
                throttle_params['skip'] += throttle_params['suggested_rows']
                throttle_params['loop'] += 1
        else:
            print(err)
            sys.exit()

    return data

if __name__ == '__main__':
    data = []
    throttle_params = {
        'throttling': False,
        'total_rows': 0,
        'suggested_rows': 0,
        'skip': 0,
        'loop': 0,
        'iterations': 0
    }

    ucm = {
        'ip': '10.10.10.1',
        'username': 'Administrator',
        'password': 'supersecret',
        'version': '12.5',
    }

    client = Axl.Client(ucm)

    data = call_axl(client, data, throttle_params)
    print(f"Collected {len(data)} objects from AXL API")
