import argparse
import os
import socket
import sys
import time

import sqlalchemy

def get_executor(dsn):
    engine = sqlalchemy.create_engine(dsn)
    connection = engine.connect()
    return connection.execute

def get_info():
    parser = argparse.ArgumentParser(description='Send SQL results to Graphite')
    parser.add_argument('--graphite-host', metavar='graphite-host', type=str, default=None, help='Host to send metrics to')
    parser.add_argument('--graphite-port', metavar='graphite-port', type=int, default=2003, help='Graphite port to send metrics to')
    parser.add_argument('--graphite-prefix', metavar='graphite-prefix', type=str, default='db', help='Prefix for metrics')
    parser.add_argument('--dsn', type=str, default=os.environ.get('S2G_DSN'), help='SQLAlchemy DSN for database connection')
    parser.add_argument('--timestamped-metric', action='store_true', help='Use 3rd column in query containing timestamp values instead of current timestamp')
    return parser.parse_args()

def run(graphite_host, graphite_port, graphite_prefix, timestamped, queries, executor):
    data = []
    now = time.time()
    sock = _socket_for_host_port(graphite_host, graphite_port)
    data = [executor(q) for q in queries]
    for result in data:
        for line in result:
            if timestamped:
                metric, value, timestamp = line[:3]
                metric = '{}.{} {} {:0.0f}\n'.format(graphite_prefix, metric, value, timestamp)
            else:
                metric, value = line[:2]
                metric = '{}.{} {} {:0.0f}\n'.format(graphite_prefix, metric, value, now)
            print (metric)
            sock.sendall(metric.encode())
    sock.close()

def _socket_for_host_port(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((host, port))
    sock.settimeout(None)
    return sock


def main():
    args = get_info()
    if args.dsn is None:
        print ('You must set your DSN in the environment variable `S2G_DSN` or the --dsn argument')
        sys.exit(1)
    else:
        print ('Using DSN: {}'.format(args.dsn))

    queries = sys.stdin.readlines()

    run(
        args.graphite_host,
        args.graphite_port,
        args.graphite_prefix,
        args.timestamped_metric,
        queries,
        get_executor(args.dsn),
    )

if __name__ == '__main__':
    main()
