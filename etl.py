from source import Source
from sink import Sink
import argparse

parser = argparse.ArgumentParser(description='<= Simple data transfer => ')
parser.add_argument('--version', action='version', version='%(ETL)s 0.1')
parser.add_argument('--from', metavar='FROM', required=True, dest='from_source',
                      help='source name from config file')
parser.add_argument('--to', metavar='TO', required=True, dest='to_sink',
                    help='sink name from config file')

if __name__ == '__main__':
    args = parser.parse_args()
    payload = Source(args.from_source).pull_data()
    Sink(args.to_sink).push_data(payload)


