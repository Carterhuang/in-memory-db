from imdb import InMemoryDB
import argparse
import sys

cache = InMemoryDB()

########### Start of main program ###################

# Parsing args
parser = argparse.ArgumentParser(description='In-memory database.')
parser.add_argument('-f', '--file', nargs='?', 
    default='', help='Read input from an input file')

args = vars(parser.parse_args())

if args['file'] != '':
    with open(args['file'], 'r') as f:
        for line in f:
            cache.take_transaction(line)

else:
    while 1:
        line = sys.stdin.readline()
        cache.take_transaction(line)
