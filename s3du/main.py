#!/usr/bin/env python3
# vim: set fileencoding=utf-8 tw=0:
#
# Uses ncdu to interactively inspect all available S3 buckets.
#
# https://github.com/umonkey/s3du
#
# TODO:
# - use system tempdir to store the intermediate json file
# - make sure ncdu is installed
# - make sure boto is configured

from __future__ import print_function

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
import warnings

import boto3


STORAGE_CLASSES = {'STANDARD', 'STANDARD_IA', 'GLACIER', 'DEEP_ARCHIVE'}


class s3du:
    def __init__(self, args):
        self.s3 = boto3.client('s3')
        self.csv_name = os.path.expanduser('~/.cache/s3du-cache.csv')
        self.classes = set()

        self.verbose = args.verbose
        self.interactive = args.interactive
        self.filename = args.filename
        self.keep_file = args.filename is not None
        self.nocache = args.no_cache
        self.storage_class = args.storage_class
        self.bucket = args.bucket
        self.prefix = args.prefix or ''
        if self.prefix and not self.prefix.endswith('/'):
            self.prefix += '/'

        if not self.filename:
            warnings.simplefilter('ignore', 'tempnam')
            self.filename = tempfile.mkstemp(dir=tempfile.gettempdir(), prefix='s3du_')[1]


    def list_buckets(self):
        if self.bucket:
            return [self.bucket]
        tmp = self.s3.list_buckets()
        return [b['Name'] for b in tmp['Buckets']]

    def cache_files(self):
        if os.path.exists(self.csv_name) and not self.nocache:
            if time.time() - os.stat(self.csv_name).st_mtime < 3600:
                print(f'Using file list from cache: {self.csv_name}')
                return

        with open(self.csv_name, 'w') as f:
            writer = csv.writer(f)
            count = 0

            buckets = self.list_buckets()
            for bucket in buckets:
                if self.verbose:
                    print(f'Listing bucket {bucket}, found {count} files already...')

                args = {'Bucket': bucket, 'MaxKeys': 1000, 'Prefix': self.prefix}
                uri = f's3://{bucket}/{self.prefix}'
                while True:
                    res = self.s3.list_objects_v2(**args)
                    for item in res['Contents']:
                        writer.writerow([bucket, item['Key'], item['Size'], item['StorageClass']])
                        count += 1

                    if 'NextContinuationToken' in res:
                        args['ContinuationToken'] = res['NextContinuationToken']
                        if self.verbose:
                            print(f'Listing bucket {uri}, found {count} files already...')
                    else:
                        break

    def list_files(self):
        """Lists all files and saves then in the cache file."""
        files = []

        with open(self.csv_name, 'r') as f:
            reader = csv.reader(f)
            for (bucket, key, size, sclass) in reader:
                if self.storage_class and self.storage_class != sclass:
                    continue

                path = '/' + bucket + '/' + key
                size = int(size)
                files.append((path, size))

        return files

    def parse_list(self, files):
        tree = {'dirs': {}, 'files': []}

        for (path, size) in files:
            path = path.split("/")
            fname = path.pop()

            r = tree
            for em in path:
                if em not in r['dirs']:
                    r['dirs'][em] = {'dirs': {}, 'files': []}
                r = r['dirs'][em]

            r['files'].append((fname, size))

        return tree

    def convert_tree(self, tree):
        ncdu = [1, 0, {
            "timestamp": int(time.time()),
            "progver": "0.1",
            "progname": "s3du",
        }]

        ncdu.append(self.convert_branch(tree['dirs'][''], 'S3'))
        return ncdu

    def convert_branch(self, branch, name):
        res = []
        res.append({'name': name or '(unnamed)'})

        for k, v in branch['dirs'].items():
            res.append(self.convert_branch(v, k))

        for (fname, size) in branch['files']:
            res.append({
                'name': fname or '(unnamed)',
                'dsize': size,
            })

        return res

    def main(self):
        self.cache_files()  # list files and write ~/.cache/s3du-cache.csv

        files = self.list_files()
        tree = self.parse_list(files)
        ncdu = self.convert_tree(tree)

        with open(self.filename, "w") as f:
            f.write(json.dumps(ncdu))

        subprocess.Popen(['ncdu', '-f', self.filename]).wait()

        if not self.keep_file:
            os.unlink(self.filename)

        print(f'Found objects of classes: {", ".join(self.classes)}.')


def main():
    parser = argparse.ArgumentParser('s3du - ncdu for S3')

    parser.add_argument('-b', '--bucket', help='Target just this bucket, rather than all available')
    parser.add_argument('-p', '--prefix', help='Limit search under this prefix')
    parser.add_argument('-i', '--interactive', action='store_true', help='Interactive, run ncdu')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose Mode')
    parser.add_argument('-c', '--storage-class', choices=STORAGE_CLASSES, help='Show only this storage class')
    parser.add_argument('-n', '--no-cache', action='store_true', help='Don\'t leverage the cache file')
    parser.add_argument('-f', '--filename', help='Output filename')

    args = parser.parse_args()
    s3du(args).main()


if __name__ == '__main__':
    main()
