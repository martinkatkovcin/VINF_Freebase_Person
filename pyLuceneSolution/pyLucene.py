import os, sys, glob
import lucene

input_file = '../data/final_dataset.csv'
index_output = 'index'

directory = lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
analyzer = lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)


def get_data():
    data = []
    with open('data.txt') as f:
        for line in f:
            line = line.decode('utf-8').strip()
            num, name = line.split('\t')
            data.append((num, name))
    return data

