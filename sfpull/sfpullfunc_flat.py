import os
import pandas as pd


def sffunc_flat():
    sf_q_results = {}
    dirpath = 'dataflow/1datasources/soql'
    for file in os.listdir(dirpath):
        if file != 'hist':
            sfi = os.path.splitext(file)[0]
            sf_q_results[sfi] = pd.read_csv(dirpath + '/' + file, encoding='ISO-8859-1')
    return sf_q_results
