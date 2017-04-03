from glob import glob
import json
import os
import pandas as pd
import sys

saved_to = os.sep.join(['.', 'data'])
input_dir = os.sep.join(['.', 'data', '*json'])

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def split_data(data):
		temp = []
		if isinstance(data, list):
			[temp.append(_) for _ in data]	
		else:
			temp.append(data)

		temp = [pd.DataFrame(flatten_json(_), index=[0]) for _ in temp]
		return temp

if __name__ == '__main__':
	data = []

	files = glob(input_dir)

	print "total json files : ", len(files)
	for file in files:
	# for file in ['data/908237.json']:
		with open(file) as f:
			data.extend(split_data(json.load(f)))


	data = pd.concat(data, ignore_index=True)
	data.to_csv("housing.csv", index=False, encoding="utf-8")
	print "saved data ", data.shape