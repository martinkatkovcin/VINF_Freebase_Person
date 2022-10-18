#import lucene
import re
import json
#import gzip
import timeit

"""
Dataset variables
"""
notGz = ['data/freebase-head-1000000', 'data/freebase-head-10000000', 'data/freebase-entity-person']
freebaseLatest = 'data/freebase-rdf-latest.gz'
freebaseHead1mil = 'data/freebase-head-1000000'
freebaseHead10mil = 'data/freebase-head-10000000'
freebaseEntityPerson = 'data/freebase-entity-person'

# Replace the name of the final dataset to be processed to json
dataset = freebaseHead1mil

# if dataset not in notGz:
#    # Not working (deadlock), this .gz file will be processed by PySpark
#
#    with gzip.open(dataset, 'rt') as file:
#        """
#        Read every line and store in the array (triplets)
#        Every line stores 3 information
#        Read from gz
#        """
#        triplets_array = file.readlines()
# else:

if dataset in notGz:
    with open(dataset) as file:
        """
        Read every line and store in the array (triplets)
        Every line stores 3 information
        Read from file
        """
        triplets_array = file.readlines()


"""
Store every line as a tuple of 3 and gather them in the array
Created pattern (regex expression) to have better understanding data
"""
triplets = []
pattern = '(http:\/\/rdf.freebase.com\/ns)|(http:\/\/www.w3.org\/[0-9]*\/[0-9]*\/[0-9]*-*)|(\t\.\n)'
dateRegex = '()'

for element in triplets_array:
    preprocessingPattern = re.sub(pattern, '', element)
    preprocessingPattern = re.sub('[<>[/]', '', preprocessingPattern).split('\t')
    triplets.append(tuple(preprocessingPattern))

"""
To have simplier way to store the information from tuple
is to store tuples in the dictionory, that can be easily
converted to json dumps with clear format
"""

json_dict = dict()
entityPersonIds = list()
flag = False
id_actual = ''

start = timeit.default_timer()

for id, type, value in triplets:

    if id_actual != id and len(json_dict) != 0:
        if not flag:
            json_dict.pop(id_actual)

        flag = False

    id_actual = id

    # If dict key with value ID doesn't exist, create the dictionary
    if id not in json_dict:
        json_dict[id] = {}

    # If dict key type doesn't exist, create the array, where we can append all types
    if type not in json_dict[id]:
        json_dict[id][type] = []

    #type = re.sub("(\w+\.)+(\w+)", r"\2", type)
    value = re.sub("\\\"([-]?(\d+-?)+)\\\".+XMLSchema.+", r"\1", value)
    value = re.sub("\"(.*?)\"", r"\1", value)
    json_dict[id][type].append(value)

    if value == 'people.person':
        flag = True


end = timeit.default_timer()
print(f'Preprocessed data in {round(end-start, 2)} seconds')

"""
Save json file format to .json format
"""
jsonLocation = re.sub('(data/)', '', dataset)
file = open(f'jsonData/{jsonLocation}_Person.json', 'w')
file.write(json.dumps(json_dict, indent = 4))
file.close()

# Load json file, that we created, and loaded for search
searchJsonFile = open(f'jsonData/{jsonLocation}_Person.json', 'r')
task = ['0. Exit program',
        '1. Search people based by year',
        '2. Search people based by name']

"""
Switch option to make it more clear in the execution
"""
def switch(searchOption: int) -> None:

    if searchOption == 1:
        pass
    elif searchOption == 2:
        pass
    else:
        print("Wrong input, please, try again!")

"""
Execution of the program for the user
By their choice of the option, they are able to filter
People, that meet the criteria of the task
"""
while True:
    print('\n'.join(task))
    searchOption = int(input("Choose your search option\n"))

    if searchOption == 0:
        searchJsonFile.close()
        break

    switch(searchOption)
