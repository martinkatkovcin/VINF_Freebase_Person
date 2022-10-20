from parser import parser
import json
import os

q = input('Do you want to regenerate dataset? (y/n)\n')

if q.lower() == 'y':
    print(f'Files in the data directory:\n'
          f'{"  ".join(os.listdir("data/"))}')
    dataset = input('Enter the name of the dataset\n')
    jsonLocation, searchJsonData = parser(dataset)
elif q.lower() == 'n':
    searchJsonFile = open(f'jsonData/freebase-head-1000000_Person.json', 'r')
    searchJsonData = json.load(searchJsonFile)
    searchJsonFile.close()
else:
    print('Wrong input, we are not regenerating the dataset')

task = ['0. Exit program',
        '1. Search people based by year',
        '2. Search people based by name',
        '3. Search people higher than 175 centimeters',
        '4. Search people based by their age and higher than centimeters']

"""
Switch option to make it more clear in the execution
"""


def switch(searchOption: int) -> None:
    counter = 0
    flag = False

    if searchOption == 1:
        year = int(input('Enter date of birth (year) of people you want to find\n'))

        for key in searchJsonData:
            try:
                for element in list(searchJsonData[key].keys()):
                    if element == 'type.object.name':
                        flag = True

                if year == int((searchJsonData[key]['people.person.date_of_birth'][0])[0:4]):
                    counter = counter + 1
                    if flag:
                        print(f'{searchJsonData[key]["type.object.name"][0][0:len(searchJsonData[key]["type.object.name"][0]) - 3]} was born at {year}.')
                    else:
                        print(f'{key} was born at {year}.')
                    flag = False
            except:
                flag = False
                pass

        print(f'There were {counter} people found.')

    elif searchOption == 2:
        name = input('Enter name for a person you would like to find\n')
        flag = False

        for key in searchJsonData:
            try:
                if name == (searchJsonData[key]['type.object.name'][0])[0:len(name)]:
                    counter = counter + 1
                    print(f'{name} was found in the dataset!\n')
                    flag = True
                    break
            except:
                pass

        if not flag:
            print(f'{name} was not found in the dataset!\n')

    elif searchOption == 3:
        for key in searchJsonData:
            try:
                for element in list(searchJsonData[key].keys()):
                    if element == 'type.object.name':
                        flag = True

                if int((searchJsonData[key]['people.person.height_meters'][0]).replace('.', '')) >= 175:
                    counter = counter + 1

                    if flag:
                        print(f'{searchJsonData[key]["type.object.name"][0][0:len(searchJsonData[key]["type.object.name"][0]) - 3]} is higher than 175 centimeters '
                              f'({int((searchJsonData[key]["people.person.height_meters"][0]).replace(".", ""))} cm)')
                    else:
                        print(f'{key} is higher than 175 centimeters '
                              f'({int((searchJsonData[key]["people.person.height_meters"][0]).replace(".", ""))} cm)')
                    flag = False
            except:
                flag = False
                pass

        print(f'There were {counter} people found.')

    elif searchOption == 4:
        year = int(input('Enter year of birth for people you want to find\n'))
        height = int(input('Enter height for people in centimeters  you want to filter, that are higher\n'))

        for key in searchJsonData:
            try:
                for element in list(searchJsonData[key].keys()):
                    if element == 'type.object.name':
                        flag = True

                if int((searchJsonData[key]['people.person.height_meters'][0]).replace('.', '')) >= height:
                    if year == int((searchJsonData[key]['people.person.date_of_birth'][0])[0:4]):
                        counter = counter + 1

                        if flag:
                            print(f'{searchJsonData[key]["type.object.name"][0][0:len(searchJsonData[key]["type.object.name"][0]) - 3]} is higher than {height} centimeters '
                                  f'({int((searchJsonData[key]["people.person.height_meters"][0]).replace(".", ""))} cm) '
                                  f'and was born in {int((searchJsonData[key]["people.person.date_of_birth"][0])[0:4])}')
                        else:
                            print(f'{key} is higher than {height} centimeters '
                                  f'({int((searchJsonData[key]["people.person.height_meters"][0]).replace(".", ""))} cm) '
                                  f'and was born in {int((searchJsonData[key]["people.person.date_of_birth"][0])[0:4])}')
                        flag = False

            except:
                flag = False
                pass

        print(f'There were {counter} people found.')
    else:
        print("Wrong input, please, try again!")


"""
Execution of the program for the user
By their choice of the option, they are able to filter
People, that meet the criteria of the task
"""
while True:
    print('\n'.join(task))

    try:
        searchOption = int(input("Choose your search option\n"))

        if searchOption == 0:
            break

        switch(searchOption)
    except:
        print('Bad input, try again, use numbers')
