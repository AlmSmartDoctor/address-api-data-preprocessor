import pandas as pd
import os
import csv

import postgres

def convert_to_dict():
    global input_lists

    input_lists = list(set(input_lists))
    for address in input_lists:
        sep = address.split(',')
        result_dict['시도'].append(sep[0])
        result_dict['시군구'].append(sep[1])
        result_dict['읍면동'].append(sep[2])

    print(input_lists[0])


def make_address(sido, sigungu, eupmyeondong):
    address = sido

    if (sigungu == ""):
        address += ",None"
    elif (sido != sigungu):
        address += "," + sigungu
    else:
        address += ",None"

    if (eupmyeondong != ""):
        address += "," + eupmyeondong
    else:
        address += ",None"

    return address


def parsing(line):
    global header
    global input_lists
    global result_dict

    addressDict = {key: value for key, value in zip(header, line)}
    zipcode = addressDict["우편번호"]
    sido = addressDict["시도"]
    sigungu = addressDict["시군구"]
    eupmyeondong = addressDict["읍면"]
    if (eupmyeondong == ""):
        eupmyeondong = addressDict["법정동명"]

    input_lists.append(make_address(sido, sigungu, eupmyeondong))


def read_file():
    global header
    global input_lists
    global result_dict

    folder_path = "/Users/ggona/Downloads/zipcode_DB"
    all_list = os.listdir(folder_path)
    file_list = [file for file in all_list if file.endswith(".txt")]
    result_dict = {
        '시도': [],
        '시군구': [],
        '읍면동': []
    }
    input_lists = []
    for file in file_list:
        with open(folder_path + "/" + file, encoding="utf-8-sig") as fileData:
            csv_reader = list(csv.reader(fileData, delimiter='|'))
            header = csv_reader[0]
            for line in csv_reader[1:]:
                parsing(line)
        print(file)

def df_to_list(df):
    global sido_list
    global sigungu_list
    global eupmyeondong_list

    sido_list = list(set(df['시도']))

    sigungu = df[['시도', '시군구']]
    sigungu_list = sigungu.values.tolist()

    sigungu = []
    for i in sigungu_list:
        sigungu.append(tuple(i))

    sigungu = list(set(sigungu))
    sigungu_list = []
    for i in sigungu:
        sigungu_list.append(list(i))

    eupmyeondong_list = df.values.tolist()

    eupmyeondong = []
    for i in eupmyeondong_list:
        eupmyeondong.append(tuple(i))

    eupmyeondong = list(set(eupmyeondong))
    eupmyeondong_list = []
    for i in eupmyeondong:
        eupmyeondong_list.append(list(i))

    print(len(sido_list))
    print(len(sigungu_list))
    print(len(eupmyeondong_list))

if __name__ == "__main__":
    read_file()
    convert_to_dict()
    df = pd.DataFrame(result_dict)
    df_to_list(df)

    pg = postgres.Database()

    for data in sido_list:
        pg.insert_db(table='upper_autonomy', colum='upper_autonomy_name', data=data)

    for data in sigungu_list:
        pg.insert_db(table='lower_autonomy', colum=['upper_autonomy_name', 'lower_autonomy_name'], data=data)

    for data in eupmyeondong_list:
        pg.insert_db(table='town', colum=['upper_autonomy_name', 'lower_autonomy_name', 'town_name'], data=data)


