import csv
import pandas as  pd
import os

def csv_to_list(file_directory):
    csv_data = pd.read_csv(file_directory, encoding='gb18030')
    train_data = np.array(csv_data)
    train_x_list = train_data.tolist()
    return train_x_list

def file_to_list(file_name):
    if file_name.endswith('csv'):
        try:
            with open(file_name, "r", encoding='utf-8') as csv_file:
                list_out = []
                csv_r =csv.reader(csv_file)
                for row in csv_r:
                    list_out.append(row)
                return list_out
        except:
            print('gb_csv')
            with open(file_name, "r", encoding='gb18030') as csv_file:
                list_out = []
                csv_r =csv.reader(csv_file)
                for row in csv_r:
                    list_out.append(row)
                return list_out
    else:
        try:
            list_out = []
            with open(file_name, "r", encoding='utf-8') as file1:
                for row in file1.readlines():
                    list_out.append(row)
            return list_out
        except:
            try:
                list_out = []
                with open(file_name, "r", encoding='gb18030') as file1:
                    for row in file1.readlines():
                        list_out.append(row)
                return list_out
            except:
                print('Can not open',file_name)
                return []

def ld_to_csv(input_dic,csv_directory,csv_name):
    with open( r'{dic_rectory}\{name}.csv'.format(dic_rectory=csv_directory ,name=csv_name),'w',newline='', encoding='gb18030') as csv_w:
        file =  csv.writer(csv_w)
        if type(input_dic).__name__ == 'dict':
            for key in input_dic.keys():
                list_write = []
                if type(input_dic[key]).__name__ =='list':
                    for write_value in input_dic[key]:
                        list_write.append(write_value)
                else:
                    list_write = [input_dic[key]]
                file.writerow(list_write)
        elif type(input_dic).__name__ == 'list':
            for key in input_dic:
                list_write = []
                for write_value in key:
                    list_write.append(write_value)
                file.writerow(list_write)


