import os
import pandas as pd
import csv

def get_files(file_dir):
    list_fife = []
    for root, dirs, files in os.walk(file_dir):
        for name in files:
            if name.endswith('py') or name.endswith('css') or name.endswith('html') or name.endswith('js') or name.endswith('vue') or name.endswith('sh'):
                list_fife.append(os.path.join(root, name))
    return list_fife

def open_file(file_name):
    #if file_name.endswith('py') or file_name.endswith('css') or file_name.endswith('html'):
    try:
        data = pd.read_table(file_name)
        list_out = data.values.T.tolist()[0]
        #list_out.insert(0,file_name.replace(project_dir , ''))
        list_out.append('\n')
        return list_out
    except:
        try:
            list_out = []
            with open(file_name,"r", encoding= 'utf-8') as file1:
                #list_out.append(file_name.replace(project_dir, '') + '\n')
                list_out.append('\n')
                for row in file1.readlines():
                    list_out.append(row)
                list_out.append('\n')
            return list_out
        except:
            print('camnt',file_name)
            return []

def build_input_col_num(input_num):
    output = "          "
    rep = len(str(input_num))
    return  str(input_num)+output[rep:]

if __name__ == '__main__':
    max_num =  178388
    front_file = 'front_file.txt'
    last_file  = 'last_file.txt'
    with open(r'{a}_front_40_col_num.txt'.format(a=front_file.split('.')[0]),'w',newline= '', encoding='gb18030') as file_witre:
        num_front = 1
        list_for_write = open_file(front_file)
        for write_line in list_for_write:
            file_witre.write(build_input_col_num(num_front) +str(write_line)+ '\n')#+ '\n'
            num_front += 1
        print(front_file,num_front)

    with open(r'{a}_last_40_col_num.txt'.format(a=last_file.split('.')[0]  ),'w', newline= '', encoding='gb18030') as file_witre:
        list_for_write = open_file(last_file)
        len_list = len(list_for_write)
        num_last = max_num - len_list + 1
        for write_line in list_for_write:
            file_witre.write(build_input_col_num(num_last) +str(write_line)+ '\n')
            num_last += 1
        print(last_file,num_last)


