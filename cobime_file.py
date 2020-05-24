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
        list_out.insert(0,file_name.replace(project_dir , ''))
        list_out.insert(1,'\n')
        list_out.append('\n')
        return list_out
    except:
        try:
            list_out = []
            with open(file_name,"r", encoding= 'utf-8') as file1:
                list_out.append(file_name.replace(project_dir, '') + '\n')
                list_out.append('\n')
                for row in file1.readlines():
                    list_out.append(row)
                list_out.append('\n')
            return list_out
        except:
            #print('camnt',file_name)
            return []

#open_file(r'F:\dataManagementSys\dataManagementSys\settings.py')
#data = open_file(r'F:\dataManagementSys\dataManagementSys\settings.py')

if __name__ == '__main__':
    project_dir = r"F:\Winscp_dir\Ccidx_project" #需要统计的文件夹
    all_file_list = get_files(project_dir)
    write_name_front = '{a}front.txt'.format(a=project_dir)
    write_name_service = '{a}service.txt'.format(a=project_dir)
    num_limit =40000 #当代码到一定行数就不统计了


    with open(write_name_front.split('\\')[-1],'w',newline= '', encoding='gb18030') as file_witre:
        num_front = 0
        for name in all_file_list:
            if num_front <=num_limit:
                if name.endswith('css') or name.endswith('html') or name.endswith('js') or name.endswith('vue'):
                    #print('1')
                    list_for_write = open_file(name)
                    for write_line in list_for_write:
                        file_witre.write(str(write_line)  + '\n')
                    num_front += len(list_for_write)

    with open(write_name_service.split('\\')[-1], 'w',newline= '', encoding='gb18030') as file_wite1:
        num_back = 0
        for name in all_file_list:
            if num_back <= num_limit:
                if name.endswith('py') or name.endswith('sh'):
                    print(name.replace(project_dir,''))
                    list_for_write = open_file(name)
                    for write_line in list_for_write:
                        file_wite1.write(str(write_line) + '\n')
                    num_back += len(list_for_write)

    print(write_name_front, num_front,write_name_service , num_back,num_front + num_back)


