import os
import pandas as pd
import csv


def csv_to_dic(file_directory):
	csv_data = pd.read_csv(file_directory,encoding= 'gb18030') # ,iterator=True)
	csv_data.columns = ['key', 'value']
	dic = dict(zip(csv_data['key'].tolist(), csv_data['value'].tolist()))
	return dic

def file_to_list(file_name):
    try:
        list_out = []
        with open( file_name,"r",encoding= 'utf-8') as file1:
            fl = file1.readlines()
            for row_num in range(len(fl)):
                list_out.append(fl[row_num].strip())
        return list_out
    except:
        try:
            list_out = []
            with open(file_name, "r", encoding='gb18030') as file1:
                fl = file1.readlines()
                for row_num in range(len(fl)):
                    list_out.append(fl[row_num].strip())
            return list_out
        except:
            print('Cannot open', file_name)
            return []


for file_name in list_of_dic_file:
    dic_calcu[file_name] = {}
    file_open =  file_to_list(os.path.join(a2,file_name))
    list_topic = []
    list_value = []
    for trp in file_open:
        list_topic.append(trp.split(',')[0])
        list_value.append(float(trp.split(',')[1]))
    sum_value = sum(list_value)
    list_percent = [i/sum_value for i in list_value]
    print(len(list_topic),len(list_value),len(list_percent))
    for num in range(len(list_percent)):
        dic_calcu[file_name][list_topic[num]] = [list_value[num],list_percent[num]]

def calulate_csv_build(input_dic,topic_num = 20,pct_type = True):
    with open('precise_{num}.csv'.format(num=topic_num), "w", encoding='gb18030', newline='') as csv_wr:
        csv_write = csv.writer(csv_wr)
        # first_line = ['topic_type']
        # num_first = 1
        if pct_type:
            write_type = 1
        else:
            write_type = 0
        # while num_first <= topic_num+1:
        # first_line.append(num_first)
        # csv_write.writerows(first_line)
        for topic_name in input_dic.keys():
            write_line = [topic_name]
            for num in range(1, topic_num + 1):
                if str(num) in input_dic[topic_name].keys():
                    write_line.append(input_dic[topic_name][str(num)][write_type])
                else:
                    write_line.append('0')
            write_Str = [str(fl) for fl in write_line]
            csv_write.writerow(write_Str)


if __name__ == '__main__':
    a2 = r'F:\PyCharm_project\short_Video_title_classify\dic_reulst'
    list_of_dic_file = os.listdir(a2)
    file_name = list_of_dic_file[1]
    fi_1 = file_to_list(os.path.join(a2, file_name))
    dic_calcu = {}
    calulate_csv_build(dic_calcu,topic_num = 20,pct_type = True)


