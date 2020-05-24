import json,re
import datetime, copy
from elasticsearch import Elasticsearch
try:
    from write_data_into_es.func_get_releaser_id import *
except:
    from .func_get_releaser_id import *
import redis
hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)
es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)
index = 'short-video-production'
doc_type = 'daily-url'
pool = redis.ConnectionPool(host='192.168.17.60', port=6379, db=2,decode_responses=True)
rds = redis.Redis(connection_pool=pool)


today = datetime.datetime.now()
first_day = datetime.datetime(today.year, today.month, 1)
day_before_first_day = first_day - datetime.timedelta(1)
l_month = day_before_first_day.month
l_year = day_before_first_day.year
count = 0


def parse_line_dict(line, line_dict,blank_space_error,new_line_error,err_id_line):
    for k in line_dict:
        try:
            if " " in line_dict[k]:
                blank_space_error = blank_space_error + str(line + 2) + ","
            if "\r" in line_dict[k]:
                new_line_error = new_line_error + str(line + 2) + ","
            if "\n" in line_dict[k]:
                new_line_error = new_line_error + str(line + 2) + ","
            if "\t" in line_dict[k]:
                new_line_error = new_line_error + str(line + 2) + ","
            line_dict[k] = line_dict[k].replace("\r", "").replace("\n", "").replace("\t", "").replace(" ", "")
            try:
                if k == "releaserUrl":
                    line_dict[k] = re.findall(r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+~]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
                               line_dict[k])[0]
            except Exception as e:
                print(e)
                err_id_line = err_id_line + str(line + 2) + ","
        except Exception as e:
            print(e)
            continue
    return line_dict,blank_space_error,new_line_error,err_id_line

def write_to_es(file, push_to_redis=True,**kwargs):
    """

    :param file:
    :param kwargs: not_push_to_redis = True 不push到redis中
    :return:
    """
    bulk_all_body = ""
    err_id_line = ""
    blank_space_error = ""
    new_line_error = ""
    error_msg_list = []
    count = 0

    try:
        f = open(file, 'r', encoding="gb18030")
        head = f.readline()
        head_list = head.strip().split(',')
    except:
        f = file
    for line,i in enumerate(f):
        if type(file) != list:
            try:
                line_list = i.strip().split(',')
                line_dict = dict(zip(head_list, line_list))
            except:
                line_dict=f
        else:
            line_dict = i
        # print(i)

        try:
            platform = line_dict['platform']
        except:
            new_line_error += str(line + 2) + ","
            continue
        line_dict, blank_space_error, new_line_error, err_id_line = parse_line_dict(line, line_dict, blank_space_error,
                                                                                    new_line_error, err_id_line)
        if "" in line_dict:
            line_dict.pop("")
        try:
            releaserUrl = line_dict['releaserUrl']
            if platform == 'new_tudou':
                if releaserUrl[-2:] == '==':
                    releaserUrl = releaserUrl + '/videos'
                    line_dict['releaserUrl'] = releaserUrl
        except:
            pass

        line_dict["releaser_id"] = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
        if line_dict["releaser_id"]:
            doc_id = line_dict['platform'] + '_' + line_dict['releaser_id']
        else:
            doc_id = line_dict['platform'] + '_' + line_dict['releaser']
            err_id_line += str(line + 2) + ","
        find_exist = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"term": {"_id": doc_id}}
                                ]
                        }
                }
        }
        search_re = es.search(index='target_releasers', doc_type='doc', body=find_exist)
        if search_re['hits']['total'] > 0:
            search_source = search_re['hits']['hits'][0]['_source']
            temp_dict = copy.deepcopy(line_dict)
            line_dict.update(search_source)
            line_dict.update(temp_dict)
            if 'frequency' in search_source and 'Nov_2018' in search_source:
                # print(search_source)
                if kwargs.get("extra_dic"):
                    line_dict.update(kwargs.get("extra_dic"))
            else:
                search_releaser = {
                        "query": {
                                "bool": {
                                        "filter": [
                                                {"term": {"releaser": line_dict['releaser']}},
                                                {"term": {"platform": line_dict['platform']}},
                                                {"term": {"data_year": l_year}},
                                                {"term": {"data_month": l_month}},
                                                {"term": {"stats_type": "new_released"}},
                                        ]
                                }
                        }
                }

                search_re_vm = es.search(index='releaser', doc_type='releasers', body=search_releaser)
                if search_re_vm['hits']['total'] > 0:
                    print('find')
                    video_num = search_re_vm['hits']['hits'][0]['_source']['video_num']
                    if video_num > 300:
                        line_dict['frequency'] = 3
                        line_dict['Nov_2018'] = video_num
                    else:
                        line_dict['frequency'] = 1
                        line_dict['Nov_2018'] = video_num
                else:
                    line_dict['frequency'] = 1
                    line_dict['Nov_2018'] = 0

        else:
            search_releaser = {
                    "query": {
                            "bool": {
                                    "filter": [
                                            {"term": {"releaser": line_dict['releaser']}},
                                            {"term": {"platform": line_dict['platform']}},
                                            {"term": {"data_year": l_year}},
                                            {"term": {"data_month": l_month}},
                                            {"term": {"stats_type": "new_released"}},
                                    ]
                            }
                    }
            }

            search_re_vm = es.search(index='releaser', doc_type='releasers', body=search_releaser)
            if search_re_vm['hits']['total'] > 0:
                print('find')
                video_num = search_re_vm['hits']['hits'][0]['_source']['video_num']
                if video_num > 300:
                    line_dict['frequency'] = 3
                    line_dict['Nov_2018'] = video_num
                else:
                    line_dict['frequency'] = 1
                    line_dict['Nov_2018'] = video_num
            else:
                line_dict['frequency'] = 1
                line_dict['Nov_2018'] = 0
        if line_dict.get("post_time"):
            pass
        else:
            line_dict['post_time'] = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        line_dict['timestamp'] = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        try:
            line_dict["releaser_id"] = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
            line_dict["releaser_id_str"] = line_dict["platform"] + "_" + line_dict["releaser_id"]
            line_dict["is_valid"] = "true"

        except:
            line_dict["releaser_id"] = ""
            line_dict["releaser_id_str"] = ""
            line_dict["is_valid"] = "false"
        if kwargs.get("post_by"):
            line_dict["post_by"] = kwargs.get("post_by")
        # try:
        #     line_dict.pop("平台账号主页URL")
        # except:
        #     pass
        production = line_dict.get("production_org_category")
        if production:
            if "SMG" in production:
                line_dict["SMG"] = "True"
            elif "BTV" in production:
                line_dict["BTV"] = "True"
            line_dict.pop("production_org_category")
        if kwargs.get("extra_dic"):
            line_dict.update(kwargs.get("extra_dic"))

        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        if push_to_redis:
            rds.lpush("releaser_doc_id_list", doc_id)
        data_str = json.dumps(line_dict, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        #        print(bulk_one_body)
        bulk_all_body += bulk_one_body
        count = count + 1
        if count % 500 == 0:
            eror_dic = es.bulk(index='target_releasers', doc_type='doc',
                               body=bulk_all_body)
            try:
                if line_dict['frequency'] >= 3:
                    eror_dic = es.bulk(index='target_releasers_org', doc_type='doc',
                                       body=bulk_all_body)
            except:
                pass
            bulk_all_body = ''
            if eror_dic['errors'] is True:
                print(eror_dic)
    if bulk_all_body != '':
        eror_dic = es.bulk(body=bulk_all_body,
                           index='target_releasers',
                           doc_type='doc',
                           )
        try:
            if line_dict['frequency'] >= 3:
                eror_dic = es.bulk(index='target_releasers_org', doc_type='doc',
                                   body=bulk_all_body)
        except:
            pass
        if eror_dic['errors'] is True:
            print(eror_dic)
    error_msg_list.append("%s条 写入成功" % count)
    if err_id_line:
        error_msg_list.append("第%s行 releaserUrl错误" % err_id_line[:-1])
    if blank_space_error:
        error_msg_list.append("第%s行 发现存在空格" % blank_space_error[:-1])
    if new_line_error:
        error_msg_list.append("第%s行 发现存在换行符" % new_line_error[:-1])
    return error_msg_list


if __name__ == "__main__":
    file = r'D:\work_file\4月补数据1.csv'
    extra_dic = {
            # "week_report": "True",
            # 'key_releaser': "True",
            # "an_hui":"True",
            # 'frequency': 3
    }
    csv_type = {"SMG": [], "an_hui": [], "ronghe": [], "su_zhou": []}
    write_to_es(file, extra_dic=extra_dic, post_by="litao")
