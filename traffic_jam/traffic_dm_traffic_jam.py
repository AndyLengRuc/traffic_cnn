#!/usr/bin/python  
#coding:utf-8  

"""
@Function: 统计道路拥堵信息，并保存在知识库
@author: Leng 
@contact: lengyoufang@163.com 
@software: PyCharm 
@file: traffic_dm_traffic_jam.py
@time: 2018/9/24 20:29 
"""
# system module
from time import sleep
from datetime import datetime, date, time, timedelta
import pymongo
import psycopg2
import pandas as pd
import logging
import requests
import traceback
import json

# self defined module

# parameters
module_name = "dm_traffic_jam"
amap_api_key_dict = {'db6b6695b77f2662dba6dc0264dfe012': 0, '929492a93387f8b0dc2097563e941e04': 0, '3d283c5ada64139ecca66e4586be1e4d': 0, '355a0d633c580e76df42e3498e8b3833': 0, '667166c9d5a62acf15e53b9285da9eb6':0}   # key means key, values means error retried.
amap_api_driving_url = "https://restapi.amap.com/v3/direction/driving"
mongodb_host = 'mongodb://admin:123456@192.168.0.117:27017/admin'
mongodb_name = 'zk_traffic'
mongodb_col_jam = 'di_traffic_jam_driving'
mongodb_col_poi = 'di_poi'
db_share_data = {'db_host': '120.77.204.22', 'db_port': 5999, 'db_user': 'readonly', 'db_pass': '123456', 'db_name': 'share_data'}
mongodb_conn_jam = pymongo.MongoClient(mongodb_host)[mongodb_name][mongodb_col_jam]
mongodb_conn_poi = pymongo.MongoClient(mongodb_host)[mongodb_name][mongodb_col_poi]

# common untils
def get_logger(module_name):
    log_file = "./logs/" + "" + module_name + ".log"  # log file
    log_format = '%(asctime)s -%(name)s-%(levelname)s-%(module)s: %(message)s'
    fomatter = logging.Formatter(log_format)
    logger = logging.getLogger(module_name)   # initial a logger name
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()    # console handler
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fomatter)
    fh = logging.FileHandler(log_file)  # file handler
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fomatter)
    logger.addHandler(fh)   # add console handler
    logger.addHandler(ch)   # add file handler
    logger.propagate = False
    return logger
logger = get_logger(module_name)


def http_get(url, params):
    retry = 0
    retry_max = 10
    wait_second = 1
    timeout = 5
    result = {}
    while True:
        try:
            response = requests.get(url, params=params, timeout=timeout)
            result = response.json()
            print(result)
            break
        except:
            retry += 1
            print("[url request error], retry count:" + str(retry) + ' ************ url **********:' + url)
            print(traceback.format_exc())
            if retry > retry_max:
                break
            else:
                sleep(retry * wait_second)
                continue
    return result

def init_USER_DEFINED_DB_CONN(db):
    conn = psycopg2.connect(database=db['db_name'], user=db['db_user'], password=db['db_pass'], host=db['db_host'],
                            port=db['db_port'])
    conn.autocommit = True
    return conn

def get_all_gov_df():
    conn_share = init_USER_DEFINED_DB_CONN(db_share_data)
    sql_sel = "SELECT * FROM dict_gov_lib_view ORDER BY gov_code ASC"
    all_govs = pd.read_sql(sql=sql_sel, con=conn_share, index_col="gov_code")
    conn_share.close()
    return all_govs
all_govs = get_all_gov_df()

def stats_all_gov_road_status_by_time_range(start_time, end_time):
    """
    统计早高峰时段各种道路的统计指标，并写入知识库
    :param start_time:
    :param end_time:
    :return:
    """
    global all_govs
    logger.info("===========> Start to statistic all road status from {start_time} to {end_time}".format(start_time=start_time, end_time=end_time))
    version = start_time.split(" ")[0]
    hour = int(start_time[11:13])
    if hour >= 5 and hour <=10:
        period = 'morning'
    elif hour >=17 and hour <= 21:
        period = 'afternoon'
    traffic_metas = [
            {"module": "traffic", "period": "morning", "parameter": "road_count", "type_code": "8510010201", "type_name": "[拥堵]早高峰采样的道路条数", "data_unit": "条", "category_big": "交通建设", "category_mid": "拥堵", "category_sub": "道路总条数", "description": "本次早高峰采样的道路条数", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "total_distance", "type_code": "8510010202", "type_name": "[拥堵]早高峰采样的道路总长度", "data_unit": "千米", "category_big": "交通建设", "category_mid": "拥堵", "category_sub": "道路总长度", "description": "本次早高峰采样的道路总长度，单位为千米", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "min_speed", "type_code": "8510010203", "type_name": "[拥堵]早高峰(7:00-10:00)最慢驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)最慢驾车速度", "description": "区域内早高峰(7:00-10:00)最慢驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "max_speed", "type_code": "8510010204", "type_name": "[拥堵]早高峰(7:00-10:00)最快驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)最快驾车速度", "description": "区域内早高峰(7:00-10:00)最快驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "avg_speed", "type_code": "8510010205", "type_name": "[拥堵]早高峰(7:00-10:00)平均驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)平均驾车速度", "description": "区域内早高峰(7:00-10:00)平均驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "unknown_segment_ratio", "type_code": "8510010206", "type_name": "[拥堵]早高峰(7:00-10:00)未知路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)未知路段比例", "description": "区域内早高峰(7:00-10:00)未知路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "expedite_segment_ratio", "type_code": "8510010207", "type_name": "[拥堵]早高峰(7:00-10:00)畅通路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)畅通路段比例", "description": "区域内早高峰(7:00-10:00)畅通路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "congested_segment_ratio", "type_code": "8510010208", "type_name": "[拥堵]早高峰(7:00-10:00)缓行路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)缓行路段比例", "description": "区域内早高峰(7:00-10:00)缓行路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "blocked_segment_ratio", "type_code": "8510010209", "type_name": "[拥堵]早高峰(7:00-10:00)拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)拥堵路段比例", "description": "区域内早高峰(7:00-10:00)拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "morning", "parameter": "sblocked_segment_ratio", "type_code": "8510010210", "type_name": "[拥堵]早高峰(7:00-10:00)严重拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)严重拥堵路段比例", "description": "区域内早高峰(7:00-10:00)严重拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "road_count", "type_code": "8510010231", "type_name": "[拥堵]晚高峰采样的道路条数", "data_unit": "条", "category_big": "交通建设", "category_mid": "道路", "category_sub": "道路总条数", "description": "本次晚高峰采样的道路条数", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "total_distance", "type_code": "8510010232", "type_name": "[拥堵]晚高峰采样的道路总长度", "data_unit": "千米", "category_big": "交通建设", "category_mid": "道路", "category_sub": "道路总长度", "description": "本次晚高峰采样的道路总长度，单位为千米", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "min_speed", "type_code": "8510010233", "type_name": "[拥堵]晚高峰(17:00-20:00)最慢驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(17:00-20:00)最慢驾车速度", "description": "区域内晚高峰(17:00-20:00)最慢驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "max_speed", "type_code": "8510010234", "type_name": "[拥堵]晚高峰(17:00-20:00)最快驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(17:00-20:00)最快驾车速度", "description": "区域内晚高峰(17:00-20:00)最快驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "avg_speed", "type_code": "8510010235", "type_name": "[拥堵]晚高峰(17:00-20:00)平均驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(17:00-20:00)平均驾车速度", "description": "区域内晚高峰(17:00-20:00)平均驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "unknown_segment_ratio", "type_code": "8510010236", "type_name": "[拥堵]晚高峰(7:00-10:00)未知路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)未知路段比例", "description": "区域内晚高峰(7:00-10:00)未知路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "expedite_segment_ratio", "type_code": "8510010237", "type_name": "[拥堵]晚高峰(17:00-20:00)畅通路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)畅通路段比例", "description": "区域内晚高峰(7:00-10:00)畅通路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "congested_segment_ratio", "type_code": "8510010238", "type_name": "[拥堵]晚高峰(17:00-20:00)缓行路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)缓行路段比例", "description": "区域内晚高峰(7:00-10:00)缓行路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "blocked_segment_ratio", "type_code": "8510010239", "type_name": "[拥堵]晚高峰(17:00-20:00)拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)拥堵路段比例", "description": "区域内晚高峰(7:00-10:00)拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "sblocked_segment_ratio", "type_code": "8510010240", "type_name": "[拥堵]晚高峰(17:00-20:00)严重拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)严重拥堵路段比例", "description": "区域内早高峰(7:00-10:00)严重拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "tpi", "type_code": "8520010201", "type_name": "[拥堵]早高峰TPI指数(交通拥堵指数)", "data_unit": "指数值", "category_big": "拥堵", "category_mid": "TPI", "category_sub": "交通指数", "description": "交通指数是交通拥堵指数或交通运行指数（Traffic Performance Index，即“TPI”）的简称，是综合反映道路网畅通或拥堵的概念性指数值。相当于把拥堵情况数字化。交通指数取值范围为0～10，分为五级。其中0～2、2～4、4～6、6～8、8～10分别对应“畅通”、“基本畅通”、“轻度拥堵”、“中度拥堵”、“严重拥堵”五个级别，数值越高表明交通拥堵状况越严重。", "submitter": "leng"},
            {"module": "traffic", "period": "afternoon", "parameter": "tpi", "type_code": "8520010202", "type_name": "[拥堵]晚高峰TPI指数(交通拥堵指数)", "data_unit": "指数值", "category_big": "拥堵", "category_mid": "TPI", "category_sub": "交通指数", "description": "交通指数是交通拥堵指数或交通运行指数（Traffic Performance Index，即“TPI”）的简称，是综合反映道路网畅通或拥堵的概念性指数值。相当于把拥堵情况数字化。交通指数取值范围为0～10，分为五级。其中0～2、2～4、4～6、6～8、8～10分别对应“畅通”、“基本畅通”、“轻度拥堵”、“中度拥堵”、“严重拥堵”五个级别，数值越高表明交通拥堵状况越严重。", "submitter": "leng"},
            {"module": "traffic", "period": "fullday", "parameter": "tpi", "type_code": "8520010203", "type_name": "[拥堵]全天TPI(交通指数)", "data_unit": "指数值", "category_big": "拥堵", "category_mid": "TPI", "category_sub": "交通指数", "description": "交通指数是交通拥堵指数或交通运行指数（Traffic Performance Index，即“TPI”）的简称，是综合反映道路网畅通或拥堵的概念性指数值。相当于把拥堵情况数字化。交通指数取值范围为0～10，分为五级。其中0～2、2～4、4～6、6～8、8～10分别对应“畅通”、“基本畅通”、“轻度拥堵”、“中度拥堵”、“严重拥堵”五个级别，数值越高表明交通拥堵状况越严重。", "submitter": "leng"}
           ]
    traffic_metas_df = pd.DataFrame.from_records(data=traffic_metas)
    print("quering")
    rows = mongodb_conn_jam.aggregate([{"$match": {"ctime": {"$gt": start_time, "$lt": end_time}}},
                                       {"$group": {"_id": "$gov_id",
                                                   "max_speed": {"$max": "$speed"},
                                                   "min_speed": {"$min": "$speed"},
                                                   "avg_speed": {"$avg": "$speed"},
                                                   "road_count": {"$sum": 1},
                                                   "total_distance": {"$sum": "$distance"},
                                                   "distance_unknown": {"$sum": "$distance_unknown"},
                                                   "distance_expedite": {"$sum": "$distance_expedite"},
                                                   "distance_congested": {"$sum": "$distance_congested"},
                                                   "distance_blocked": {"$sum": "$distance_blocked"},
                                                   "distance_sblocked": {"$sum": "$distance_sblocked"}
                                                    }
                                        }])
    print("quering finished")
    row_id = 0
    road_count_dict = {}
    road_distance_dict = {}
    min_speed_dict = {}
    max_speed_dict = {}
    avg_speed_dict = {}
    unknown_segment_ratio = {}      # 未知路段比例
    expedite_segment_ratio = {}     # 畅通路段比例
    congested_segment_ratio = {}    # 缓行路段比例
    blocked_segment_ratio = {}      # 拥堵路段比例
    sblocked_segment_ratio = {}     # 严重拥堵路段比例，s代表severe(严重)
    for row in rows:
        row_id += 1
        gov_id = row.get("_id")
        road_count_dict[gov_id] = round(row.get("road_count"), 2)                                            # 采样道路总条数
        road_distance_dict[gov_id] = round(row.get("total_distance")/1000, 2)                                # 采样道路总长度
        min_speed_dict[gov_id] = round(row.get("min_speed"), 2)                                              # 最慢驾车速度
        max_speed_dict[gov_id] = round(row.get("max_speed"), 2)                                              # 最快驾车速度
        avg_speed_dict[gov_id] = round(row.get("avg_speed"), 2)                                              # 平均驾车速度
        unknown_segment_ratio[gov_id] = round(100 * row.get("distance_unknown")/row.get("total_distance"), 2)     # 未知路段比例，保留两位小数
        expedite_segment_ratio[gov_id] = round(100 * row.get("distance_expedite")/row.get("total_distance"), 2)   # 畅通路段比例，保留两位小数
        congested_segment_ratio[gov_id] = round(100 * row.get("distance_congested")/row.get("total_distance"), 2) # 缓行路段比例，保留两位小数
        blocked_segment_ratio[gov_id] = round(100 * row.get("distance_congested")/row.get("total_distance"), 2)   # 拥堵路段比例，保留两位小数
        sblocked_segment_ratio[gov_id] = round(100 * row.get("distance_congested")/row.get("total_distance"), 2)  # 严重拥堵路段比例，s代表severe(严重)，保留两位小数

    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'road_count'), "type_code"].values[0],
                        datas=json.dumps(road_count_dict), version=version)         # 采样道路总条数
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'total_distance'), "type_code"].values[0],
                        datas=json.dumps(road_distance_dict), version=version)      # 采样道路总条数
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'min_speed'), "type_code"].values[0],
                        datas=json.dumps(min_speed_dict), version=version)          # 最慢驾车速度
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'max_speed'), "type_code"].values[0],
                        datas=json.dumps(max_speed_dict), version=version)          # 最快驾车速度
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'avg_speed'), "type_code"].values[0],
                        datas=json.dumps(avg_speed_dict), version=version)          # 平均驾车速度
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'unknown_segment_ratio'), "type_code"].values[0],
                        datas=json.dumps(unknown_segment_ratio), version=version)   # 未知路段比例
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'expedite_segment_ratio'), "type_code"].values[0],
                        datas=json.dumps(expedite_segment_ratio), version=version)  # 畅通路段比例
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'congested_segment_ratio'), "type_code"].values[0],
                        datas=json.dumps(congested_segment_ratio), version=version) # 缓行路段比例
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'blocked_segment_ratio'), "type_code"].values[0],
                        datas=json.dumps(blocked_segment_ratio), version=version)   # 拥堵路段比例
    write_one_knowledge(type_code=traffic_metas_df.loc[(traffic_metas_df.period == period) & (traffic_metas_df.parameter == 'sblocked_segment_ratio'), "type_code"].values[0],
                        datas=json.dumps(sblocked_segment_ratio), version=version)  # 严重拥堵路段比例, s代表severe(严重)
    logger.info("===========> Finish to statistic all road status from {start_time} to {end_time}".format(start_time=start_time, end_time=end_time))


def write_one_knowledge(type_code, datas, version, submitter="leng"):
    knowledge_base_url = "http://192.168.0.88:9400/knowledgebase/"
    need_write_data = {}
    need_write_data["operation_type"] = "write"
    need_write_data["type_code"] = type_code
    need_write_data["datas"] = datas
    need_write_data["submitter"] = submitter
    need_write_data["version"] = version
    http_get(url=knowledge_base_url, params=need_write_data)
    logger.info("Success write knowledge base. type_code: {type_code}, version: {version}".format(type_code=type_code, version=version))


def stats_traffic_status_by_date(day=None):
    if day is None:
        # 如果不给定时间时，默认统计昨天的数据
        day = datetime.strftime(datetime.today() + timedelta(days=-1), "%Y-%m-%d")  # yesterday

    # 1. morning
    start_time = day + " 07:00:00"
    end_time = day + " 10:00:00"
    stats_all_gov_road_status_by_time_range(start_time=start_time, end_time=end_time)

    # 2. afternoon
    start_time = day + " 17:00:00"
    end_time = day + " 20:00:00"
    stats_all_gov_road_status_by_time_range(start_time=start_time, end_time=end_time)

if __name__ == "__main__":
    # stats_all_gov_road_morning_status_by_time_range(start_time="2018-09-29 07:00:00", end_time="2018-09-29 12:00:00")
    # stats_all_gov_road_afternoon_status_by_time_range(start_time="2018-09-29 17:00:00", end_time="2018-09-29 21:00:00")
    # stats_all_gov_road_status_by_time_range(start_time="2018-10-03 05:00:00", end_time="2018-10-03 10:00:00")
    stats_traffic_status_by_date()
