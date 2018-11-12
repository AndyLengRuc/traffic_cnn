#!/usr/bin/python  
#coding:utf-8  

""" 
@author: Leng
@function: Crawling the road shape.
@file: traffic_di_road_shape.py
@time: 2018/9/3 17:49 
"""

# system module
from time import sleep
from datetime import date, time, datetime
import random
import requests
import hashlib
import logging
import traceback
import pymongo
import psycopg2
import pandas as pd
import json
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne, DeleteMany
import os
import multiprocessing
import re
from math import radians, cos, sin, asin, sqrt

# parameters
module_name = "di_road_shape"
amap_api_key_dict = {'db6b6695b77f2662dba6dc0264dfe012': 0, '929492a93387f8b0dc2097563e941e04': 0, '3d283c5ada64139ecca66e4586be1e4d': 0, '355a0d633c580e76df42e3498e8b3833': 0, '667166c9d5a62acf15e53b9285da9eb6':0}   # key means key, values means error retried.
amap_api_poi_url = "https://restapi.amap.com/v3/place/text"
mongodb_host = 'mongodb://admin:123456@192.168.0.117:27017/admin'
mongodb_name = 'zk_traffic'
mongodb_collection_poi = 'di_poi'
db_share_data = {'db_host': '120.77.204.22', 'db_port': 5999, 'db_user': 'readonly', 'db_pass': '123456', 'db_name': 'share_data'}

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

def get_md5_value(src):
    """get md5 of the src string"""
    myMd5 = hashlib.md5()
    myMd5.update(src.encode('utf-8'))
    myMd5_Digest = myMd5.hexdigest()
    return myMd5_Digest

proxy_dict = {}
def get_proxy():
    proxy_server = "http://192.168.0.88:5010/get/"
    timeout = 5
    response = requests.get(proxy_server, timeout=timeout)
    proxy = response.text
    return proxy

last_proxy = None
is_last_proxy_ok = False
def http_get_no_key(url, need_proxy=False, **kwargs):
    retry = 0
    retry_max = 100
    delete_proxy_threshold = 10
    wait_second = 1
    timeout = 5
    global proxy_dict
    global last_proxy
    global is_last_proxy_ok
    result = {}
    while True:
        try:
            if is_last_proxy_ok is False or proxy_dict[last_proxy] >= delete_proxy_threshold:
                requests.get(url="http://192.168.0.88:5010/delete?proxy={proxy_ip_port}".format(proxy_ip_port=last_proxy))
                last_proxy = get_proxy()
            proxy = {"https": "http://" + last_proxy} if need_proxy else None
            print("proxy: " + json.dumps(proxy) + " pid: " + str(os.getpid()))
            response = requests.get(url, params=kwargs, timeout=timeout, proxies=proxy)
            result = response.json()
            is_last_proxy_ok = True
            break
        except:
            proxy_dict[last_proxy] = proxy_dict.get(last_proxy, 0) + 1
            if proxy_dict[last_proxy] >= delete_proxy_threshold:
                requests.get(url="http://192.168.0.88:5010/delete?proxy={proxy_ip_port}".format(proxy_ip_port=proxy_ip_port))
                is_last_proxy_ok = False
            if retry >= retry_max:
                logger.error("[url request error], retry count:" + str(retry) + ' ************ url **********:' + url)
                logger.error(traceback.format_exc())
                break  # if retry time get to the max, failed and raise error
            retry += 1
            sleep(retry * wait_second)
            logger.warning("[url request warning], retry count:" + str(retry) + ' ************ url **********:' + url)
            logger.warning(traceback.format_exc())
    # print(response.json())
    return result

def get_mongodb_conn(collection):
    myclient = pymongo.MongoClient(mongodb_host)
    mydb = myclient[mongodb_name]
    mycol = mydb[collection]
    return mycol

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

def check_the_name_is_road(name):
    """
    Function: 用正则表达式检查抓取到的道路名称的合法性
    :param name: 道路名称
    :return: True or False
    """
    name = name or ""
    match_result = re.search(r"(?:路|街|道|胡同|环|段|巷|线|高速|弄|桥$|里$|条$|排$|横$|沿$)", name)
    check_result = True if match_result is not None else False
    return check_result

def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    # 根据两点经纬度求直线距离的离线方法
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(float, [lon1, lat1, lon2, lat2])
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000    # 返回为米
    # return c * r    # 返回为千米

def get_max_distance_of_one_road(geos, gov_center="0,0"):
    """
    Function: 根据给定的多个geo坐标搜索距离最远的两个点
    :param geos: 多个坐标点的经纬度
    :return: 两个最远的坐标点, origin: 离center远的点, destination: 离center近的点
    """
    if isinstance(geos, (list, set, tuple)):
        geos_list = geos
    else:
        geos_list = re.split(r'[;| ]+', geos)
    max_distance = 0
    node_1 = ""
    node_2 = ""
    for i in range(len(geos_list)-1):
        for j in range(i+1, len(geos_list)):
            i_lon = float(geos_list[i].split(",")[0])
            i_lat = float(geos_list[i].split(",")[1])
            j_lon = float(geos_list[j].split(",")[0])
            j_lat = float(geos_list[j].split(",")[1])
            distance_i_j = haversine(i_lon, i_lat, j_lon, j_lat)
            if distance_i_j > max_distance:
                max_distance = round(distance_i_j, 2)
                node_1 = geos_list[i]
                node_2 = geos_list[j]
    node_1_to_center = haversine(node_1.split(",")[0], node_1.split(",")[1], gov_center.split(",")[0], gov_center.split(",")[1])
    node_2_to_center = haversine(node_2.split(",")[0], node_2.split(",")[1], gov_center.split(",")[0], gov_center.split(",")[1])
    if node_1_to_center > node_2_to_center:
        origin = node_1
        destination = node_2
    else:
        origin = node_2
        destination = node_1
    return max_distance, origin, destination

def check_road_name_by_gov(gov_code):
    global all_govs
    global proxy_dict
    global last_proxy
    global is_last_proxy_ok
    gov_id = all_govs.loc[gov_code, "gov_id"]
    gov_name = all_govs.loc[gov_code, "full_name"]
    logger.info('=' * 10 + "Start check road name of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}".format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name))
    conn_mongo = get_mongodb_conn(mongodb_collection_poi)
    init_status = "0"
    running_status = "1"
    success_status = "2"
    mismatch_status = "-1"
    batch_size = 10
    rows = conn_mongo.find(filter={"gov_code": gov_code}, batch_size=batch_size)  # must add batch_size to optimize the performance
    actions = []
    row_id = 0
    for row in rows:
        row_id += 1
        now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        this_loop_poi_id = row["poi_id"]
        this_loop_poi_name = row["poi_name"]
        check_result = check_the_name_is_road(this_loop_poi_name)
        if not check_result:
            proxy_dict[last_proxy] = proxy_dict.get(last_proxy, 0) + 1
            logger.info("Checking {row_id}, poi_id: {poi_id}, road name: {road_name}, result: {check_result}"
                        .format(row_id=row_id, poi_id=this_loop_poi_id, road_name=this_loop_poi_name, check_result=check_result))
            conn_mongo.update_one(filter={"_id": row["_id"]}, update={"$set": {"status": mismatch_status}})
    logger.info("Complete check road name: pid=%s, gov_id=%s, gov_code=%s, gov_name=%s, count=%s" % (os.getpid(), gov_id, gov_code, gov_name, row_id))
    return row_id

def get_poi_shape(request_poi_id, request_poi_name=None, gov_code=None):
    global all_govs
    gov_center = all_govs.loc[gov_code, "center"]

    sleep(random.random() * 10)
    url = "https://www.amap.com/detail/get/detail"
    need_proxy = True
    poi_detail_payload = http_get_no_key(url, need_proxy=need_proxy, id=request_poi_id)   # Do not need key for this request.
    matched = False
    poi_detail = {}
    if isinstance(poi_detail_payload, dict) and poi_detail_payload.get("status") == "1":
        
        poi_detail['poi_id'] = poi_detail_payload.get("data", {}).get("base", {}).get("poiid", "")
        poi_detail['poi_name'] = poi_detail_payload.get("data", {}).get("base", {}).get("name", "")
        poi_detail['type_code'] = poi_detail_payload.get("data", {}).get("base", {}).get("new_type", "")
        poi_detail['category'] = poi_detail_payload.get("data", {}).get("base", {}).get("new_keytype", "")
        poi_detail['address'] = poi_detail_payload.get("data", {}).get("base", {}).get("address", "")
        poi_detail['location'] = poi_detail_payload.get("data", {}).get("base", {}).get("x", "") + "," + poi_detail_payload.get("data", {}).get("base", {}).get("y", "")
        poi_detail['poi_shape'] = poi_detail_payload.get("data", {}).get("spec", {}).get("mining_shape", {}).get("shape", "")
        poi_detail['poi_payload'] = poi_detail_payload
        matched = check_the_name_is_road(poi_detail['poi_name'])
    if matched and poi_detail['poi_shape']:
        geo_max_distance, origin, target = get_max_distance_of_one_road(geos=poi_detail['poi_shape'], gov_center=gov_center)
        poi_detail["geo_max_distance"] = geo_max_distance
        poi_detail["to_city"] = origin + ";" + target
        poi_detail["out_city"] = target + ";" + origin
        logger.info("shape_matched ? {matched}, pid: {pid} request_poi_id: {request_poi_id}, returned_poi_id: {returned_poi_id}, request_poi_name: {request_poi_name}, returned_poi_name: {returned_poi_name}, poi_shape:{returned_poi_shape}"
                    .format(matched=matched, pid=os.getpid(), request_poi_id=request_poi_id, returned_poi_id=poi_detail.get('poi_id'), request_poi_name=request_poi_name, returned_poi_name=poi_detail.get('poi_name'), returned_poi_shape=poi_detail.get("poi_shape")))
    else:
        logger.info("shape_matched ? {matched}, pid: {pid} request_poi_id: {request_poi_id}, returned_poi_id: {returned_poi_id}, request_poi_name: {request_poi_name}, returned_poi_name: {returned_poi_name}, poi_shape:{returned_poi_shape}"
                .format(matched=matched, pid=os.getpid(), request_poi_id=request_poi_id, returned_poi_id=poi_detail.get('poi_id'),
                    request_poi_name=request_poi_name, returned_poi_name=poi_detail.get('poi_name'),
                    returned_poi_shape=poi_detail.get("poi_shape")))
    return matched, poi_detail

def update_status(gov_id, status):
    init_status = "0"
    running_status = "1"
    finish_status = "2"
    conn_mongo = get_mongodb_conn()
    now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    sql_result = conn_mongo.update_many({"$and": [
        {"gov_id": gov_id},
        {"status": {"$in": [init_status, running_status, finish_status]}},
    ]},
        {"$set": {"status": status, "ctime": now}}
    )


def update_mongodb(gov_id, status):
    init_status = "0"
    running_status = "1"
    finish_status = "2"
    conn_mongo = get_mongodb_conn(mongodb_collection_poi)
    now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    sql_result = conn_mongo.update_many(filter={"$and": [
        {"status": {"$ne": finish_status}},
    ]},
        update={"$set": {"status": init_status}}
    )

def insert_many_pois(poi_list):
    """
    Function: 保存多个poi
    :param poi_list: 多个poi dict组成的list
    :return:
    """
    logger.info("Saving poi ...")
    poi_list_length = len(poi_list)
    if poi_list_length > 0:
        mycol = get_mongodb_conn(collection=mongodb_collection_poi)
        mycol.insert_many(poi_list)
    logger.info("Complete to save {poi_list_length} poi ...".format(poi_list_length=poi_list_length))


def crawl_poi_shape_by_gov(gov_code):
    global all_govs
    gov_id = all_govs.loc[gov_code, "gov_id"]
    gov_name = all_govs.loc[gov_code, "full_name"]
    logger.info('=' * 10 + "Start crawling poi shape of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}".format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name))
    conn_mongo = get_mongodb_conn(mongodb_collection_poi)
    init_status = "0"
    running_status = "1"
    success_status = "2"
    mismatch_status = "-1"
    sql_result = conn_mongo.update_many({"$and": [
                                                    {"gov_code": gov_code},
                                                    {"status": {"$in": [init_status, running_status, mismatch_status]}},
                                                ]},
                                        {"$set": {"status": running_status}}
                                        )
    row_count = sql_result.matched_count or sql_result.modified_count
    batch_size = 10
    rows = conn_mongo.find(filter={"gov_code": gov_code, "status": running_status}, batch_size=batch_size)  # must add batch_size to optimize the performance
    actions = []
    row_id = 0
    for row in rows:
        row_id += 1
        now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        this_loop_poi_id = row["poi_id"]
        this_loop_poi_name = row["poi_name"]
        logger.info("Querying poi_shape: {row_id}/{row_count}".format(row_id=row_id, row_count=row_count))
        shape_matched, poi_detail = get_poi_shape(this_loop_poi_id, this_loop_poi_name, gov_code)
        status = success_status if shape_matched else mismatch_status
        poi_detail['shape_matched'] = shape_matched
        poi_detail['status'] = status
        poi_detail['ctime'] = now
        action_update = UpdateOne(filter={"_id": row["_id"]}, update={"$set": poi_detail})
        actions.append(action_update)
        if (row_id % batch_size == 0) or (row_id == row_count):
            if len(actions) > 0:
                conn_mongo.bulk_write(requests=actions)
                logger.info("pid=%s, gov_id=%s, gov_code=%s, gov_name=%s, updating poi shape #: %s/%s" % (os.getpid(), gov_id, gov_code, gov_name, row_id, row_count))
                actions = []
    logger.info("Complete get poi shape : pid=%s, gov_id=%s, gov_code=%s, gov_name=%s, count=%s" % (os.getpid(), gov_id, gov_code, gov_name, row_count))
    return row_count

def crawl_all_gov_poi_shape_one_process():
    """
    Function: 此模块的单线程主函数, 循环爬取所有gov的道路shape主函数
    :return:
    """
    # bj_gov_code = {'110100': '北京市市辖区', '110101': '东城区', '110102': '西城区', '110105': '朝阳区',
    #                '110106': '丰台区', '110107': '石景山区', '110108': '海淀区', '110109': '门头沟区',
    #                '110111': '房山区', '110112': '通州区', '110113': '顺义区', '110114': '昌平区',
    #                '110115': '大兴区', '110116': '怀柔区', '110117': '平谷区', '110118': '密云区',
    #                '110119': '延庆区'}
    logger.info("Starting to get all roads...")
    global all_govs
    road_poi_code = "190301"
    start_gov = "130503"

    need_crawl_gov = all_govs[(all_govs['gov_type'].isin([3, 4, 5, 6])) & (all_govs['gov_id'] > 2)]
    for index, gov in need_crawl_gov.iterrows():
        gov_code = index
        gov_id = gov['gov_id']
        gov_name = gov['full_name']
        crawed_poi_shape_count = crawl_poi_shape_by_gov(gov_code=gov_code)
    logger.info("Gaode api key status: {amap_api_key_dict}".format(amap_api_key_dict=amap_api_key_dict))
    logger.info("Complete to get all poi shapes.")


def check_all_gov_road_name_multi_process(process_num=None):
    """
    Function: 检查所有的道路名称
    :return:
    """
    logger.info("Starting to check all roads...")
    global all_govs
    need_index_county = all_govs[(all_govs['gov_type'].isin([3, 4, 5, 6]))]
    pool = multiprocessing.Pool(process_num)
    for index, gov in need_index_county.iterrows():
        gov_code = index
        gov_id = gov['gov_id']
        gov_name = gov['full_name']
        pool.apply_async(check_road_name_by_gov, (gov_code,))
    pool.close()
    pool.join()
    logger.info("Complete check all roads.")


def crawl_all_gov_poi_shape_multi_process(process_num=None):
    """
    Function: 此模块的多线程主函数, 循环爬取所有gov的道路shape主函数
    :return:
    """
    logger.info("Starting to get all roads...")
    global all_govs
    road_poi_code = "190301"
    start_gov = "130503"
    need_index_county = all_govs[(all_govs['gov_type'].isin([3, 4, 5, 6]))]
    pool = multiprocessing.Pool(process_num)
    for index, gov in need_index_county.iterrows():
        gov_code = index
        gov_id = gov['gov_id']
        gov_name = gov['full_name']
        pool.apply_async(crawl_poi_shape_by_gov, (gov_code,))
    pool.close()
    pool.join()
    logger.info("Gaode api key status: {amap_api_key_dict}".format(amap_api_key_dict=amap_api_key_dict))
    logger.info("Complete to get all poi shapes.")


if __name__ == "__main__":
    pass
    # crawl_all_gov_poi_shape()
    crawl_all_gov_poi_shape_multi_process()
    # crawl_poi_shape_by_gov("510108")
    # check_all_gov_road_name_multi_process()
    # check_road_name_by_gov("510106")