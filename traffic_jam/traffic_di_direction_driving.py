#!/usr/bin/python  
#coding:utf-8  

"""
Function: 高德驾车路径规划
@author: Leng
@contact: lengyoufang@163.com
@software: PyCharm
@file: traffic_di_direction_driving.py
@time: 2018/9/10 16:04
"""
# system module
from time import sleep
from datetime import date, time, datetime
import random
import requests
import logging
import traceback
import re
from math import radians, cos, sin, asin, sqrt
import pymongo
import psycopg2
import pandas as pd
import multiprocessing
import json
from urllib.parse import urlencode

# self defined module

# parameters
module_name = "di_road_direction"
amap_api_key_dict = {'db6b6695b77f2662dba6dc0264dfe012': 0, '929492a93387f8b0dc2097563e941e04': 0, '3d283c5ada64139ecca66e4586be1e4d': 0, '355a0d633c580e76df42e3498e8b3833': 0, '667166c9d5a62acf15e53b9285da9eb6':0}   # key means key, values means error retried.
amap_api_key_dict = {'db6b6695b77f2662dba6dc0264dfe012': 0, '929492a93387f8b0dc2097563e941e04': 0, '3d283c5ada64139ecca66e4586be1e4d': 0, '355a0d633c580e76df42e3498e8b3833': 0}   # key means key, values means error retried.
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

def init_USER_DEFINED_DB_CONN(db):
    conn = psycopg2.connect(database=db['db_name'], user=db['db_user'], password=db['db_pass'], host=db['db_host'],
                            port=db['db_port'])
    conn.autocommit = True
    return conn

def get_all_gov_df():
    conn_share = init_USER_DEFINED_DB_CONN(db_share_data)
    sql_sel = "SELECT * FROM dict_gov_lib_view ORDER BY cast(substring(center from '^(.*?),') as NUMERIC) desc"
    all_govs = pd.read_sql(sql=sql_sel, con=conn_share, index_col="gov_code")
    conn_share.close()
    return all_govs
all_govs = get_all_gov_df()

def random_get_a_key():
    retry_max = 10

    # random choice api key whose error retry count less than 10
    global amap_api_key_dict
    valid_key = [k for k, v in amap_api_key_dict.items() if v < retry_max]
    if len(valid_key) == 0:
        logger.error(
            "There is no keys to use in: {amap_api_key_dict}".format(amap_api_key_dict=json.dumps(amap_api_key_dict)))
        raise Exception("All keys are limited!")
    key = random.choice(valid_key)
    return key

def http_get(url, **kwargs):
    retry = 0
    retry_max = 10
    wait_second = 1
    timeout = 5

    while True:
        try:
            key = random_get_a_key()
            kwargs["key"] = key
            response = requests.get(url, params=kwargs, timeout=timeout)
            break
        except:
            amap_api_key_dict[key] += 1
            if retry >= retry_max:
                logger.error("[url request error], retry count:" + str(retry) + ' ************ url **********:' + url)
                logger.error(traceback.format_exc())
                raise  # if retry time get to the max, failed and raise error
            retry += 1
            sleep(retry * wait_second)
            logger.warning("[url request warning], retry count:" + str(retry) + ' ************ url **********:' + url)
    return response.json()

def http_post(url, data_body):
    retry = 0
    retry_max = 10
    wait_second = 1
    timeout = 5
    while True:
        try:
            response = requests.post(url, data=data_body)
            break
        except:
            logger.error(traceback.format_exc())
            if retry >= retry_max:
                logger.error("[url request error], retry count:" + str(retry) + ' ************ url **********:' + url)
                raise  # if retry time get to the max, failed and raise error
            retry += 1
            sleep(retry * wait_second)
            logger.warning("[url request warning], retry count:" + str(retry) + ' ************ url **********:' + url)
    return response.json()


def generate_url(base_url, args_dict=None):
    if args_dict:
        request_params_str = urlencode(args_dict)
        urljoin = base_url + "?" + request_params_str
    else:
        urljoin = base_url
    return urljoin

def gaode_batch_api(batch_request_args_list):
    """
    高德批量请求接口, 批量请求API父请求服务地址
    :return:
    """
    # 1. Get a valid key randomly.
    key = random_get_a_key()

    # 2. Generate batch request body.
    batch_request_url = []
    single_base_url = "/v3/direction/driving"
    for single_request_args in batch_request_args_list:
        single_request_args["output"] = "JSON"
        single_request_args["strategy"] = 0
        single_request_args["key"] = key    # 子请求的key, 必须和子请求保持一致
        single_request_url = generate_url(single_base_url, single_request_args)
        single_request_dict = {"url": single_request_url}
        batch_request_url.append(single_request_dict)
    post_body = {"ops": batch_request_url}

    # 3. Request from gaode and return response.
    gaode_batch_base_url = "https://restapi.amap.com/v3/batch"     # 父请求的key, 必须和子请求保持一致
    gaode_batch_url = generate_url(base_url=gaode_batch_base_url, args_dict={"key": key})
    result = http_post(url=gaode_batch_url, data_body=json.dumps(post_body))
    return result

def format_parameters(paras):
    # 拆分参数
    pattern = re.compile(r'''([^ \(\)\[\]\{\}'",;]+)''')
    matched = pattern.findall(paras)
    return matched

# 根据两点经纬度求直线距离的离线方法
def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
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

def get_geo_list(geos_str, method = 1):
    # 找出字符串中以逗号隔开的多个经纬度对
    test_geos_str = "116.411317,39.910565;116.411371,39.910498;116.411482,39.908147;116.411430,39.908001;116.411335,39.908143;116.411263,39.910495;116.411317,39.910565;116.411358,39.911242;116.411323,39.911931;116.410471,39.911920|116.411058,39.918592;116.411138,39.915438;116.411323,39.911931|116.410762,39.924251;116.410988,39.918796;116.411058,39.918592;116.411114,39.918806;116.411098,39.919402;116.410930,39.921885;116.410878,39.924251"
    method = 1
    if method == 1:
        # method 1: re.split
        pattern_split = re.compile(r'[;| ]+')
        geos_list = re.split(pattern_split, geos_str)
    else:
        # method 2: re.findall
        pattern_findall = re.compile(r'''(\d*.\d*,\d*.\d*)''')
        geos_list = re.findall(pattern_findall, geos_str)
    return geos_list


def get_poi_detail_by_poi(poi_id):
    """
    从数据库中读取已经爬到的一条道路的线路图及其它信息（静态信息）
    :param poi_id:
    :return:
    """
    rows = mongodb_conn_poi.find(filter={"poi_id": poi_id}, batch_size=1)  # must add batch_size to optimize the performance
    result = {} if len(rows) == 0 else rows[0]
    return result


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


def crawl_one_driving_direction(origin, destination, waypoints=None, output="JSON"):
    """
    Function: 指定起点，终点，以及必经点，返回路径规划
    :param origin: 起点经纬度
    :param destination: 终点经纬度
    :param waypoints: 途经点
    :return:
    """
    global amap_api_driving_url
    """
    下方策略仅返回一条路径规划结果
    0，不考虑当时路况，返回耗时最短的路线，但是此路线不一定距离最短
    1，不走收费路段，且耗时最少的路线
    2，不考虑路况，仅走距离最短的路线，但是可能存在穿越小路/小区的情况
    """
    strategy = 0    # 选择策略
    get_driving_result = http_get(url=amap_api_driving_url, origin=origin, destination=destination, strategy=strategy, waypoints=waypoints, output=output)
    return get_driving_result


def get_road_segment_status_distance(driving_detail_payload):
    """
    计算分段状态的长度
    :param detail_payload:
    :return:
    """
    road_status_map = {"未知": "distance_unknown",
                       "畅通": "distance_expedite",
                       "缓行": "distance_congested",
                       "拥堵": "distance_blocked",
                       "严重拥堵": "distance_sblocked"
                       }
    road_segment_status_distance = dict()
    road_segment_status_distance[road_status_map.get("未知")] = 0          # 未知
    road_segment_status_distance[road_status_map.get("畅通")] = 0          # 畅通
    road_segment_status_distance[road_status_map.get("缓行")] = 0          # 缓行
    road_segment_status_distance[road_status_map.get("拥堵")] = 0          # 拥堵
    road_segment_status_distance[road_status_map.get("严重拥堵")] = 0      # 严重拥堵

    # 统计各种路段的长度
    steps = driving_detail_payload.get("route", {}).get("paths", [{}])[0].get("steps", [])
    for step in steps:
        for tmc in step.get("tmcs", []):
            segment_status = tmc.get("status", "未知")
            segment_distance = int(tmc.get("distance", 0))
            road_segment_status_distance[road_status_map.get(segment_status)] += segment_distance
    return road_segment_status_distance


def crawl_one_road_status_by_geos(geos):
    """
    Function: 指定一堆坐标点，找出距离最大的两个点，返回两点之间道路的拥堵情况
    :param road_poi_id: 道路的POI_ID
    :return: road_detail
    """
    global amap_api_driving_url
    max_distance, origin, destination = get_max_distance_of_one_road(geos)
    waypoints = None
    got_driving_payload = crawl_one_driving_direction(origin=origin, destination=destination, waypoints=waypoints)
    road_status = transform_one_road_status(got_driving_payload)
    return road_status


def transform_one_road_status(driving_payload):
    """
    Function: 从driving payload中离线提取有用信息
    :param driving_payload: 驾车路径规划的原始信息
    :return: road_detail
    """
    global amap_api_driving_url
    road_detail = dict()
    if driving_payload.get("status") == "1":   # "1": 请求成功, "0": 请求失败
        now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        road_detail["distance"] = int(driving_payload.get("route", {}).get("paths", [{}])[0].get("distance", 0))
        road_detail["duration"] = int(driving_payload.get("route", {}).get("paths", [{}])[0].get("duration", -1))
        road_detail["traffic_lights"] = int(driving_payload.get("route", {}).get("paths", [{}])[0].get("traffic_lights", -1))
        road_detail["speed"] = round((road_detail["distance"]/road_detail["duration"]) * 3.6, 2)      # 速度转换为：公里/小时
        road_detail["driving_payload"] = driving_payload
        road_detail["ctime"] = now
        road_segment_status_distance = get_road_segment_status_distance(driving_payload)    # 道路分段后的各种状态的统计长度，如：畅通：1000米，拥堵：200米
        road_detail.update(road_segment_status_distance)
    else:
        logger.error(driving_payload)
    return road_detail


def crawl_one_road_status_by_poi(poi):
    """
    Function: 指定一条道路的POI_ID，或poi_detail，返回道路的拥堵情况
    :param road_poi_id: 道路的POI_ID
    :return: road_status
    """
    global amap_api_driving_url
    poi_detail = get_poi_detail_by_poi(poi_id=poi)
    poi_shape = poi_detail.get("poi_shape")
    road_status = crawl_one_road_status_by_geos(geos=poi_shape)
    return road_status

def crawl_all_road_status_by_poi_detail_list(pois_detail_list):
    min_distance = 300
    valid_row_id = 0
    request_args_list = []
    request_pois_list = []
    for poi_detail in pois_detail_list:
        reserve_fields = ["poi_id", "poi_name", "gov_id", "gov_code", "gov_name", "poi_shape", "geo_max_distance", "to_city", "out_city"]
        poi_detail = {k: poi_detail.get(k) for k in reserve_fields if k in poi_detail.keys()}
        poi_id = poi_detail.get("poi_id", "")
        poi_name = poi_detail.get("poi_name", "")
        geo_max_distance = poi_detail.get("geo_max_distance", 0)
        to_city = poi_detail.get("to_city")
        if geo_max_distance >= min_distance and to_city and not re.search(r"胡同$|步行街$|弄$|里$|巷$", poi_name):  # 道路过滤：1. 只抓大于min_distance以上的道路情况, 2. 不抓取胡同等小路
            valid_row_id += 1
            origin = to_city.split(";")[0]
            destination = to_city.split(";")[1]
            request_args = {"origin": origin, "destination": destination}
            request_args_list.append(request_args)
            request_pois_list.append(poi_detail)
    if len(request_args_list) > 0:
        got_roads_status_list = gaode_batch_api(batch_request_args_list=request_args_list)  # ETL - Extract
        for i in range(len(request_pois_list)):
            got_road_status_doc = transform_one_road_status(got_roads_status_list[i].get("body"))  # ETL - Transform
            if got_road_status_doc.get("distance"):  # 如果获取到数据，则更新库
                request_pois_list[i].update(got_road_status_doc)
            else:  # 如果获取数据异常，则不插入到mongo db
                del pois_detail_list[i]

    if len(request_pois_list) > 0:
        mongodb_conn_jam.insert_many(request_pois_list)  # ETL - Load

def crawl_all_road_status_of_one_gov(gov_code):
    """
    爬取一个gov_code下面所有道路的拥堵情况，并保存到mongodb
    :param gov_code: 待获取的gov_code
    :return:
    """
    global all_govs
    success_status = "2"
    batch_size = 20     # gaode batch最多同时支持20个批量请求
    gov_id = all_govs.loc[gov_code, "gov_id"]
    gov_name = all_govs.loc[gov_code, "full_name"]
    logger.info("===========> Start to crawl road status of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, "
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name))
    rows = mongodb_conn_poi.find(filter={"gov_code": gov_code, "status": success_status}, batch_size=batch_size)  # must add batch_size to optimize the performance
    row_id = 0
    pois_detail_list = []
    for row in rows:
        row_id += 1
        pois_detail_list.append(row)
        if row_id % batch_size == 0:
            crawl_all_road_status_by_poi_detail_list(pois_detail_list)
            pois_detail_list = []
            logger.info("gov_id: {gov_id}, gov_name: {gov_name}, row_id: {row_id}, road_id: {road_id}, road_name: {road_name}"
                        .format(gov_id=gov_id, gov_name=gov_name, road_id=row.get("poi_id"), road_name=row.get("poi_name"), row_id=row_id))
    if len(pois_detail_list) > 0:
        crawl_all_road_status_by_poi_detail_list(pois_detail_list)
        logger.info("gov_id: {gov_id}, gov_name: {gov_name}, row_id: {row_id}, road_id: {road_id},road_name: {road_name}"
                    .format(gov_id=gov_id, gov_name=gov_name, road_id=row.get("poi_id"), road_name=row.get("poi_name"), row_id=row_id))
    logger.info("===========> Finish to crawl road status of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, road count: {road_count}"
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name, road_count=row_id))
    return row_id


def update_all_road_status_of_one_gov_in_mongo(gov_code):
    """
    计算一个gov_code下面所有道路的拥堵路段的长度，并保存到mongodb
    :param gov_code: 待获取的gov_code
    :return:
    """
    global all_govs
    batch_size = 100
    gov_id = all_govs.loc[gov_code, "gov_id"]
    gov_name = all_govs.loc[gov_code, "full_name"]
    logger.info("===========> Start to update road segment status of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, "
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name))
    rows = mongodb_conn_jam.find(filter={"gov_code": gov_code}, batch_size=batch_size)  # must add batch_size to optimize the performance
    row_id = 0
    for doc in rows:
        row_id += 1
        driving_payload = doc.get("driving_payload")
        road_segment_status_distance = get_road_segment_status_distance(driving_payload)  # 道路分段后的各种状态的统计长度，如：畅通：1000米，拥堵：200米
        logger.info("gov_id: {gov_id}, gov_name: {gov_name}, road_id: {road_id}, road_name: {road_name}, row_id: {row_id}"
                    .format(gov_id=gov_id, gov_name=gov_name, road_id=doc.get("poi_id"), road_name=doc.get("poi_name"), row_id=row_id))
        doc.update(road_segment_status_distance)
        mongodb_conn_jam.replace_one(filter={"_id": doc["_id"]}, replacement=doc)
    logger.info("===========> Finish to crawl road status of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, road count: {road_count}"
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name, road_count=row_id))
    return row_id

def update_all_road_distance_of_one_gov_in_mongo(gov_code):
    """
    计算一个gov_code下面所有道路的拥堵路段的长度，并保存到mongodb
    :param gov_code: 待获取的gov_code
    :return:
    """
    global all_govs
    batch_size = 100
    gov_id = all_govs.loc[gov_code, "gov_id"]
    gov_name = all_govs.loc[gov_code, "full_name"]
    gov_center = all_govs.loc[gov_code, "center"]
    logger.info("===========> Start to update road segment status of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, "
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name))
    rows = mongodb_conn_poi.find(filter={"gov_code": gov_code}, batch_size=batch_size)  # must add batch_size to optimize the performance
    row_id = 0
    for doc in rows:
        row_id += 1
        poi_shape = doc.get("poi_shape")
        if not poi_shape:
            continue
        logger.info("gov_id: {gov_id}, gov_name: {gov_name}, road_id: {road_id}, road_name: {road_name}, row_id: {row_id}"
                    .format(gov_id=gov_id, gov_name=gov_name, road_id=doc.get("poi_id"), road_name=doc.get("poi_name"), row_id=row_id))
        geo_max_distance, origin, target = get_max_distance_of_one_road(geos=poi_shape, gov_center=gov_center)
        doc["geo_max_distance"] = geo_max_distance
        doc["to_city"] = origin + ";" + target
        doc["out_city"] = target + ";" + origin
        mongodb_conn_poi.replace_one(filter={"_id": doc["_id"]}, replacement=doc)
    logger.info("===========> Finish to crawl road status of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, road count: {road_count}"
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name, road_count=row_id))
    return row_id

def crawl_all_gov_road_status_multi_process(process_num=None):
    """
    Function: 此模块的多线程主函数, 循环爬取所有gov的道路拥堵情况
    :return:
    """
    logger.info("==========> Starting to crawl all roads status... <==========")
    global all_govs
    need_index_county = all_govs[(all_govs['gov_type'].isin([3, 4, 5, 6, 7, 10, 11, 12, 13]))]
    pool = multiprocessing.Pool(process_num)
    for index, gov in need_index_county.iterrows():
        gov_code = index
        gov_id = gov['gov_id']
        gov_name = gov['full_name']
        pool.apply_async(crawl_all_road_status_of_one_gov, (gov_code,))
        # pool.apply_async(update_all_road_distance_of_one_gov_in_mongo, (gov_code,))
    pool.close()
    pool.join()
    logger.info("Gaode api key status: {amap_api_key_dict}".format(amap_api_key_dict=amap_api_key_dict))
    logger.info("==========> Complete to crawl all roads status... <==========")

if __name__ == "__main__":
    pass
    # result = get_one_driving_direction(origin="116.411317,39.910565", destination="116.410471,39.911920", waypoints="116.411317,39.910565;116.411371,39.910498;116.411482,39.908147;116.411430,39.908001;116.411335,39.908143;116.411263,39.910495;116.411317,39.910565;116.411358,39.911242;116.411323,39.911931;116.410471,39.911920")
    # result = haversine(1, 1, 2, 2)
    # geos = "116.411317,39.910565;116.411371,39.910498;116.411482,39.908147;116.411430,39.908001;116.411335,39.908143;116.411263,39.910495;116.411317,39.910565;116.411358,39.911242;116.411323,39.911931;116.410471,39.911920|116.411058,39.918592;116.411138,39.915438;116.411323,39.911931|116.410762,39.924251;116.410988,39.918796;116.411058,39.918592;116.411114,39.918806;116.411098,39.919402;116.410930,39.921885;116.410878,39.924251"
    # print(get_one_road_status_by_geos(geos))
    # result = crawl_all_road_status_of_one_gov("110108")
    # update_all_road_status_of_one_gov_in_mongo("110108")
    crawl_all_gov_road_status_multi_process(4)
    # update_all_road_distance_of_one_gov_in_mongo("510107")
    # gaode_batch_api()
    # crawl_all_road_status_of_one_gov(gov_code="510107")