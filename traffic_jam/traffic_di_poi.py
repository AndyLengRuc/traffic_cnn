#!/usr/bin/python  
#coding:utf-8  

""" 
@author: Leng 
@contact: lengyoufang@163.com 
@software: 通过poi_code从高德地图抓取POI
@file: traffic_di_poi.py
@time: 2018/9/2 14:49 
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

# parameters
module_name = "di_road"
amap_api_key_dict = {'db6b6695b77f2662dba6dc0264dfe012': 0, '929492a93387f8b0dc2097563e941e04': 0, '3d283c5ada64139ecca66e4586be1e4d': 0, '355a0d633c580e76df42e3498e8b3833': 0, '667166c9d5a62acf15e53b9285da9eb6':0}   # key means key, values means error retried.
amap_api_poi_url = "https://restapi.amap.com/v3/place/text"
mongodb_host = 'mongodb://admin:123456@192.168.0.117:27017/admin'
mongodb_name = 'zk_traffic'
mongodb_col_poi = 'di_poi'
db_share_data = {'db_host': '120.77.204.22', 'db_port': 5999, 'db_user': 'readonly', 'db_pass': '123456', 'db_name': 'share_data'}
mongodb_conn_poi = pymongo.MongoClient(mongodb_host)[mongodb_name][mongodb_col_poi]

# common untils
def get_logger(module_name):
    log_file = "./logs/" + "" + module_name + ".log"  # log file
    log_format = "%(asctime)s -%(name)s-%(levelname)s-%(module)s: %(message)s"
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

def http_get(url, **kwargs):
    # random choice api key whose error retry count less than 100
    global amap_api_key_dict
    valid_key = [k for k, v in amap_api_key_dict.items() if v < 100]
    if len(valid_key) == 0:
        logger.error("There is no keys to use in: {amap_api_key_dict}".format(amap_api_key_dict=json.dumps(amap_api_key_dict)))
        raise Exception("All keys are limited!")
    key = random.choice(valid_key)
    kwargs['key'] = key

    retry = 0
    retry_max = 10
    wait_second = 1
    timeout = 5
    while True:
        try:
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

# self defined module
def crawl_pois_of_type_code(gov_code, type_code):
    """
    Function: 获取某个poi_code下所有的poi, 如果有多页则分页查询
    :param gov_code:  6位gov_code
    :param type_code:  高德poi type_code
    :return: 返回抓取到的POI List
    """
    # initial parameters

    max_page = 100
    offset = 20     # 每页记录数据，默认为20，强烈建议不超过25，若超过25可能造成访问报错
    citylimit = True

    # get pois
    poi_list = []
    page = 0
    poi_count = 0
    gov_name = all_govs.loc[gov_code, "full_name"]
    logger.info("Crawling poi of gov_code: {gov_code}, gov_name: {gov_name}, type_code: {type_code}".format(gov_code=gov_code, gov_name=gov_name, type_code=type_code))
    while True:
        # 分页查询
        page += 1
        logger.info("Crawling poi of gov_code: {gov_code}, gov_name: {gov_name}, type_code: {type_code}, page: {page}".format(gov_code=gov_code, gov_name=gov_name, type_code=type_code, page=page))
        this_loop_result = http_get(amap_api_poi_url, city=gov_code, types=type_code, citylimit=citylimit, offset=offset, page=page, output='JSON')
        this_loop_pois = this_loop_result.get('pois', [])
        this_loop_pois_count = len(this_loop_pois)
        poi_count += this_loop_pois_count
        if this_loop_pois_count == 0 or page > max_page:
            break   # 最后一页时退出
        for poi_item in this_loop_pois:
            poi_dict = transform_poi_item(poi_item)
            poi_dict["gov_code"] = gov_code
            poi_dict["gov_id"] = int(all_govs.loc[gov_code, "gov_id"])
            search_result = mongodb_conn_poi.count(filter={"$and": [{"poi_id": {"$eq": poi_dict["poi_id"]}}]})
            if search_result == 0:
                logger.info("gov_code: {gov_code}, gov_name: {gov_name}, type_code: {type_code}, poi_id: {poi_id}, poi_name: {poi_name}".format(gov_code=gov_code, gov_name=gov_name, type_code=type_code, poi_id=poi_dict["poi_id"], poi_name=poi_dict["poi_name"]))
                poi_list.append(poi_dict)
        if len(poi_list) > 0:
            save_many_pois(poi_list)
            poi_list = []
    logger.info("Complete crawl poi of gov_code: {gov_code}, gov_name: {gov_name}, type_code: {type_code}, poi_count: {poi_count}".format(gov_code=gov_code, gov_name=gov_name, type_code=type_code, poi_count=poi_count))
    return poi_count

def transform_poi_item(poi_item):
    now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    init_status = "0"
    running_status = "1"
    finish_status = "2"
    poi_dict = dict()
    poi_dict["gov_name"] = "|".join([poi_item.get('pname'), poi_item.get('cityname'), poi_item.get('adname')])
    poi_dict["address"] = poi_item.get("address")
    poi_dict["type_code"] = poi_item.get('typecode')
    poi_dict["category"] = poi_item.get('type')

    poi_dict["poi_id"] = poi_item.get("id")
    poi_dict["poi_name"] = poi_item.get("name")
    poi_dict["location"] = poi_item.get("location")
    poi_dict["is_head"] = 1
    poi_dict["poi_payload"] = poi_item
    poi_dict["status"] = init_status
    poi_dict["ctime"] = now
    return poi_dict

def update_head_flag(gov_id, type_code):
    # 将之前的数据的is_head 修改为 0
    mycol = get_mongodb_conn(collection=mongodb_col_poi)
    myquery = {"$and": [{"gov_id": gov_id}, {"type_code": type_code}]}
    newvalues = {"$set": {"is_head": 0}}
    x = mycol.update_many(myquery, newvalues)
    logger.info(x.modified_count, "条数据的is_head被修改")

def save_one_poi(poi):
    """
    Function: 保存一个poi
    :param poi: 一个poi的dict
    :return:
    """
    logger.info("Saving poi ...")
    mycol = get_mongodb_conn(collection=mongodb_col_poi)
    mycol.insert_one(poi)
    logger.info("Complete to save poi ...")

def save_many_pois(poi_list):
    """
    Function: 保存多个poi
    :param poi_list: 多个poi dict组成的list
    :return:
    """
    logger.info("Saving poi ...")
    poi_list_length = len(poi_list)
    if poi_list_length > 0:
        mycol = get_mongodb_conn(collection=mongodb_col_poi)
        mycol.insert_many(poi_list)
    logger.info("Complete to save {poi_list_length} poi ...".format(poi_list_length=poi_list_length))

def delete_duplicate_poi_id_of_one_gov(gov_code):
    """
    删除重复的POI_ID
    :return:
    """
    global all_govs
    init_status = "0"
    running_status = "1"
    success_status = "2"
    mismatch_status = "-1"
    batch_size = 100
    gov_id = all_govs.loc[gov_code, "gov_id"]
    gov_name = all_govs.loc[gov_code, "full_name"]
    logger.info("===========> Start to delete duplicate poi_id of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, "
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name))
    rows = mongodb_conn_poi.find(filter={"gov_code": gov_code}, sort=[("status", pymongo.DESCENDING)], batch_size=batch_size)  # must add batch_size to optimize the performance
    row_id = 0
    for row in rows:
        result = mongodb_conn_poi.delete_many(filter={"$and": [
                                                        {"poi_id": {"$eq": row["poi_id"]}},
                                                        {"_id": {"$ne": row["_id"]}}]
                                                    })
        if result.deleted_count > 0:
            row_id += 1
            print(result.deleted_count, row["poi_id"], row["poi_name"])
    logger.info("===========> Finish to delete duplicate poi_id of gov_id: {gov_id}, gov_code: {gov_code}, gov_name: {gov_name}, road count: {road_count}"
                .format(gov_id=gov_id, gov_code=gov_code, gov_name=gov_name, road_count=row_id))
    return row_id

import multiprocessing
def delete_all_duplicate_poi_id(process_num=None):
    """
    Function: 检查所有的道路名称
    :return:
    """
    logger.info("Starting to delete all duplicate poi_id...")
    global all_govs
    need_index_county = all_govs[(all_govs['gov_type'].isin([3, 4, 5, 6]))]
    pool = multiprocessing.Pool(process_num)
    for index, gov in need_index_county.iterrows():
        gov_code = index
        gov_id = gov['gov_id']
        gov_name = gov['full_name']
        pool.apply_async(delete_duplicate_poi_id_of_one_gov, (gov_code,))
    pool.close()
    pool.join()
    logger.info("Complete delete all duplicate poi_id.")

def crawl_all_gov_roads(process_num=None):
    """
    Function: 此模块的主函数, 循环爬取所有gov的道路主函数
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
    pool = multiprocessing.Pool(process_num)
    need_crawl_gov = all_govs[(all_govs['gov_type'].isin([3, 4, 5, 6]))]
    for index, gov in need_crawl_gov.iterrows():
        gov_code = index
        gov_id = gov['gov_id']
        gov_name = gov['full_name']
        pool.apply_async(crawl_pois_of_type_code, (gov_code, road_poi_code,))
    pool.close()
    pool.join()
    logger.info("AMAP api key status: {amap_api_key_dict}".format(amap_api_key_dict=amap_api_key_dict))
    logger.info("Complete to get all roads.")

if __name__ == "__main__":
    # crawl_pois_of_type_code(gov_code="110108", type_code="190301")
    # delete_all_duplicate_poi_id()
    # delete_duplicate_poi_id_of_one_gov("440307")
    crawl_all_gov_roads()