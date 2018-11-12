#!/usr/bin/python  
#coding:utf-8  

""" 
@Function: 添加交通拥堵元数据到知识库
@author: Leng 
@contact: lengyoufang@163.com 
@software: PyCharm 
@file: traffic_dc_meta.py
@time: 2018/9/20 15:45 
"""
# system module
from time import sleep
from datetime import date, time, datetime
import requests
import traceback

# self defined module

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

def add_knowledge_meta_of_traffic_jam():
    """
    添加交通拥堵元数据到知识库
    :return:
    """
    traffic_metas = [
            {"module": "traffic", "type_code": "8510010201", "type_name": "[拥堵]早高峰采样的道路条数", "data_unit": "条", "category_big": "交通建设", "category_mid": "拥堵", "category_sub": "道路总条数", "description": "本次早高峰采样的道路条数", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010202", "type_name": "[拥堵]早高峰采样的道路总长度", "data_unit": "千米", "category_big": "交通建设", "category_mid": "拥堵", "category_sub": "道路总长度", "description": "本次早高峰采样的道路总长度，单位为千米", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010203", "type_name": "[拥堵]早高峰(7:00-10:00)最慢驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)最慢驾车速度", "description": "区域内早高峰(7:00-10:00)最慢驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010204", "type_name": "[拥堵]早高峰(7:00-10:00)最快驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)最快驾车速度", "description": "区域内早高峰(7:00-10:00)最快驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010205", "type_name": "[拥堵]早高峰(7:00-10:00)平均驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)平均驾车速度", "description": "区域内早高峰(7:00-10:00)平均驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010206", "type_name": "[拥堵]早高峰(7:00-10:00)未知路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)未知路段比例", "description": "区域内早高峰(7:00-10:00)未知路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010207", "type_name": "[拥堵]早高峰(7:00-10:00)畅通路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)畅通路段比例", "description": "区域内早高峰(7:00-10:00)畅通路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010208", "type_name": "[拥堵]早高峰(7:00-10:00)缓行路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)缓行路段比例", "description": "区域内早高峰(7:00-10:00)缓行路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010209", "type_name": "[拥堵]早高峰(7:00-10:00)拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)拥堵路段比例", "description": "区域内早高峰(7:00-10:00)拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010210", "type_name": "[拥堵]早高峰(7:00-10:00)严重拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "早高峰(7:00-10:00)严重拥堵路段比例", "description": "区域内早高峰(7:00-10:00)严重拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010231", "type_name": "[拥堵]晚高峰采样的道路条数", "data_unit": "条", "category_big": "交通建设", "category_mid": "道路", "category_sub": "道路总条数", "description": "本次晚高峰采样的道路条数", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010232", "type_name": "[拥堵]晚高峰采样的道路总长度", "data_unit": "千米", "category_big": "交通建设", "category_mid": "道路", "category_sub": "道路总长度", "description": "本次晚高峰采样的道路总长度，单位为千米", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010233", "type_name": "[拥堵]晚高峰(17:00-20:00)最慢驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(17:00-20:00)最慢驾车速度", "description": "区域内晚高峰(17:00-20:00)最慢驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010234", "type_name": "[拥堵]晚高峰(17:00-20:00)最快驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(17:00-20:00)最快驾车速度", "description": "区域内晚高峰(17:00-20:00)最快驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010235", "type_name": "[拥堵]晚高峰(17:00-20:00)平均驾车速度", "data_unit": "千米/小时", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(17:00-20:00)平均驾车速度", "description": "区域内晚高峰(17:00-20:00)平均驾车速度，单位为千米/小时", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010236", "type_name": "[拥堵]晚高峰(7:00-10:00)未知路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)未知路段比例", "description": "区域内晚高峰(7:00-10:00)未知路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010237", "type_name": "[拥堵]晚高峰(17:00-20:00)畅通路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)畅通路段比例", "description": "区域内晚高峰(7:00-10:00)畅通路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010238", "type_name": "[拥堵]晚高峰(17:00-20:00)缓行路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)缓行路段比例", "description": "区域内晚高峰(7:00-10:00)缓行路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010239", "type_name": "[拥堵]晚高峰(17:00-20:00)拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)拥堵路段比例", "description": "区域内晚高峰(7:00-10:00)拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8510010240", "type_name": "[拥堵]晚高峰(17:00-20:00)严重拥堵路段比例", "data_unit": "%", "category_big": "拥堵", "category_mid": "驾车拥堵", "category_sub": "晚高峰(7:00-10:00)严重拥堵路段比例", "description": "区域内早高峰(7:00-10:00)严重拥堵路段长度占总道路长度的比例，单位为%", "submitter": "leng"},
            {"module": "traffic", "type_code": "8520010201", "type_name": "[拥堵]早高峰TPI指数(交通拥堵指数)", "data_unit": "指数值", "category_big": "拥堵", "category_mid": "TPI", "category_sub": "交通指数", "description": "交通指数是交通拥堵指数或交通运行指数（Traffic Performance Index，即“TPI”）的简称，是综合反映道路网畅通或拥堵的概念性指数值。相当于把拥堵情况数字化。交通指数取值范围为0～10，分为五级。其中0～2、2～4、4～6、6～8、8～10分别对应“畅通”、“基本畅通”、“轻度拥堵”、“中度拥堵”、“严重拥堵”五个级别，数值越高表明交通拥堵状况越严重。", "submitter": "leng"},
            {"module": "traffic", "type_code": "8520010202", "type_name": "[拥堵]晚高峰TPI指数(交通拥堵指数)", "data_unit": "指数值", "category_big": "拥堵", "category_mid": "TPI", "category_sub": "交通指数", "description": "交通指数是交通拥堵指数或交通运行指数（Traffic Performance Index，即“TPI”）的简称，是综合反映道路网畅通或拥堵的概念性指数值。相当于把拥堵情况数字化。交通指数取值范围为0～10，分为五级。其中0～2、2～4、4～6、6～8、8～10分别对应“畅通”、“基本畅通”、“轻度拥堵”、“中度拥堵”、“严重拥堵”五个级别，数值越高表明交通拥堵状况越严重。", "submitter": "leng"},
            {"module": "traffic", "type_code": "8520010203", "type_name": "[拥堵]全天TPI(交通指数)", "data_unit": "指数值", "category_big": "拥堵", "category_mid": "TPI", "category_sub": "交通指数", "description": "交通指数是交通拥堵指数或交通运行指数（Traffic Performance Index，即“TPI”）的简称，是综合反映道路网畅通或拥堵的概念性指数值。相当于把拥堵情况数字化。交通指数取值范围为0～10，分为五级。其中0～2、2～4、4～6、6～8、8～10分别对应“畅通”、“基本畅通”、“轻度拥堵”、“中度拥堵”、“严重拥堵”五个级别，数值越高表明交通拥堵状况越严重。", "submitter": "leng"}
           ]

    meta_url = "http://192.168.0.88:9400/knowledgemeta/"
    for traffic_meta in traffic_metas:
        traffic_meta["operation_type"] = "write"
        http_get(url=meta_url, params=traffic_meta)


if __name__ == "__main__":
    add_knowledge_meta_of_traffic_jam()