#utf-8#
'''此程序为LJ所有，其功能为爬取全国不同类型的铁路车次信息'''

from pyecharts import options as opts
from pyecharts.charts import Map
from pyecharts.charts import Geo
from pyecharts.faker import Faker
import requests
import pandas as pd
import urllib.request
import igraph as ig
from bs4 import BeautifulSoup
import random
import re
import numpy as np
import networkx as nx
import csv
import datetime
import threading
from lxml import etree
from selenium import webdriver
import time
from headers import get_headers
import os
import json
from geocoding_amap import geocode_multi_infos
tim = random.randint(6, 10)


# 制作关系数据库，生成OD网络
def station_od_shape(source_province_stations, target_province_stations):
    stations_os, stations_ds = [], []
    for i in range(len(source_province_stations)):
        station_i = source_province_stations[i]
        for j in range(len(target_province_stations)):
            station_j = target_province_stations[j]
            if station_j != station_i:
                stations_os.append(station_i)
                stations_ds.append(station_j)
    df_ods = pd.DataFrame({"source_station": stations_os, "target_station": stations_ds})
    return df_ods


# 从携程爬取中间站信息
def crawl_train(start_url, train_code, csv_write, csv_write1):
    print(train_code)
    url = start_url + str(train_code) +'/'  # https://trains.ctrip.com/TrainSchedule/G1/
    # print(url)
    try:
        res = requests.get(url, headers=headers, timeout=30).content
        # print(res)
        soups = BeautifulSoup(res, 'lxml')  # html.parser
        # print(soups)
        infos = soups.select("tbody:nth-of-type(1)")[1]
        # print(infos)
        rows = infos('tr')
        # print(rows)
        # station_codes=[]
        # station_nums=[]
        # stations = []
        # time_enters = []
        # time_exits = []
        # time_lasts = []
        #获取中间站点信息
        for row in rows:
            # station_codes.append(train_code)
            station_num=row('td')[1].text.strip()
            # station_nums.append(station_num)
            station=row('td')[2].text.strip()
            # stations.append(station)
            time_enter = row('td')[3].text.strip()
            # time_enters.append(time_enter)
            time_exit = row('td')[4].text.strip()
            # time_exits.append(time_exit)
            time_last = row('td')[5].text.strip()
            # time_lasts.append(time_last)
            #print(station_num,station)

            # 逐行写入中间站信息
            csv_write.writerow([train_code, station_num, station, time_enter, time_exit, time_last])
        # df=pd.DataFrame({'train_code':station_codes,'station_num':station_nums,'station':stations,'time_enter':time_enters,
        #                  'time_exit':time_exits,'time_last':time_lasts,})
        # # print(df.head())
    except:
        print(train_code+': error!')
        csv_write1.writerow([train_code])

    time.sleep(tim)
    # return df


# 列表转OD
def list_od(path):
    path1 = path[:-1]
    path2 = path[1:]
    path3 = list(zip(path1, path2))
    return path3


# 直达
def get_railinfos2(url):
    headers = {'User-Agent': get_headers()}
    webdata = requests.get(url, headers=headers, timeout=30)
    # print(webdata.text)
    if webdata.status_code == 200:
        # print(webdata.text)
        # # soups = BeautifulSoup(webdata.text, "html.parser")
        soups = BeautifulSoup(webdata.text, 'lxml')  # html.parser
        # soups = BeautifulSoup(webdata.content.decode("utf-8"), "xml")
        # print(soups)
        try:
            haoshi = soups.select("div.haoshi")[1].get_text()
            # print(haoshi)
            checi = soups.select("div.checi")[0].get_text()
            # print(checi)
            # html = etree.HTML(webdata.text)
            # haoshi_div = html.xpath("//div[@class='haoshi']//text()")[0]
            if type(checi) is str:
                checi, haoshi = [checi], [haoshi]
            else:
                pass
            dicti = list_dict(checi, haoshi)
        except:
            dicti = {"status": "no direct"}
    else:
        dicti = {"status": "error"}
    return dicti


# 中转
def get_transferinfos1(url):
    webdata = requests.get(url, headers=headers, timeout=30)
    if webdata.status_code == 200:
        webdata1 = webdata.text
        # print(webdata1)
        railway_list = json.loads(webdata1)["data"]["transferList"]
        # print(railway_list)
        res = [dict(item) for item in railway_list]
        # print(res)
        dict_traininfoi = [dict(item) for item in res]
        # print(dict_traininfoi)
        dict_traininfois = []
        for i in range(len(dict_traininfoi)):
            transferStation = dict_traininfoi[i]["transferStation"]
            transferTakeTime = dict_traininfoi[i]["transferTakeTime"]
            trainTransferInfos = dict_traininfoi[i]["trainTransferInfos"]
            dict_trainTransferInfos = [dict(item) for item in trainTransferInfos]
            # print(dict_trainTransferInfos)
            trainNos = []
            for j in range(len(dict_trainTransferInfos)):
                trainNoj = trainTransferInfos[j]["trainNo"]
                trainNos.append(trainNoj)
            totalRuntimeValues =dict_traininfoi[i]["totalRuntimeValue"]
            dict_i = {"Nb%s" % i:
                {
                    "transferStation": transferStation,
                    "transferTakeTime": transferTakeTime,
                    "trainNos": trainNos,
                    "totalRuntimeValues": totalRuntimeValues
                }
            }
            dict_traininfois.append(dict_i)
        # print(dict_traininfois)
    else:
        dict_traininfois = []
    return dict_traininfois


def url_encode(city):
    city1 = urllib.parse.quote(city)
    city2 = urllib.parse.quote(city1)
    # print(city2)
    return city2


def list_dict(trainname_tmp, usetime_tmp):  # 列表转为字典
    lens = len(trainname_tmp)
    dicts = []
    for i in range(lens):
        dici = {"%s" % i:
            {
                "trainName": trainname_tmp[i],
                "useTime": usetime_tmp[i]
            }
        }
        dicts.append(dici)
    # print(dicts)
    return dicts


# 制作关系数据库，生成OD网络
def get_stations_od(df_wgs, fileod_tmp):
    # 配置出发站信息
    stations = df_wgs['station'].tolist()
    station_provinces, station_citys, station_districts = df_wgs['province'].tolist(), \
                                                                df_wgs['city'].tolist(), \
                                                                df_wgs['district'].tolist()
    station_lngs, station_lats = df_wgs['wgslngs'].tolist(), \
                                 df_wgs['wgslats'].tolist()
    station_os, station_ds = [], []
    station_province_os, station_city_os, station_district_os, station_province_ds, station_city_ds, station_district_ds = [], [], [], [], [], []
    station_lng_os, station_lat_os, station_lng_ds, station_lat_ds = [], [], [], []
    for i in range(len(stations)):
        station_i = stations[i]
        station_province_o_i, station_city_o_i, station_district_o_i = station_provinces[i], \
                                                                       station_citys[i], \
                                                                       station_districts[i]
        station_lngs_i, station_lats_i = station_lngs[i], station_lats[i]
        for j in range(len(stations)):
            station_j = stations[j]
            station_province_d_j, station_city_d_j, station_district_d_j = station_provinces[j], \
                                                                           station_citys[j], \
                                                                           station_districts[j]
            station_lngs_j, station_lats_j = station_lngs[j], station_lats[j]
            if station_j != station_i:
                station_os.append(station_i)
                station_ds.append(station_j)
                station_province_os.append(station_province_o_i)
                station_city_os.append(station_city_o_i)
                station_district_os.append(station_district_o_i)
                station_province_ds.append(station_province_d_j)
                station_city_ds.append(station_city_d_j)
                station_district_ds.append(station_district_d_j)
                station_lng_os.append(station_lngs_i)
                station_lat_os.append(station_lats_i)
                station_lng_ds.append(station_lngs_j)
                station_lat_ds.append(station_lats_j)
    df_station_od = pd.DataFrame({"station_o": station_os,
                                  "station_o_province": station_province_os,
                                  "station_o_city": station_city_os,
                                  "station_o_district": station_district_os,
                                  "station_o_lng": station_lng_os,
                                  "station_o_lat": station_lat_os,
                                  "station_d": station_ds,
                                  "station_d_province": station_province_ds,
                                  "station_d_city": station_city_ds,
                                  "station_d_district": station_district_ds,
                                  "station_d_lng": station_lng_ds,
                                  "station_d_lat": station_lat_ds})
    df_station_od.to_csv(fileod_tmp, sep=',', encoding="ANSI")


def get_zhidao_zhongzhuan(station_oi, station_di, urlii1, urlii2, writer_od):
    dicti1 = get_railinfos2(urlii1)   # 直达
    # print(dicti1)
    railinfois = get_transferinfos1(urlii2)  # 中转
    # print(railinfois)
    writer_od.writerow([station_oi, station_di, dicti1, railinfois])
    time.sleep(tim)


# 爬取一定数量信息
def get_train_infos(t_length, stations_sources, stations_targets, writer_od):
    t_min, t_max = t_length[0], t_length[1]
    # print(t_min, t_max)
    for i in range(t_min, t_max):
        print(i, stations_sources[i], stations_targets[i])
        # 解码
        station_oi, station_di = url_encode(stations_sources[i]), url_encode(stations_targets[i])
        try:
            '''构建直达网址'''
            urli = "dStation=%s&aStation=%s" % (station_oi, station_di)
            urlii1 = url_base1 + urli + "&dDate=2023-12-20"
            # print(urlii1)
            '''构建中转网址'''
            urli1 = "departureStation=%s&arrivalStation=%s" % (station_oi, station_di)
            urlii2 = url_base2 + urli1 + "&departDateStr=2023-12-20"
            # print(urlii2)
            get_zhidao_zhongzhuan(stations_sources[i], stations_targets[i], urlii1, urlii2, writer_od)
        except:
            print("error!")
            continue


# 时间转换
def time_to_minutes(time_str):
    # Use regular expression to extract hours and minutes
    match = re.match(r'(\d+)时(\d+)分', time_str)
    if match:
        # Extract hours and minutes from the matched groups
        hours, minutes = map(int, match.groups())
        # Convert hours to minutes and add to total minutes
        total_minutes = hours * 60 + minutes
        return total_minutes
    else:
        print("error")
        raise ValueError("Invalid time format")


# 高铁中转即至少有一个班次为高铁，普铁中转即二者均为普铁
def get_zhida_zhongzhuan_time(station_o, station_d, zhida, zhongzhuan, writer_od):
    zhida_min, zhongzhuan_min = 0, 0
    try:
        # 直达
        dict_zhida = [dict(item) for item in eval(zhida)]
        # print(dict_zhida)
        useTime = dict_zhida[0]["0"]["useTime"]
        # print(useTime)
        zhida_min = time_to_minutes(useTime)
        # print(zhida_min)
    except:
        print("no direct")
        pass
    try:
        dict_zhongzhuan = [dict(item) for item in eval(zhongzhuan)]
        usetime_zhongzhuans = []
        for i in range(len(dict_zhongzhuan)):
            print(i)
            usetimei = dict_zhongzhuan[i]["Nb%s" % i]["totalRuntimeValues"]
            usetime_zhongzhuans.append(usetimei)
        time_zhongzhuan_min = min(usetime_zhongzhuans)
        # print(time_zhongzhuan_min)
        zhongzhuan_min = time_zhongzhuan_min
    except:
        pass
    writer_od.writerow([station_o, station_d, zhida, zhongzhuan, zhida_min, zhongzhuan_min])


def geo_map():
    # c = (
    #     Geo()
    #         .add_schema(maptype="china")
    # )

    c = (
        Map()
        .add("", [list(z) for z in zip(Faker.guangdong_city, Faker.values())], "广东")
        .set_global_opts(
            title_opts=opts.TitleOpts(title="gd"), visualmap_opts=opts.VisualMapOpts()
        )
        .render("map_gd.html")
    )

    # return c


if __name__=='__main__':
    time_start = time.time()
    print("开始时间:%s" % time_start)
    '''从携程爬取所有车次的具体信息'''
    start_url = 'https://trains.ctrip.com/TrainSchedule/'
    headers = {'User-Agent': get_headers()}
    path = os.getcwd()
    # path = "c:\\users\\lj\\desktop\\"
    train_list = ['G_undone',]  # 'D','C','K','O','T','Z',
    steps = [0, 0, 0, 0, 1]

    for file in train_list:
        filename = os.path.join(path, file) + ".csv"
        fileodn=filename.replace('.csv', '_odn.csv')
        filestations = os.path.join(path, "CDG.csv")
        filestationsn = filestations.replace('.csv', '_n.csv')

        '''第一步,获取中间站点数据'''
        filen = filename.replace('.csv', '_n.csv')
        fileun = filename.replace('.csv', '_undone.csv')

        # step1 = steps[0]
        # if step1 != 0:
        #     train_codes=pd.read_csv(filename, sep=',')['station_code'].values
        #     # print(train_codes)
        #     # 写入CSV表头
        #     with open(filen, 'a+', newline='', encoding='UTF-8') as f, \
        #             open(fileun, 'a+', newline='', encoding='utf-8') as f1:
        #         # 写入f的表头
        #         csv_write = csv.writer(f)
        #         csv_write.writerow(['train_code', 'station_num', 'station', 'time_enter', 'time_exit', 'time_last'])
        #         # df=pd.DataFrame()
        #         # undone=[]
        #         # 写入f1的表头
        #         csv_write1 = csv.writer(f1)
        #         csv_write1.writerow(['station_code'])
        #         # df=pd.DataFrame()
        #         # 建立多线程
        #         threads = []
        #         for i in range(0, len(train_codes)):  #　len(train_codes)2659
        #         # for i in range(1, 10):
        #             train_code = train_codes[i]
        #             threads.append(
        #                 threading.Thread(target=crawl_train, args=(start_url, train_code,
        #                                                            csv_write, csv_write1))
        #             )
        #         # 开启多线程
        #         for thread in threads:
        #             thread.start()
        #         # 关闭多线程
        #         for thread in threads:
        #             thread.join()
        #         # dfundone = pd.DataFrame({'station_code': undone})
        #         # dfundone.to_csv(fileun,sep=',',encoding='utf-8')
        # else:
        #     pass
        print("已完成中间站点爬取！")

        '''第二步，地理编码'''
        filecdgs = os.path.join(path, "CDG_stations.csv")
        filecdgsn = filecdgs.replace('.csv', '_n.csv')
        filewgs = filecdgs.replace('.csv', '_wgs.csv')
        filecdgsn_time = filecdgs.replace('.csv', '_time.csv')

        # step2 = steps[1]
        # if step2 == 0:
        #     pass
        # else:
        #     stations = pd.read_csv(filecdgs, sep=',', encoding='ANSI')['station'].values
        #     # print(stations)
        #     # 逐行写入CSV
        #     with open(filewgs, 'a+', newline='', encoding='ANSI') as f:
        #         # 写入f的表头
        #         csv_write3 = csv.writer(f)
        #         csv_write3.writerow(['station', 'province', 'city', 'district',
        #                              'lngs', 'lats', "wgslngs", "wgslats"])
        #         # for i in range(5):
        #         for i in range(len(stations)):
        #             print(i)
        #             station_i = stations[i] + "站"
        #             station_info = geocode_multi_infos.Geocodeamap(station_i, "").geocode()
        #             csv_write3.writerow(station_info)
        #             time.sleep(random.randint(2, 3))
        print("已完成地理编码！")

        '''第三步,制作列车站OD图'''
        filecdgs_tmp = filecdgs.replace('.csv', '_tmp.csv')
        fileod = os.path.join(path, "CDG_stations_od.csv")
        fileod_tmp = fileod.replace(".csv", "_tmp.csv")

        # step3 = steps[2]
        # if step3 == 0:
        #     pass
        # else:
        #     df_stations = pd.read_csv(filestations, sep=',', encoding="ANSI")
        #     df_wgs = pd.read_csv(filewgs, sep=',', encoding="ANSI")
        #     get_stations_od(df_wgs, fileod_tmp)  # 生成OD表
        print("已完成列车OD图制作！")

        '''第四步,爬取两两站点的旅行时间'''
        filestations_od = filestations.replace('.csv', '_od.csv')
        filestations_od_tmp = filestations_od.replace('.csv', '_tmp.csv')

        # step4 = steps[3]
        # if step4 == 0:
        #     pass
        # else:
        #     # df_stations = pd.read_csv(fileod_tmp, sep=',', encoding="ANSI")
        #     # source_province = ["上海市"]
        #     # target_province = ["山东省", "河南省", "安徽省",
        #     #                    "湖北省", "江西省", "福建省",
        #     #                    "浙江省", "江苏省"]
        #     # df_stations_od = df_stations.loc[(df_stations["station_o_province"].isin(source_province)) &
        #     #                                  df_stations["station_d_province"].isin(target_province)]
        #     # df_stations_od.to_csv(filestations_od_tmp, sep=',', encoding="ANSI")
        #     # print("已完成目标站点提取！")
        #     df_stations_od1 = pd.read_csv(filestations_od_tmp, sep=',', encoding="ANSI")
        #     stations_sources = df_stations_od1["station_o"].tolist()
        #     stations_targets = df_stations_od1["station_d"].tolist()
        #     # print(len(stations_sources))
        #
        #     '''get rail infos'''
        #     url_base1 = "https://trains.ctrip.com/webapp/train/list?ticketType=0&"  # 直达
        #     # https://trains.ctrip.com/webapp/train/list?ticketType=0&dStation=%E5%90%89%E6%9E%97&aStation=%E5%8F%8C%E5%90%89&dDate=2023-12-18&rDate=&hubCityName=&highSpeedOnly=true
        #     url_base2 = "https://trains.ctrip.com/pages/booking/getTransferList?"  # 中转
        #     '''get nonstop and transfer rails at one time'''
        #     writer_od = csv.writer(open(filestations_od, "a", newline='', encoding='ANSI'))
        #     csvheader_od = ["station_o", "station_d", "traininfos", "traininfos1"]
        #     writer_od.writerow(csvheader_od)
        #     # 建立多线程
        #     threadx = []
        #     thx = [(0, 1000), (1000, 2000), (2000, len(stations_sources))]
        #     # thx = [(0, 1)]
        #     for t in thx:
        #         threadx.append(
        #             threading.Thread(target=get_train_infos, args=(t, stations_sources, stations_targets, writer_od))
        #         )
        #     # 开启多线程
        #     for thread in threadx:
        #         thread.start()
        #     # 关闭多线程
        #     for thread in threadx:
        #         thread.join()
        print("已完成旅行时间爬取！")

        '''第五步,提取时间'''
        filestations_od_time = filestations_od.replace('.csv', '_n.csv')
        filestations_od_time_tmp = filestations_od_time.replace('.csv', '_tmp.csv')

        # step5 = steps[4]
        # if step5 == 0:
        #     pass
        # else:
        #     df_od_time = pd.read_csv(filestations_od, sep=',', encoding="ANSI")
        #     station_os, station_ds = df_od_time["station_o"].tolist(), \
        #                              df_od_time["station_d"].tolist()
        #     li_zhida, li_zhongzhuan = df_od_time["traininfos"].tolist(), \
        #                               df_od_time["traininfos1"].tolist()
        #     writer_od = csv.writer(open(filestations_od_time_tmp, "a", newline='', encoding='ANSI'))
        #     csvheader_od = ["station_o", "station_d", "traininfos", "traininfos1",
        #                     "time_zhida", "time_zhongzhuan"]
        #     writer_od.writerow(csvheader_od)
        #
        #     for i in range(0, len(li_zhida)):  # len(li_zhida)
        #         print(i)
        #         station_o_i, station_d_i = station_os[i], station_ds[i]
        #         zhida_i, zhongzhuan_i = li_zhida[i], li_zhongzhuan[i]
        #         get_zhida_zhongzhuan_time(station_o_i, station_d_i,
        #                                   zhida_i, zhongzhuan_i, writer_od)
        #
        #     # df_time_tmp = pd.read_csv(filestations_od, sep=',', encoding="ANSI")
        #     # df_wgs = pd.read_csv(filewgs, sep=',', encoding="ANSI")
        #     # df_travel_time = pd.merge(left=df_time_tmp, right=df_wgs,
        #     #                           left_on=["station_o", "station_d"], right_on=["station", "station"])
        #     # print(df_travel_time.head())
        print("已完成最短旅行时间提取！")

        '''第六步，可视化'''
        c = geo_map()
        # c.render()
        print("已绘制好地图！")
    print('great!')
    time_end = time.time()
    print('结束时间:%s,共用时:%s' % (time_end, (time_end - time_start)))