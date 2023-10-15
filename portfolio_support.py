# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 10:48:36 2023

@author: Administrator
"""

import os, datetime, math
import numpy as np
import pandas as pd

def get_date_list(cycle, cycle_num, para):
    if cycle == 'day':
        list_rank_D = pd.DataFrame(pd.date_range(para.st,para.today),columns = ['predict_st'],index=pd.date_range(para.st,para.today))

    if cycle == 'week':
        list_rank_D = pd.DataFrame(pd.date_range(para.st,(datetime.datetime.now()+datetime.timedelta(days=7*cycle_num)).strftime('%Y-%m-%d'),freq= str(cycle_num)+ para.adjust.st),columns = ['predict_st'])

    if cycle == 'month':
        list_rank_D = pd.DataFrame(pd.date_range(para.st,(datetime.datetime.now()+datetime.timedelta(days=30*cycle_num)).strftime('%Y-%m-%d'),freq= str(cycle_num)+'M'),columns = ['predict_st'])
    
    return list_rank_D

def mkdir(path):
	folder = os.path.exists(path)
    
    # 判断是否存在文件夹如果不存在则创建为文件夹
	if not folder:              
        # makedirs 创建文件时如果路径不存在会创建这个路径
		os.makedirs(path)            
		print ("---  new folder...  ---")
		print ("---  OK  ---")
	else:
		print( "---  There is this folder!  ---")


def get_all_path(pdf_path, xtype_2, target):

    #取得列表中所有的type文件
    def collect_xls(list_collect,type1):
        for each_element in list_collect:
            if isinstance(each_element,list):
                collect_xls(each_element,type1)
            elif each_element.endswith(type1):
                  typedata.insert(0,each_element)
        return typedata#读取所有文件夹中的xls文件
    
    #遍历路径文件夹
    def read_xls(path,type2,i):
        for file in os.walk(path):
            for each_list in file[2]:
                file_path=file[0]+"/"+each_list#os.walk()函数返回三个参数：路径，子文件夹，路径下的文件，利用字符串拼接file[0]和file[2]得到文件的路径
                name.insert(0,file_path)
            all_xls = collect_xls(name, type2)#遍历所有type文件路径并读取数据
        return all_xls
    
    typedata, name = [], []
    all_pdf = read_xls(pdf_path,xtype_2,0)
    
    if len(target)>0:
        all_pdf = [x for x in all_pdf if x.find(target)>=0]
    
    all_pdf = list(sorted(all_pdf))
    return all_pdf


    
def match_index(df, col, col_list, col_target):
    df.index = df[col]
    df = df.loc[col_list, col_target]
    df = df.reset_index(drop=True)
    return df


def get_lot_opt(stats, stk_list, true_w, para, position):
    
    lot_num = match_index(stats, 'code', stk_list, ['code','lotsize','name','lot'])
    lotsize = lot_num['lotsize']
    
    lot_num['w_true'] = true_w
    lot_num['position_true'] = lot_num['w_true']*position
    
    lot_num['lot_num'] = lot_num['position_true']/lot_num['lotsize']
    lot_num['lot_num_max'] = lot_num['lot_num'].apply(lambda x: math.ceil(x) if x>1 else 1)
    lot_num['lot_num_min'] = lot_num['lot_num'].apply(lambda x: math.floor(x) if x>1 else 1)
    
    lot_num_max = lot_num['lot_num_max']
    lot_num_min = lot_num['lot_num_min']
    
    def opt_lot(lot):
        y = np.sum( (((lot)*lotsize)/position - true_w)**2) ++ np.sum( (lot - lot_num_max)**2)/(lot_num_max.mean()**2)+ np.sum( (lot - lot_num_min)**2)/(lot_num_min.mean()**2)
        return y
    
    def lot_buy(lot):
        return opt_lot(lot)
    
    lot_num['lot_num_opt'] = lot_num['lot_num'].apply(lambda x: round(x,0))      
    lot_num['lot_num_opt'] = lot_num['lot_num_opt'] .apply(lambda x: round(x,0))
    lot_num['lot_num_opt'] = lot_num['lot_num_opt'] .apply(lambda x: 1 if x==0 else x)
    
    lot_num['position_opt'] = lot_num['lot_num_opt']*lot_num['lotsize']
    lot_num['position_opt_cum'] = lot_num['position_opt'].cumsum()
    
    lot_num['w_opt'] =  (lot_num['position_opt'])/position
    #lot_num['error'] = opt_lot( lot_num['lot_num_opt'])               
    lot_num['error'] = lot_num['position_opt'].sum() - position          
    lot_num['buy_opt'] = lot_num['lot_num_opt']*lot_num['lot']

    wts_lot_buy =  lot_num['lot_num_opt']
    
    return lot_num, wts_lot_buy 



def get_value(df, col_list):
    for col in col_list:
        df[col] = df['close']*df[col]*100
        df[col] = df[col].iloc[0]
    return df

def get_buy_return(df, st, end, group, para, lot_name):
    
    if group==para.select.group_split-1:
        test_col =  para.adjust.method + lot_name
    else:
        test_col =  para.adjust.method 
    
    df_r = df[['open','close', 'mom1', 'code','tradeDate'] +test_col].sort_values(by='tradeDate')
    df_r = df_r.reset_index(drop=True)
    
    df_r['R_Adj'] = df_r['mom1']
    df_r.loc[0, 'R_Adj'] = df_r.loc[0, 'R_Adj'] - para.select.cost*2* (para.select.buy_adj-para.select.buffer)/para.select.buy_adj
    df_r['R_Adj_Cum'] = (1+df_r['R_Adj']).cumprod()
    
    for w in test_col:
        df_r['g_' + str(group) +'_'+ w] = df_r['R_Adj_Cum'] *df_r[w]
    
    df_r['trade_mark'] = 1
    df_r = df_r.drop_duplicates()
    
    all_d = pd.date_range(st, end)
    all_d = pd.DataFrame(all_d,  columns=['tradeDate'] )
    all_d['tradeDate'] = all_d['tradeDate'].apply(lambda x: x.strftime('%Y%m%d'))
    
    df_rp2 = pd.merge(all_d, df_r, how='left')
    for col in ['trade_mark','R_Adj']:
        df_rp2[col] = df_rp2[col].fillna(0)
        
    df_rp2 = df_rp2.fillna(method='pad')
    df_rp2 = df_rp2.fillna(method='backfill')
    
    return df_rp2



def get_data_path(path_0, name, types):
    path_0 = pd.DataFrame(path_0,columns=['path'])
    path_0['judge'] =path_0 ['path'].apply(lambda x: x.find(name))
    path_stats = path_0[path_0['judge']>0]
    df_all_stats = pd.DataFrame()
    for p in path_stats['path']:
        if types=='excel':
            df_0 = pd.read_excel(p)
        elif types=='csv':
            df_0 = pd.read_csv(p)
        df_all_stats = pd.concat([df_all_stats,df_0], axis=0)
    df_all_stats = df_all_stats.drop_duplicates()
    return df_all_stats

#%%