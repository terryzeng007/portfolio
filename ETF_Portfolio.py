# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 13:19:27 2018

@author: Terry
"""
import pandas as pd
import numpy as np
import os, datetime, pypfopt
import portfolio_support as ps
import Mongo_Class_20210928 as mc

# 1. Parameter Setting
class Para:
    
    # Date
    st = '2003-01-01' # 回测起始期
    start = '2003-01-01'#'20050101' # 回测起始期
    day_end =  '2023-09-10'  #t Actual Last Day on Database for update advance
    today= datetime.datetime.now().strftime('%Y-%m-%d')

    # Backtest Application
    class select:
        position = 100000 # 初始本金 /2000000
        cost = 0/10000 # finance cost

        buffer = 0# 本版本使用buffer 0 
        buy_num = 20 # raw stock number
        buy_adj = 20 # target stock number
        buy_add = 5
        
        backup = 100 #保留3000HKD
        bottom = 100 # 购买最低的价格
        return_T = 252 # TradeDate
        annual_T = 252 # TradeDate
        wmax = 0.2 #最大权重10% 和 选股数目匹配
        wmin = 0.02
        group_split = 6
        
    class adjust:
        cycle = 'week' # cycle frequency
        train_num = 52*3#52*3     # Backtest cycle, train_w = 4
        pred_num = 8    # Predict cycle, week = 3
        week_day = 'Monday'   # week_day = 'Monday' 
        st = 'w-mon'  # week_day_2 = 'w-mon'   
        method = ['ew','r_parity'] # opt method
        accumulate = 'true'

    project_path = 'D:/Git_Project/portfolio_etf'

para = Para()

# 2. Data Preparation
def document_prepare(para):
    '''获取支持的数据

    Parameters
    ==========
    para: default setting
    output:
        -1.path_to_save
        -2.stock list path
    '''

    # path setting
    folder_output = para.project_path + '/result_portfolio/t' + str(para.adjust.train_num) + '_p' + str(
        para.adjust.pred_num) + '_cash_' + str(para.select.position) + '_' + str(
        para.select.cost) + '_' + para.adjust.accumulate + '_' + para.start + '_' + '_wmax' + str(
        para.select.wmax) + '_' + '_wmin' + str(para.select.wmin)
    contain_name = [folder_output + '/' + x for x in ['1_stats', '3_performance']]

    # folder prepare
    for path in [folder_output] + contain_name:
        ps.mkdir(path)

    # (1)  Stock Path & Price Data
    df = pd.read_excel('data/price.xlsx')
    df = df.set_index('date', drop=True)
    df = df.dropna(how='all', axis=1)

    df_r = np.log(df / df.shift(1))
    df_r = df_r.iloc[1:, :]
    df_r['holiday'] = df_r.apply(lambda x: x.mean(), axis=1)
    df_r = df_r[df_r['holiday'] != 0]

    df_r['holiday'] = df_r.iloc[:, :9].apply(lambda x: x.mean(), axis=1)
    df_r = df_r[df_r['holiday'] != 0]
    df_r = df_r.iloc[:, :-1]

    df = df.loc[df_r.index]

    # (2)  Interest Rate
    df_ir = None
    #db_ir = mc.MongoBase('boss', 'Cta', 'ir')
    #df_ir = db_ir.readDB({}, {'_id': 0})
    #df_ir = df_ir[['date', 'ir_us_10yr', 'ir_us_1yr', 'libor_3m', 'libor_1m']]
    #df_ir['date'] = df_ir['date'].apply(lambda x: (pd.to_datetime(x)).strftime('%Y-%m-%d'))
    #df_ir = df_ir.fillna(method='pad').set_index('date', drop=True)
    #df_ir['ir'] = df_ir.apply(lambda x: x.mean(), axis=1)

    # (3) Ranking Model Test Date
    list_rank_D = ps.get_date_list(para.adjust.cycle, para.adjust.pred_num, para)
    list_rank_D['train_end'] = list_rank_D['predict_st'].apply(lambda x: x - datetime.timedelta(days=1))
    list_rank_D['predict_end'] = list_rank_D['predict_st'].shift(-1).apply(lambda x: x - datetime.timedelta(days=1))
    day_judge = para.adjust.train_num * 7 if para.adjust.cycle == 'week' else para.adjust.train_num * 30
    list_rank_D['train_st'] = list_rank_D['train_end'].apply(lambda x: x - datetime.timedelta(days=day_judge))
    if para.adjust.cycle == 'week':
        list_rank_D['predict_end'].iloc[-1] = list_rank_D['train_end'].iloc[-1] + datetime.timedelta(
            days=para.adjust.pred_num * 7)
    list_rank_D = list_rank_D.reset_index(drop=False).dropna()
    list_rank_D = list_rank_D.astype('str')
    if para.adjust.accumulate == 'true':
        list_rank_D['train_st'] = list_rank_D['train_st'].iloc[0]

    return folder_output, df, df_r, df_ir, list_rank_D

folder_output, df, df_r, df_ir, list_rank_D = document_prepare(para)


# 3. Backtest Training
error_list = []
for i in range(len(list_rank_D)):

    st, train_end, end, train_st = list_rank_D[['predict_st', 'train_end', 'predict_end', 'train_st']].iloc[i].values

    # Get Stock List
    return_all = df_r.loc[train_st:end].dropna(how='all', axis=1)
    stock_p_all = df.loc[train_st:end].dropna(how='all', axis=1)

    # Split Data
    stock_return = return_all.loc[train_st:train_end].dropna(how='all', axis=1)
    stock_price = stock_p_all.loc[train_st:train_end].dropna(how='all', axis=1)

    # Portfolio Record
    if i > 0:
        path_po = ps.get_all_path(folder_output + '/3_performance', 'csv', 'raw')
        po_record = pd.read_csv(path_po[i - 1])
        para.select.position = po_record[['g_5_' + x + '_lot' for x in para.adjust.method]].iloc[-1].mean()
        po_record_i = po_record[['g_5_' + x + '_lot' for x in para.adjust.method]].iloc[-1]
    else:
        po_record_i = pd.DataFrame([para.select.position] * 2, index=['g_5_ew_lot', 'g_5_r_parity_lot'])
        po_record_i = po_record_i.T.iloc[-1]
    print(i, para.select.position, st)

    # Get Training Data
    if len(stock_return) > 0:

        # Constrainst Dataset
        stock_last = []

        # Market Stats
        #df_ir_i = df_ir.loc[train_st:train_end, 'ir']
        #rf = df_ir_i.mean() / 100 if len(df_ir_i) > 0 else 0.03

        # Stats
        #var, mean, std, sharper, cov = optim.return_stats(stock_return, para, rf, 'A')
        stats = stock_price.iloc[-1:, :].T
        stats['last_tradedate'] = stats.columns[0]
        stats['code'] = stats.index
        stats['name'] = stats.index
        stats.columns.values[0] = 'price'
        stats['lot'] = 1
        stats['lotsize'] = stats['lot'] * stats['price']

        stk_list = list(stats['code'])
        n_stk = len(stk_list)

        # Equal Weight
        wts_ew = np.array([1 / n_stk] * n_stk)

        # Risk Parity Weight
        df_cov = stock_return.cov()
        model = pypfopt.hierarchical_portfolio.HRPOpt(stock_return, df_cov)
        w = model.optimize(linkage_method="single")
        df_w = pd.DataFrame(w, index=w.keys()).T
        df_w = df_w.loc[stock_return.columns]
        wts_rp = np.array(df_w.iloc[:,0])

        # All Weight Output
        wts_output = [wts_ew, wts_rp]
        all_w = pd.DataFrame(wts_output, index=para.adjust.method, columns=stk_list).T
        all_w.index.name = 'code'
        lot_name = [x + '_lot' for x in para.adjust.method]

        # Lot Value Optimization
        ew_stats, ew_lot = ps.get_lot_opt(stats, stk_list, wts_ew, para, po_record_i['g_5_ew_lot'])
        rp_stats, rp_lot = ps.get_lot_opt(stats, stk_list, wts_rp, para, po_record_i['g_5_r_parity_lot'])
        all_stats = [ew_stats, rp_stats]

        all_lot = pd.DataFrame([ew_lot, rp_lot], index=lot_name).T
        all_lot.index = stk_list
        all_lot.index.name = 'code'

        #  Portfolio Return
        if i != len(list_rank_D) - 1:

            # Raw Return
            code_list = list(stock_price.columns)
            hk_p_group = None
            for c in code_list:
                df_i = stock_p_all[[c]]
                df_i.columns = ['close']
                df_i['mom1'] = df_i['close'] / df_i['close'].shift(1) - 1
                df_i['open'] = df_i['close'].shift(1)
                df_i['code'] = c
                df_i = df_i.loc[st:end]
                df_i['tradeDate'] = df_i.index
                df_i['tradeDate'] = df_i['tradeDate'].apply(lambda x: x.strftime('%Y%m%d'))
                df_i = df_i.reset_index(drop=True)
                hk_p_group = pd.concat([hk_p_group, df_i], axis=0)

            hk_p_group = hk_p_group.set_index('code', drop=True)
            hk_p_group = hk_p_group.loc[stk_list]
            hk_p_group = pd.merge(hk_p_group, all_w, left_index=True, right_index=True)
            hk_p_group = pd.merge(hk_p_group, all_lot, left_index=True, right_index=True)
            hk_p_group = hk_p_group.reset_index(drop=False)

            hk_p_group = hk_p_group.groupby('code').apply(ps.get_value, lot_name)
            hk_p_group[lot_name] = hk_p_group[lot_name] / 100
            hk_p_group = hk_p_group.reset_index(drop=True)
            value_group = hk_p_group.groupby('code').apply(ps.get_buy_return, st, end, 5, para, lot_name)

            # Portfolio Combine
            pofolio = value_group.groupby('tradeDate').sum()
            pofolio['trade_mark'] = (pofolio['trade_mark'] / pofolio['trade_mark']).fillna(0)
            portfolio_all = pofolio[['g_' + str(5) + '_' + w for w in para.adjust.method + lot_name]]

        if i != len(list_rank_D) - 1:
            # Return ReBase
            col_g = [x for x in portfolio_all.columns if x.find('trade_mark') < 0 and x.find('lot') < 0]
            col_lot = [x for x in portfolio_all.columns if x.find('lot') > 0]
            col_5 = ['g_' + str(para.select.group_split - 1) + '_' + x for x in para.adjust.method]

            po_r = portfolio_all[col_g] / portfolio_all[col_g].shift(1) - 1
            po_r.iloc[:1, :] = portfolio_all[col_g].iloc[:1, :] - 1

            po_r[col_lot] = portfolio_all[col_lot] / portfolio_all[col_lot].shift(1) - 1
            po_r.loc[po_r.index[0], col_lot] = po_r.loc[po_r.index[0], col_5].values

            para.select.position = portfolio_all[['g_5_' + x for x in lot_name]].iloc[-1].mean()

            po_r = po_r.round(4)

            po_r['pred_st'] = st
            po_r.to_csv(folder_output + '/3_performance/r_' + st + '.csv', index=True)

            portfolio_all['pred_st'] = st
            portfolio_all.to_csv(folder_output + '/3_performance/raw_' + st + '.csv', index=True)
    else:
        print('error', i, st, end)
        error_list.append(st)

print('total error', error_list)


def maxdrawdown(arr):
	i = np.argmax((np.maximum.accumulate(arr) - arr)/np.maximum.accumulate(arr)) # end of the period
	j = np.argmax(arr[:i]) # start of period
	return (1-arr[i]/arr[j])

# (4.1) Performance
path_perm = ps.get_all_path(folder_output + '/3_performance','csv', 'r_')
df_po_r = ps.get_data_path( path_perm, 'r_', 'csv')
df_po_r['tradeDate'] = df_po_r['tradeDate'].apply(lambda x: str(x))
df_po_r.to_csv(folder_output + '/po_r_' + para.today + '.csv')
df_po_r = df_po_r.set_index('tradeDate', drop=True)

df_r_cum = (df_po_r.drop(['pred_st'], axis=1)+1).cumprod()
df_r_cum['r_parity_leverage3'] = (df_po_r['g_5_r_parity']*3 + 1).cumprod()
df_r_cum = df_r_cum.fillna(method='pad')
df_r_cum = df_r_cum.dropna()
df_r_cum = df_r_cum/df_r_cum.iloc[0,:]
df_r_cum.index = pd.to_datetime(df_r_cum.index)
col_g5 = ['g_5_ew','g_5_r_parity','r_parity_leverage3']
df_r_cum[col_g5].plot(grid=True)


