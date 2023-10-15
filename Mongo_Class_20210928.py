# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 16:26:24 2019

@author: Owner
"""
from pymongo import MongoClient
import json
import datetime
import pandas as pd
import pymongo

class MongoBase:
    def __init__(self,db_owner,database,collection):
        self.db_owner = db_owner
        self.database = database
        self.collection_name = collection
        self.collection = collection
        self.openDB()
    def openDB(self):
        # CN Inside
        if self.db_owner =='amy_inside':
            user='root'
            passwd='utf8utf8'
            host='192.168.0.21'
            port='27017'
            auth_db='admin'
            uri = "mongodb://"+user+":"+passwd+"@"+host+":"+port+"/"+auth_db+"?authMechanism=SCRAM-SHA-1"
        if self.db_owner =='boss_inside':
            user='root'
            passwd='utf8utf8'
            host='192.168.0.20'
            port='27017'
            auth_db='admin'
            uri = "mongodb://"+user+":"+passwd+"@"+host+":"+port+"/"+auth_db+"?authMechanism=SCRAM-SHA-1"

        # CN Outside
        if self.db_owner == 'amy':
            user='root'
            passwd='utf8utf8'
            host='jztxtech.tpddns.cn'
            port='27011'
            auth_db='admin'
            uri = "mongodb://"+user+":"+passwd+"@"+host+":"+port+"/"+auth_db+"?authMechanism=SCRAM-SHA-1"
        if self.db_owner == 'boss':
            user='root'
            passwd='utf8utf8'
            host='jztxtech.tpddns.cn'
            port='27010'
            auth_db='admin'
            uri = "mongodb://"+user+":"+passwd+"@"+host+":"+port+"/"+auth_db+"?authMechanism=SCRAM-SHA-1"
        if self.db_owner == 'wind_outside':
            user='root'
            passwd='utf8utf8'
            host='61.238.86.10'
            port='27017'
            auth_db='admin'
            uri = "mongodb://"+user+":"+passwd+"@"+host+":"+port+"/"+auth_db+"?authMechanism=SCRAM-SHA-1"

        # HK Outside
        if self.db_owner =='wind_outside':
            user='root'
            passwd='utf8utf8'
            host='61.238.86.10'
            port='27017'
            auth_db='admin'
            uri = "mongodb://"+user+":"+passwd+"@"+host+":"+port+"/"+auth_db+"?authMechanism=SCRAM-SHA-1"

        self.con = MongoClient(uri, connect=False)
        self.db=self.con[self.database]
        self.collection=self.db[self.collection]
 
    def readDB(self,filter_con={},filter_con_2={}):
        data_list = self.collection.find(filter_con,filter_con_2)
        data = pd.DataFrame(list(data_list))
        return data

    def readDB_limit(self,limitnum=1,skipnum=0):
        data_list = self.collection.find().limit(limitnum).skip(skipnum)
        data = pd.DataFrame(list(data_list))
        return data

    def insertDB(self,df):
        self.collection.insert_many(json.loads(df.T.to_json()).values())
        print(format(self.collection_name)+'_update_time: '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
    
    def updateDB_col(self,col1,col2):
        self.collection.update_many({},{'$rename':{col1:col2}})
        
    def drop_col(self,col):
        self.collection.update_many({},{'$unset': {col:""}})
    
    def dropDB(self,filter_con={}):
        result = self.collection.delete_many(filter_con)
        print('Delete_num: '+str(result.deleted_count))
        
    def dropDB_one(self,filter_con={}):
        result = self.collection.delete_one(filter_con)
        print('Delete_num: '+str(result.deleted_count))

    def dropDB_filter(self,filter_con={}):
        result = self.collection.delete_many(filter_con)
        print('Delete_num: '+str(result.deleted_count))

    def dropDB_all(self):
        self.collection.drop()
        print('done')
    
    def closeDB(self):
        self.con.close()   
        
    def updateDB(self, condition1, condition2, many):
        '''
        Parameter
        -------
        collection.update_one(filter, new_values, upsert=False, bypass_document_validation=False, collat​​ion=None, array_filters=None, session=None)
            ‘filter’：与要更新的文档匹配的查询。
            ‘new_values’:适用的修改。
            ‘upsert’(可选)：如果是 “True”，则在没有文档与过滤器匹配时执行插入。
            ‘bypass_document_validation’(可选)：如果是 “True”，则允许写入文档级验证的 opt-out。默认值为 “False”。
            ‘collation’(可选)：类的实例：‘~pymongo.collat​​ion.Collat​​ion’。此选项仅在 MongoDB 3.4 及更高版本上受支持。
            ‘array_filters’(可选)：指定更新应应用哪些数组元素的过滤器列表。需要 MongoDB 3.6+。
            ‘session’(可选)：一个类：'~pymongo.client_session.ClientSession'。        
        '''
        if many==True:
            res = self.collection.update_many(condition1, condition2)
        else:
            res = self.collection.update_one(condition1, condition2)
        print(res, res.modified_count)
    
    #def copyDB(self, name):
    #    self.db.copyDatabase
    #    self.db.createcollection(name);
    #    self.db[name].createIndex({"custno":1},{"background":1})    
    #    self.collection.aggregate({'$match': {}}, {'$out': name})
        
    def changeDB_name(self,name_new):
        self.collection.rename(name_new)
    
    def get_index(self,col):
        self.collection.create_index([(col, pymongo.ASCENDING)])
    
    def get_db_name(self):
        db_name=self.con.database_names()
        return db_name
        
    def get_sheet_name(self):
        sheet_name=self.db.list_collection_names(session=None)
        return sheet_name

    def sort_db(self,col1,col2):
        self.collection.find().sort([(col1, pymongo.ASCENDING), (col2, pymongo.ASCENDING)])
        
    def groupby_count(self,col1):
        groupby =col1
        group = {'_id': "$%s" % (groupby if groupby else None),
                 'count': {'$sum': 1} }
        ret = self.collection.aggregate(
                 [{'$group': group}] )
        data = []
        for i in ret:
            data.append(i)
        data = pd.DataFrame(data)
        return data
    
    def groupby_match_count(self,start_time,end_time,col1,col_time):
        match = {col_time: {
         '$gte': start_time,
         '$lte': end_time, }}
        groupby =col1
        group = {'_id': "$%s" % (groupby if groupby else None),
                 'count': {'$sum': 1} }
        ret = self.collection.aggregate(
                 [ {'$match': match},
                 {'$group': group},] )
        data = []
        for i in ret:
            data.append(i)
        data = pd.DataFrame(data)
        return data

    
    def groupby_first(self,col1,col2):
        groupby =col1
        group = {'_id': "$%s" % (groupby if groupby else None),
                 'first_'+col2: {'$first': '$'+col2} }
        ret = self.collection.aggregate(
                 [{'$group': group}] )
        data = []
        for i in ret:
            data.append(i)
        data = pd.DataFrame(data)
        return data
    

    def groupby_last_2(self,start_time,col_time,col1,col2):
        '''增加时间约束'''       
        match = {col_time: {
         '$gte': start_time }}
        groupby =col1
        group = {'_id': "$%s" % (groupby if groupby else None),
                 'last_'+col2: {'$last': '$'+col2} }
        ret = self.collection.aggregate(
                 [ {'$match': match},
                 {'$group': group}])
        data = []
        for i in ret:
            data.append(i)
        data = pd.DataFrame(data)
        return data
    
    def get_col_name(self):
        data_list = self.collection.find().limit(1).skip(0)
        data = pd.DataFrame(list(data_list))
        data_col = list(data.columns)
        return data_col
    
#%%
if __name__ == '__main__':
    mongo_test = MongoBase('database_name','collenction_name')
    data=mongo_test.readDB()
    mongo_test.insertDB(data)


'''
if __name__ == '__main__':
    with MongoBase('local','database_name','collenction_name') as mongo_test:
        data = mongo_test.readDB({},{})
        mongo_test.insertDB(data)

'''
