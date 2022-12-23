from common_module import movie_func, my_func
import numpy as np
import cv2
import os, glob,sys
from tqdm import tqdm
import time

from face_recognition_module import sql_func


def Initialize(sql):
    for path2 in glob.glob("db/split_db/*.db"):
        # print(path2)
        sql2 = sql_func.FaceDB(path2)
        mrs = sql2.GetRecords(table='Movies')
        for mr in mrs:
            sql.UpdateRecords('Movies',{'id':mr['id']},mr)

        crs = sql2.GetRecords(table='Completes')
        for cr in crs:
            sql.UpdateRecords('Completes',{'movie_id':mr['id']},cr)

def Update(sql):
    mr1s = sql.GetRecords(table='Movies')
    for mr1 in mr1s:
        mr1_id = mr1['id']
        sql2 = sql_func.FaceDB(f"db/split_db/FaceDB{mr1_id}.db")
        sql2.UpdateRecords('Movies',{'name':mr1['name']},mr1)

        cr1s = sql.GetRecords('Completes',['*'],{'movie_id':mr1_id})
        # print(cr1s[0])
        sql2.UpdateRecords('Completes',{'movie_id':mr1_id},cr1s[0])

if __name__ == '__main__':
    path = "db/MovieManage.db"

    sql = sql_func.FaceDB(path)
    args = sys.argv
    if len (args) < 3:
        # print(args)
        if int(args[1]) == 0: # manageファイルをアップデート
            print('init')
            Initialize(sql)

        elif int(args[1]) == 1: # FaceDBをアップデート
            print('update')
            Update(sql)
