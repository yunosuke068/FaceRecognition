
import numpy as np
import cv2
import os, glob
from tqdm import tqdm
import time

import movie_func, sql_func, my_func

# path = 'db/split_db/FaceDB1_5.db'
path = 'db/split_db/FaceDB6_.db'
# path = 'db/FaceDB_test.db'
sql = sql_func.FaceDB(path)

# ids = [1,2,3,4,5]
ids = range(25,50)#[21,22,23,24]
for id in ids:
    path2 = f'db/split_db/FaceDB{id}.db'
    if os.path.exists(path2):
        os.remove(path2)
    sql2 = sql_func.FaceDB(path2)

    movies_records = sql.GetRecords('Movies',['*'],{'id':id})
    for mr in movies_records:
        print(mr)
        sql2.UpdateRecords('Movies',{'name':mr['name']},mr)

        movie_id = mr['id']
        complete_records = sql.GetRecords('Completes',['*'],{'movie_id':movie_id})
        for cr in complete_records:
            sql2.UpdateRecords('Completes',{'movie_id':movie_id},cr)

        faces_records = sql.GetRecords('Faces',['*'],{'movie_id':movie_id})
        progressbar = tqdm(faces_records,ncols=0)
        progressbar.set_description(f"Faces")
        for fr in progressbar:
            sql2.UpdateRecords('Faces',{'id':fr['id']},fr)

        subject_records = sql.GetRecords('Subjects',['*'],{'movie_id':movie_id})
        progressbar = tqdm(subject_records,ncols=0)
        progressbar.set_description(f"Subjects&FaceSubjects")
        for sr in progressbar:
            sql2.UpdateRecords('Subjects',{'id':sr['id']},sr)
            for fsr in sql.GetRecords('FaceSubjects',['*'],{'subject_id':sr['id']}):
                sql2.UpdateRecords('FaceSubjects',{'id':fsr['id']},fsr)

            # for br in sql.GetRecords('Bonds',['*'],{'subject_id_0':sr['id']}):
            #     sql2.UpdateRecords('Bonds',{'id':br['id']},br)
