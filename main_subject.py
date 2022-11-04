# FacesテーブルとSubjectsテーブルの中間テーブルの生成

from common_module import movie_func
import numpy as np
import cv2
import os, glob, sys
from tqdm import tqdm
import time

from face_recognition_module import sql_func, my_func
from operator import itemgetter

args = sys.argv

if len(args) < 2:
    # return 0
    sys.exit()

path = args[1]# 'db/split_db/FaceDB1.db'
sql = sql_func.FaceDB(path)

movie_records = sql.GetRecords('Movies',['id'],{}) # Movieテーブルのレコード一覧
for movie_record in movie_records:
    # movie_record = movie_records[0]
    movie_id = movie_record['id']

    print(movie_record)
    if (sql.GetRecords('Completes',['movie_id','flag_main'],{'movie_id':movie_id})[0]['flag_main'] == 1)&(sql.GetRecords('Completes',['movie_id','flag_subject'],{'movie_id':movie_id})[0]['flag_subject'] != 1):
        if len(sql.GetRecords('FaceSubjects',['id'],{'face_id':sql.GetFacesLastRecord(['id'],{'movie_id':movie_id})['id']})) == 0:
            frame_last = sql.GetFacesLastFrame(movie_record['id'])
            fsrs = sorted(sql.GetRecords('FaceSubjects',['id','face_id'],{}),key=itemgetter('face_id'))
            if len(fsrs) > 0:
                last_face_id = fsrs[-1]['face_id']
                frame_start = sql.GetRecords('Faces',['frame'],{'id':last_face_id})[0]['frame']
            else:
                frame_start = 1
            # for frame in tqdm(np.arange(1,frame_last+1,10)):78101
            print('frame_start:',frame_start,'frame_last:',frame_last)
            progressbar = tqdm(np.arange(frame_start,frame_last+1,10),ncols= 0)
            progressbar.set_description(f"movie_id:{movie_id}")
            frs_all = sql.GetRecords('Faces',['id','movie_id','frame','embedding'],{'movie_id':movie_id})
            for frame in progressbar:
                face_records = [r for r in frs_all if r['frame'] == frame]#sql.GetRecords('Faces',['id','movie_id','frame','embedding'],{'movie_id':movie_id,'frame':frame})

                face_records_prev = [r for r in frs_all if r['frame'] == frame-10] # sql.GetRecords('Faces',['id','movie_id','frame','embedding'],{'movie_id':movie_id,'frame':frame-10})
                # print(face_records[0].keys())
                progressbar.set_description(f"movie_id:{movie_id} {len(face_records)}")

                if (len(face_records_prev)>0) & (len(face_records)>0):
                    # 類似度の高い組み合わせを生成
                    face_combination = my_func.RecordsCombination(face_records_prev,face_records)

                    for i in range(len(face_combination[0])):
                        if face_combination[1][i] == 0:
                            face_id = face_records[i]['id']
                            sql.InsertSubjects(movie_id,face_id)

                            # FaceSubjectsの追加
                            subject_id = sql.GetRecords('Subjects',['id'],{'movie_id':movie_id,'face_id':face_id})[0]['id']
                            sql.InsertRecords('FaceSubjects',{'face_id':face_id,'subject_id':subject_id})
                        else:
                            face_subject_records = sql.GetRecords('FaceSubjects',['subject_id'],{'face_id':face_combination[1][i]})
                            sql.UpdateFaceSubjects(face_combination[0][i],face_subject_records[0]['subject_id'])
                elif (len(face_records_prev)==0) & (len(face_records)>0):
                    for face_record in face_records:
                        face_id = face_record['id'] # 最初のframeのid
                        # Subjectsの重複追加を防ぐ
                        if len(sql.GetRecords('Subjects',['id'],{'movie_id':movie_id,'face_id':face_id}))==0:
                            sql.InsertSubjects(movie_id,face_id)

                            # FaceSubjectsの追加
                            subject_id = sql.GetRecords('Subjects',['id'],{'movie_id':movie_id,'face_id':face_id})[0]['id']
                            sql.InsertFaceSubjects(face_id,subject_id)
            sql.UpdateRecords('Completes',{'movie_id':movie_id},{'movie_id':movie_id,'flag_subject':True})
