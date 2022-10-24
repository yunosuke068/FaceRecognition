# FacesテーブルとSubjectsテーブルの中間テーブルの生成

import numpy as np
import cv2
import os, glob
from tqdm import tqdm
import time

import movie_func, sql_func, my_func

path = 'db/FaceDB.db'
sql = sql_func.FaceDB(path)

movie_records = sql.GetMoviesProperty() # Movieテーブルのレコード一覧
for movie_record in movie_records:
    # movie_record = movie_records[0]
    movie_id = movie_record['id']

    print(movie_record)
    if sql.GetCompletes(['movie_id','flag_subject'],{'movie_id':movie_id})[0]['flag_subject'] != 1:
        if len(sql.GetFaceSubjects({'face_id':sql.GetFacesLastRecord(movie_id)[0]})) == 0:
            frame_last = sql.GetFacesLastFrame(movie_record['id'])
            print(frame_last)
            # for frame in tqdm(np.arange(1,frame_last+1,10)):78101
            for frame in tqdm(np.arange(1,frame_last+1,10)):
                # print(frame)
            # for frame in np.arange(1,20+1,10):
                # print(frame)
                face_records = sql.GetFaceRecords({'movie_id':movie_id,'frame':frame})

                # 最初のframeでのSubjectsの追加
                if frame == 1:
                    for face_record in face_records:
                        face_id = face_record['id'] # 最初のframeのid
                        # Subjectsの重複追加を防ぐ
                        if len(sql.GetSubjects({'movie_id':movie_id,'face_id':face_id}))==0:
                            sql.InsertSubjects(movie_id,face_id)

                            # FaceSubjectsの追加
                            subject_id = sql.GetSubjects({'movie_id':movie_id,'face_id':face_id})[0]['id']
                            sql.InsertFaceSubjects(face_id,subject_id)

                # 最初のフレーム以降
                else:
                    # 前フレームのFacesと特徴量の比較
                    face_records_prev = sql.GetFaceRecords({'movie_id':movie_id,'frame':frame-10})
                    # print(face_records[0].keys())

                    # 類似度の高い組み合わせを生成
                    face_combination = my_func.RecordsCombination(face_records_prev,face_records)
                    # print(face_combination)

                    for i in range(len(face_combination[0])):
                        if face_combination[1][i] == 0:
                            face_id = face_records[i]['id']
                            sql.InsertSubjects(movie_id,face_id)

                            # FaceSubjectsの追加
                            subject_id = sql.GetSubjects({'movie_id':movie_id,'face_id':face_id})[0]['id']
                            sql.InsertFaceSubjects(face_id,subject_id)
                        else:
                            face_subject_records = sql.GetFaceSubjects({'face_id':face_combination[1][i]})
                            sql.UpdateFaceSubjects(face_combination[0][i],face_subject_records[0]['subject_id'])
            sql.UpdateCompletes({'movie_id':movie_id,'flag_subject':True})
