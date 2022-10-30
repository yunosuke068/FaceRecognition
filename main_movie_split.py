import numpy as np
import cv2
import os, glob,sys
from tqdm import tqdm
import time

import movie_func, sql_func, my_func

args = sys.argv

if len(args) < 2:
    # return 0
    sys.exit()

path = args[1]# 'db/split_db/FaceDB1.db'
sql = sql_func.FaceDB(path)

frame_rate = 30.0 # フレームレート
size = (500,500) # 動画の画面サイズ
fmt = cv2.VideoWriter_fourcc('m', 'p', '4', 'v') # ファイル形式(ここではmp4)

movie_records = sql.GetMoviesProperty()
print(movie_records)



for movie_record in movie_records:
    movie_id = movie_record['id']
    movie_name = movie_record['name']
    movie = movie_func.Movie(movie_record['path'])
    # sql.GetSubjects({'movie_id':movie_id})
    if (sql.GetCompletes(['movie_id','flag_split'],{'movie_id':movie_id})[0]['flag_split'] != 1)&(sql.GetCompletes(['movie_id','flag_bond'],{'movie_id':movie_id})[0]['flag_bond'] == 1):
        subject_movies = {}
        subject_ids = {}
        for r in sql.GetFaces(['id','bbox'],{'movie_id':movie_id,'frame':1}):
            subject_id = sql.GetFaceSubjects({'face_id':r['id']})[0]['subject_id']
            ret, img = movie.GetImage(1)
            bbox = my_func.CalcBox(img,r['bbox'])
            subject_ids[subject_id] = {'subject_id':subject_id,'face_id_last':sql.GetFaceSubjectsLastRecord(subject_id)[1],'bbox':bbox}

        start = frame_count = 1


        face_records = sql.GetFaces(['id','frame','bbox'],{'movie_id':movie_id})
        faces_last_frame = sql.GetFacesLastRecord(['id','movie_id','frame'],{'movie_id':movie_id})['frame'] # movie_record['frame']
        progressbar = tqdm(np.arange(start,faces_last_frame),ncols= 0)
        progressbar.set_description(f"movie_id:{movie_id}")
        for frame in progressbar:
            if frame > faces_last_frame:
                break
            ret, img = movie.GetImage(frame)
            if frame_count == frame:
                # records = sql.GetFaces(['id','bbox'],{'movie_id':movie_id,'frame':frame_count})
                records = [r for r in face_records if r['frame'] == frame_count]
                frame_count += 10
                # print()
                # print('frame',frame)
                for record in records:
                    face_id = record['id']
                    face_subject_record = sql.GetFaceSubjects({'face_id':face_id})[0]
                    subject_id = face_subject_record['subject_id']
                    # print("face_id",face_id,'subject_id',subject_id)

                    if subject_id in subject_ids.keys():
                        if subject_ids[subject_id]['face_id_last'] != face_id:
                            bbox = my_func.CalcBox(img,record['bbox'])
                            subject_ids[subject_id]['face_id_last'] = sql.GetFaceSubjectsLastRecord(subject_id)[1]
                            subject_ids[subject_id]['bbox'] = bbox

                        elif subject_ids[subject_id]['face_id_last'] == face_id:
                            bbox = my_func.CalcBox(img,record['bbox'])
                            subject_ids[subject_id]['bbox'] = bbox
                            bonds_record = sql.GetBonds({'subject_id_0':subject_id})
                            if len(bonds_record) > 0:
                                subject_ids[bonds_record[0]['subject_id_1']] = subject_ids[subject_id]
                                del subject_ids[subject_id]
                            else:
                                del subject_ids[subject_id]
                    else:
                        bbox = my_func.CalcBox(img,record['bbox'])
                        subject_ids[subject_id] = {'subject_id':subject_id,'face_id_last':sql.GetFaceSubjectsLastRecord(subject_id)[1],'bbox':bbox}
                # print('\nsubject_ids')
                # for k in subject_ids.keys():
                    # print(k,subject_ids[k])
            for k,subject_ids_row in zip(subject_ids.keys(), subject_ids.values()):
                subject_id_output = subject_ids_row['subject_id']
                if not os.path.exists(f'split_movie/{subject_id_output}.mp4'):
                    subject_movies[subject_id_output] = cv2.VideoWriter(f'split_movie/{movie_name}_{subject_id_output}.mp4', fmt, frame_rate, size) # ライター作成

                [left,top,right,bottom] = subject_ids_row['bbox']
                subject_img = img[top:bottom,left:right]
                if subject_img.shape[0] > subject_img.shape[1]: # 画像の幅の方が長い
                    magnification = size[0]/subject_img.shape[0]  # 倍率の計算
                    subject_img = cv2.resize(subject_img,dsize=None,fx=magnification,fy=magnification)
                    subject_img = cv2.copyMakeBorder(subject_img,0,0,0,size[0]-subject_img.shape[0], cv2.BORDER_CONSTANT, (0,0,0))

                else: # 画像の高さの方が長い
                    magnification = size[1]/subject_img.shape[1]
                    subject_img = cv2.resize(subject_img,dsize=None,fx=magnification,fy=magnification)
                    subject_img = cv2.copyMakeBorder(subject_img,0,size[0]-subject_img.shape[0],0,0, cv2.BORDER_CONSTANT, (0,0,0))

                cv2.putText(subject_img, f"movie:{movie_id} subject:{subject_id_output} subject2:{k} frame:{frame}", (10,450), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA)

                subject_movies[subject_id_output].write(subject_img)

        for writer in subject_movies.values():
            writer.release()

        sql.UpdateCompletes({'movie_id':movie_id,'flag_split':True})
