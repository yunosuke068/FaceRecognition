# FacesテーブルとSubjectsテーブルの中間テーブルの生成

from common_module import movie_func, my_func
import numpy as np
import cv2
import os, glob, sys, yaml
from tqdm import tqdm
import time
from tabulate import tabulate

from face_recognition_module import sql_func
from operator import itemgetter

# ========================
# 設定ファイルの読み込み
# ========================

# 環境の確認
with open('../enviroment.yaml', 'r') as yml:
    enviroment = yaml.safe_load(yml)['enviroment']
print('enviroment:', enviroment)

# 設定ファイルの読み込み
with open('config.yaml', 'r') as yml:
    config = yaml.safe_load(yml)[enviroment]
# print(config)

# =========================================
# MovieManageDBのMoviesとCompletesの状態確認
# =========================================

# MovieMangeを参照するsql
movie_manage_sql = sql_func.FaceDB(config['movie_manage_path'])

# MovieManageのMoviesとCompletesの状態確認
records = movie_manage_sql.cursor.execute('SELECT m.id, m.name, m.path, m.fps, m.frame, c.flag_main, c.flag_subject, c.flag_bond, c.flag_split, c.created_at, c.updated_at FROM Movies m INNER JOIN Completes c ON m.id = c.movie_id').fetchall()
print(tabulate(records, headers=['id','name','path','fps','frame','Fmain','Fsubject','Fbond','Fsplit','created_at','updated_at']))
del records


# =========================================================
# 人物のグループ化処理（連続して顔認識が行われている人物の結合）
# =========================================================

# MovieMangeを参照するsql
movie_manage_sql = sql_func.FaceDB(config['movie_manage_path']) 

# MoviesとCompletesをinner joinして取得
sql_str = 'SELECT m.id, m.name, m.path, m.fps, m.frame, c.flag_main, c.flag_subject, c.flag_bond, c.flag_split FROM Movies m INNER JOIN Completes c ON m.id = c.movie_id'
movie_complete_records =  movie_manage_sql.cursor.execute(sql_str).fetchall() 
movie_complete_records = [{'id':r[0], 'name':r[1], 'path':r[2], 'fps':r[3], 'frame':r[4], 'flag_main':r[5], 'flag_subject':r[6], 'flag_bond':r[7], 'flag_split':r[8]} for r in movie_complete_records]


for mcr in movie_complete_records:
    # 人物のグループ化処理
    if (mcr['flag_main']==1)&(mcr['flag_subject']==9):
        # movie_idを取得
        movie_id = mcr['id']

        print(movie_id, mcr['name'], f"flag_main: {mcr['flag_main']}", f"flag_subject: {mcr['flag_subject']}")

        # FaceDBを参照するsql
        face_db_sql = sql_func.FaceDB(f"{config['face_db_path']}/FaceDB{mcr['id']}.db")

        # FaceDBレコードのCompletesテーブルを更新
        movie_manage_cr = movie_manage_sql.GetRecords('Completes',['*'],{'movie_id':mcr['id']},option={'sql_str':'LIMIT 1'})[0]
        face_db_sql.UpdateRecords('Completes', {'movie_id':mcr['id']}, movie_manage_cr)

        # Facesテーブルのframeカラムの最大値を取得（前回最後に読み込んフレーム値を取得）
        last_frame = face_db_sql.GetRecords('Faces',['frame'],option={'sql_str':'ORDER BY FRAME DESC LIMIT 1'})[0]['frame']

        # FaceSubjectsテーブルのface_idカラムの最大値を取得（前回最後に読み込まれたFacesのidを取得）
        last_face_id = face_db_sql.GetRecords('FaceSubjects',['face_id'],option={'sql_str':'ORDER BY face_id LIMIT 1'})
        
        # FaceSubjectsテーブルのface_idカラムの最大値の存在チェック
        if len(last_face_id) > 0: # face_idカラムにレコードが存在する
            last_face_id = last_face_id[-1]['face_id']
            frame_start = face_db_sql.GetRecords('Faces',['frame'],{'id':last_face_id},option={'sql_str':'LIMIT 1'})[0]['frame'] # 読み込み開始フレーム
        else: # face_idカラムにレコードが存在しない
            frame_start = 1 # 読み込み開始フレーム
        print(frame_start)

        # movie_idが一致するFacesレコードを取得
        frs_all = face_db_sql.GetRecords('Faces',['id','movie_id','frame','embedding'],{'movie_id':movie_id})

        # 結合処理
        progressbar = tqdm(np.arange(frame_start,last_frame+1,config['face_recognition_frame_rate']),ncols= 0)
        progressbar.set_description(f"movie_id:{movie_id}")
        for frame in progressbar:
            frs = [fr for fr in frs_all if fr['frame'] == frame]

            frs_prev = [fr for fr in frs_all if fr['frame'] == frame-config['face_recognition_frame_rate']]

            # ひとつ前の顔認識処理実行frameのFaces Recordsの存在チェック
            if (len(frs_prev)>0)&(len(frs)>0): # 存在する
                # 類似度の高い組み合わせの生成
                face_combination = my_func.RecordsCombination(frs_prev, frs)
                
                # Subjectsレコード・FaceSubjectsレコードの追加処理
                for i in range(len(face_combination[0])):
                    if face_combination[1][i] == 0: # 対応する人物idが存在しない
                        face_id = frs[i]['id']
                        
                        # SubjectsレコードをFaceDBに追加
                        face_db_sql.InsertSubjects(movie_id, face_id)

                        # face_idが一致するSubjectsレコードのidを取得
                        subject_id = face_db_sql.GetRecords('Subjects',['id'],{'movie_id':movie_id,'face_id':face_id},option={'sql_str':'LIMIT 1'})[0]['id']

                        # FaceSubjectsレコードをFaceDBに追加
                        face_db_sql.InsertRecords('FaceSubjects',{'face_id':face_id, 'subject_id':subject_id})

                    else: # 対応する人物idが存在する
                        # current frameのface_idを取得
                        cur_face_id = face_combination[0][i]

                        # マッチするひとつ前のface_idを取得
                        pre_face_id = face_combination[1][i]

                        # pre_face_idに一致するsubject_idを取得
                        subject_id = face_db_sql.GetRecords('FaceSubjects',['subject_id'],{'face_id':pre_face_id},option={'sql_str':'LIMIT 1'})[0]['subject_id']

                        # FaceSubjectsのupdate
                        face_db_sql.UpdateRecords('FaceSubjects',{'face_id':cur_face_id},{'face_id':cur_face_id,'subject_id':subject_id})
            
            elif (len(frs_prev)==0)&(len(frs)>0): # 存在しない
                for fr in frs:
                    face_id = fr['id']

                    # Subjectsの重複追加を防ぐ
                    if len(face_db_sql.GetRecords('Subjects',['id'],{'movie_id':movie_id,'face_id':face_id}))==0:
                        # SubjectsレコードをFaceDBに追加
                        face_db_sql.InsertSubjects(movie_id, face_id)

                        # face_idに一致するsubject_idを取得する
                        subject_id = face_db_sql.GetRecords('Subjects',['id'],{'movie_id':movie_id,'face_id':face_id}, option={'sql_str':'LIMIT 1'})[0]['id']

                        # FaceSubjectsレコードをFaceDBに追加
                        face_db_sql.InsertRecords('FaceSubjects',{'face_id':face_id,'subject_id':subject_id})

        # Completesのfalag_subjectを更新
        face_db_sql.UpdateRecords('Completes',{'movie_id':movie_id},{'flag_subject':1})