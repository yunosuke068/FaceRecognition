from common_module import movie_func, my_func
import insightface

import numpy as np
import cv2
import os, glob, sys, yaml
from tqdm import tqdm
from tabulate import tabulate

from face_recognition_module import sql_func

from tabulate import tabulate

# 環境の確認
with open('../enviroment.yaml', 'r') as yml:
    enviroment = yaml.safe_load(yml)['enviroment']
print('enviroment:', enviroment)

# 設定ファイルの読み込み
with open('config.yaml', 'r') as yml:
    config = yaml.safe_load(yml)[enviroment]
# print(config)

# ============================
# MovieManageの更新
# ============================
print('Update MovieManage.db')

# MovieMangeを参照するsql
movie_manage_sql = sql_func.FaceDB(config['movie_manage_path'])
for path in glob.glob(f"{config['movie_path']}/*.mp4"):
    # Youtube動画IDを取得
    name = os.path.basename(path).replace('.mp4','')

    if movie_manage_sql.GetRecords('Movies',['id'],{'name':f"{name}"}, option={'sql_str':'LIMIT 1'}) == 0:
        # 動画を参照するmovieクラスを生成
        movie = movie_func.Movie(path)

        # 動画の総フレーム数を取得
        frame = movie.frame_count

        # 動画のfpsを取得
        fps = movie.fps

        # Moviesレコードを追加
        movie_manage_sql.InsertRecords('Movies',{'name':name,'fps':fps,'frame':frame,'path':path})

        # Completesレコードの追加
        movie_id = movie_manage_sql.GetRecords('Movies',['id'],{'name':f"{name}"})[0]['id']
        movie_manage_sql.InsertRecords('Completes',{'movie_id':movie_id, 'flag_main':9, 'flag_subject':9, 'flag_bond':9, 'flag_split':9})

# MovieManageのMoviesとCompletesの状態確認
records = movie_manage_sql.cursor.execute('SELECT m.id, m.name, m.path, m.fps, m.frame, c.flag_main, c.flag_subject, c.flag_bond, c.flag_split, c.created_at, c.updated_at FROM Movies m INNER JOIN Completes c ON m.id = c.movie_id').fetchall()
print(tabulate(records, headers=['id','name','path','fps','frame','Fmain','Fsubject','Fbond','Fsplit','created_at','updated_at']))
del records


# ============================
# FaceDBの更新・顔認識処理
# ============================

# insightFaceのセットアップ
face_analytics = insightface.app.FaceAnalysis()
face_analytics.prepare(ctx_id=0, det_size=(640,640))

# MovieMangeを参照するsql
movie_manage_sql = sql_func.FaceDB(config['movie_manage_path']) 

# MoviesとCompletesをinner joinして取得
sql_str = 'SELECT m.id, m.name, m.path, m.fps, m.frame, c.flag_main, c.flag_subject, c.flag_bond, c.flag_split FROM Movies m INNER JOIN Completes c ON m.id = c.movie_id'
movie_complete_records =  movie_manage_sql.cursor.execute(sql_str).fetchall() 
movie_complete_records = [{'id':r[0], 'name':r[1], 'path':r[2], 'fps':r[3], 'frame':r[4], 'flag_main':r[5], 'flag_subject':r[6], 'flag_bond':r[7], 'flag_split':r[8]} for r in movie_complete_records]

# 顔認識処理を行うframe rate
frame_rate = config['face_recognition_frame_rate']

for mcr in movie_complete_records:

    if len(glob.glob(f"{config['movie_path']}/{mcr['name']}.mp4")) == 0:
        print(f"{mcr['id']} {mcr['name']}.mp4 is not exists")
        continue
    else:
        print(f"{mcr['id']} {mcr['name']}.mp4 is exists")

    # 顔認識処理
    if mcr['flag_main'] == 9: # 顔認識処理が実行されていない
        # FaceDBを参照するsql
        face_db_sql = sql_func.FaceDB(f"{config['face_db_path']}/FaceDB{mcr['id']}.db")

        # FaceDBレコードのCompletesテーブルを更新
        movie_manage_cr = movie_manage_sql.GetRecords('Completes',['*'],{'movie_id':mcr['id']},option={'sql_str':'LIMIT 1'})[0]
        face_db_sql.UpdateRecords('Completes', {'movie_id':mcr['id']}, movie_manage_cr)

        # FaceDBのMoviesレコードを取得
        face_db_mrs = face_db_sql.GetRecords('Movies',['*'])

        # FaceDBにおけるMoviesレコードの存在チェック
        if len(face_db_mrs) > 0: # FaceDBのMoviesにレコードが存在する
            face_db_mr = face_db_mrs[0]

            # movie manageの情報とFaceDbの情報が一致しているかチェック
            if mcr['name'] != face_db_mr['name']:
                print('MovieManageとFaceDBが一致していません. id:',mcr['id'])
                break
        else: # FaceDBのMoviesにレコードが存在しない
            # FaceDBのMoviesにMovieManageのMoviesレコードを追加
            movie_manage_mr = movie_manage_sql.GetRecords('Movies',['*'],{'id':mcr['id']})[0]
            face_db_sql.InsertRecords('Movies',movie_manage_mr)

            # FaceDBのMoviesレコードを取得
            face_db_mr = face_db_sql.GetRecords('Movies',['*'])[0]
        
        # 動画を参照するMovieインスタンス
        movie = movie_func.Movie(f"{config['movie_path']}/{mcr['name']}.mp4")
        # Facesテーブルのframeカラムの最大値を取得（前回最後に読み込んフレーム値を取得）
        last_frame_records = face_db_sql.GetRecords('Faces',['frame'],option={'sql_str':'ORDER BY FRAME DESC LIMIT 1'})

        # Facesテーブルのframeカラムの最大値の存在チェック
        if len(last_frame_records) > 0: # frameカラムの最大値が存在する
            last_frame = last_frame_records[0]['frame'] # 読み込み開始フレーム

            # frameカラムの最大値以上のレコードを削除
            sql_str = f'DELETE FROM Faces WHERE frame >= {last_frame}'
            face_db_sql.cursor.execute(sql_str)
            face_db_sql.connect.commit()
        else: # frameカラムの最大値が存在しない
            last_frame = 1
        
        # movie_idを取得
        movie_id = face_db_mr['id']

        # プログレスバーを定義
        progressbar = tqdm(np.arange(last_frame, face_db_mr['frame']+1,frame_rate),ncols=0)
        progressbar.set_description(f"movie_id:{movie_id}")

        insert_values = []
        for frame in progressbar:
            ret, img = movie.GetImage(frame)
            if ret:
                faces = face_analytics.get(np.asarray(img))
                for face in faces:
                    for k in face.keys():
                        if k in ['bbox','kps','landmark_3d_68','pose','landmark_2d_106','embedding']:
                            face[k] = face[k].tolist()
                        elif k in ['det_score']:
                            face[k] = float(face[k])
                        elif k in ['gender','age']:
                            face[k] = int(face[k])
                    face['movie_id'] = int(movie_id)
                    face['frame'] = int(frame)
                insert_values += faces
            if len(insert_values) > 5000:
                face_db_sql.BulkInsertRecords('Faces',insert_values)
                insert_values = []
        face_db_sql.BulkInsertRecords('Faces',insert_values)

        # FaceDBのCompletesを更新
        face_db_sql.UpdateRecords('Completes',{'movie_id':movie_id}, {'flag_main':1})
        face_db_cr = face_db_sql.GetRecords('Completes',['*'],{'movie_id':movie_id})[0]
        
        # MovieManageDBのCompletesを更新
        movie_manage_sql.UpdateRecords('Completes',{'movie_id':movie_id},face_db_cr)