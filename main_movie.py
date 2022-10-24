import numpy as np
import cv2
import os, glob
from tqdm import tqdm
import time

import movie_func, sql_func, my_func

path = 'db/FaceDB.db'
sql = sql_func.FaceDB(path)

movie_records = sql.GetMoviesProperty()
movie_id = 1
movie_record = movie_records[movie_id]
print(movie_record)
movie = movie_func.Movie(movie_record['path'])

ret, img = movie.GetImage(1)

frame_rate = 10.0 # フレームレート
size = (img.shape[1], img.shape[0]) # 動画の画面サイズ

# fmt = cv2.VideoWriter_fourcc('m', 'p', '4', 'v') # ファイル形式(ここではmp4)
# writer = cv2.VideoWriter('outtest.mp4', fmt, frame_rate, size) # ライター作成

start = frame_count = 4501
for frame in tqdm(np.arange(start,movie_record['frame'],10)):
    ret, img = movie.GetImage(frame)

    if frame_count == frame:
        records = sql.GetFaceRecords({'movie_id':movie_id+1,'frame':frame_count})
        # for record in records:
            # print(record['id'],record['movie_id'],record['frame'])
        frame_count += 10

    # 画像描画
    cv2.putText(img, f"{str(frame)}", (50,50), cv2.FONT_HERSHEY_PLAIN, 2, (0,0,255),2,cv2.LINE_AA)
    for record in records:
        #print(record['id'],record['movie_id'],record['frame'],record['bbox'])
        id = record['id']

        [left,top,right,bottom] = [int(v) for v in record['bbox']]

        cv2.putText(img, f"{sql.GetSubjectIdentify(record['id'])} {str(id)}", (left,top), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA)
        # print(abs(right-left),abs(bottom-top))
        wi = int(abs(right-left)*0.8)
        he = int(abs(bottom-top)/2)
        cv2.rectangle(img, (left-wi,top-he),(right+wi,bottom+he),(0,0,255),2)
        cv2.rectangle(img, (left,top),(right,bottom),(0,0,255),2)
    cv2.imshow("img", img)
    cv2.waitKey(0)

#     writer.write(img) # 画像を1フレーム分として書き込み
# writer.release() # ファイルを閉じる
