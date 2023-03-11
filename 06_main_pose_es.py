from face_recognition_module import sql_func as face_sql_func
from skelton_estimation_module import sql_func as skelton_sql_func
from skelton_estimation_module import pose_estimation
from common_module import movie_func, my_func

import cv2
import numpy as np
import more_itertools as mit
import matplotlib.pyplot as plt
import os, sys, glob, time, datetime

from tqdm import tqdm

print(f"Start Movies Loading...")
split_movie_manage_sql_path = 'db/InspectionSplitMoviesManage.db'
manage_sql = skelton_sql_func.MoviesDB(split_movie_manage_sql_path)
_mv_counts = 0
for path in tqdm(glob.glob("db/inspection_split_movie/*.mp4"),leave=False):
    # path = path.replace("\\",'/')
    # print(path, os.path.exists(path))
    _mv_counts += 1
    name = os.path.basename(path).replace('.mp4','')
    movie = movie_func.Movie(path)
    manage_sql.UpdateRecords('Movies',{'name':name},{'name':name,'path':path, 'fps':movie.fps,'frame':movie.frame_count})
    movie_id = manage_sql.GetRecords('Movies',['id'],{'name':name})[0]['id']
    manage_sql.UpdateRecords('Completes',{'movie_id':movie_id},{'movie_id':movie_id})

print(f"Movies Loading Complete Counts:{_mv_counts}")

print(f"Start PoseEstimation...")

landmark_db_dir_path = 'db/inspection_landmark_db' 
if not os.path.exists(landmark_db_dir_path):
    os.mkdir(landmark_db_dir_path)

crs = manage_sql.GetRecords('Completes')
mrs = manage_sql.GetRecords('Movies')
key = ['pose','face','left_hand','right_hand']


pose = pose_estimation.PoseEstimation()
pbar_crs = tqdm(crs[0:])
for cr in pbar_crs:
    movie_id = cr['movie_id']
    landmark_db_path = os.path.join(landmark_db_dir_path,f'LandmarkDB{movie_id}.db')
    landmark_sql = skelton_sql_func.LandmarkDB(landmark_db_path)

    cr = [r for r in crs if r['movie_id'] == movie_id][0] # movie_idと一致するCompletesレコードを取得
    landmark_sql.UpdateRecords('Completes',{'movie_id':movie_id},cr) # LandmarkDBのMoviesテーブルとSplitMoviesManagementのMoviesテーブルを同期
    
    if not cr['flag_pose_esitmation'] in [1,9]:   # 1:success、9:pending  
        for i,_ in enumerate(mrs):
            if mrs[i]['id'] == movie_id:
                mr = mrs[i]
                break  
        movie_path = mr['path']
        landmark_sql.UpdateRecords('Movies',{'name':mr['name']},mr) # LandmarkDBのMoviesテーブルとSplitMoviesManagementのMoviesテーブルを同期
        
        movie = movie_func.Movie(movie_path)
        
        last_record = landmark_sql.cursor.execute("SELECT frame FROM Landmarks ORDER BY frame DESC LIMIT 1").fetchone()
        if last_record:
            start_frame = last_record[0]+1
        else:
            start_frame = 1
        pbar_frame = tqdm(np.arange(start_frame,mr['frame']+2))#,leave=False)
        _count = 0
        insert_data_list = []
        for frame in pbar_frame:
            # print('frame',frame,frame/movie.fps,movie.fps,movie_id)
            time_str = my_func.seconds2time(frame/movie.fps)
            pbar_frame.set_description(f"{mr['name']} {time_str}")
            ret, img = movie.GetImage(frame)
            if ret:
                pose.set_image(img)
                landmarks = {k:pose.get_landmarks(k) for k in key}
                if sum([1 for v in landmarks.values() if v == [[]]]) == 4:
                    continue
                else:
                    _count += 1
                    insert_data = landmarks.copy()
                    insert_data['movie_id'] = movie_id
                    insert_data['frame'] = frame.item()
                    insert_data['time'] = time_str
                    insert_data['width'] = pose.image_width
                    insert_data['height'] = pose.image_height
                    # for k,v in zip(insert_data.keys(),insert_data.values()):
                    #     print(k,v,type(v))
                    # landmark_sql.UpdateRecords('Landmarks',{'frame':frame},insert_data)
                    insert_data_list.append(insert_data)
                    if _count % 20000 == 0:
                        landmark_sql.BulkInsertRecords('Landmarks',insert_data_list)
                        insert_data_list = []
                        _count=0                
                        
        landmark_sql.BulkInsertRecords('Landmarks',insert_data_list)
        manage_sql.UpdateRecords('Completes',{'movie_id':movie_id},{'flag_pose_esitmation':True})
        landmark_sql.UpdateRecords('Completes',{'movie_id':movie_id},cr) # LandmarkDBのMoviesテーブルとSplitMoviesManagementのMoviesテーブルを同期
print(f"Finish PoseEstimation")  

            