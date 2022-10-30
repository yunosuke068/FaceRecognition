import insightface

import numpy as np
import cv2
import os, glob
from tqdm import tqdm

import movie_func, sql_func

path = 'db/FaceDB.db'
sql = sql_func.FaceDB(path)

for path in glob.glob('db/movie/*.mp4'):
    name = os.path.basename(path).replace('.mp4','')
    movie = movie_func.Movie(path)
    frame = movie.frame_count
    fps = movie.fps
    sql.UpdateMovies(name,fps,frame,path)
    movie_id = sql.GetMovies(['id'],{'name':f"'{name}'"})[0]['id']
    sql.UpdateCompletes({'movie_id':movie_id})

face_analytics = insightface.app.FaceAnalysis()
face_analytics.prepare(ctx_id=0, det_size=(640,640))

print("".join(["=" for _ in range(50)]))

rate = 10
for movie_record in sql.GetMoviesProperty():
    movie = movie_func.Movie(movie_record['path'])
    movie_id = movie_record['id']
    print('movie_record',movie_record)
    # if sql.GetCompletes(movie_id) == 0:
    if sql.GetCompletes(['movie_id','flag_main'],{'movie_id':movie_id})[0]['flag_main'] != 1:
        frame_count = movie.frame_count/rate
        frame_last = sql.GetFacesLastFrame(movie_id)
        print('lastid',frame_last)
        sql.DeleteFacesByFrame(movie_id,frame_last)
        progressbar = tqdm(np.arange(frame_last,movie_record['frame']+1,rate),ncols= 0)
        progressbar.set_description(f"movie_id:{movie_id}")
        for frame in progressbar:
            frame_count -= 1
            ret, img = movie.GetImage(frame)
            if ret:
                faces = face_analytics.get(np.asarray(img))

                for face in faces:
                    sql.InsertFaces(int(movie_id),int(frame),face)
        # sql.InsertCompletes(movie_id,True)
        sql.UpdateCompletes({'movie_id':movie_id,'flag_main':True})

# rimg = face_analytics.draw_on(img, faces)
# cv2.imwrite("./output.jpg", rimg)
