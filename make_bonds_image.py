from common_module import movie_func
import numpy as np
import cv2
import os, glob,sys
from tqdm import tqdm
import time

from face_recognition_module import sql_func, my_func

args = sys.argv

if len(args) < 1:
    # return 0
    sys.exit()

path = args[1]# 'db/split_db/FaceDB1.db'
sql = sql_func.FaceDB(path)

if not os.path.exists('images'):
    os.mkdir('images')
else:
    for p in glob.glob('images/*'):
        os.remove(p)

mr = sql.GetRecords('Movies',['id','name','path'],{})[0]
movie = movie_func.Movie(mr['path'])

srs = sql.GetRecords('Subjects',['id','face_id'],{})
for sr in srs:
    sr_id = sr['id']
    frs = sql.GetRecords('Faces',['id','frame','bbox'],{'id':sr['face_id']})
    for record in frs:
        frames = [record['frame']-10,record['frame'],record['frame']+10]
        subject_imgs = []
        for frame in frames:
            ret, img = movie.GetImage(frame)
            if ret:
                pos = [int(v) for v in record['bbox']]
                for i,v in enumerate(pos):
                    if v < 0:
                        pos[i] = 0
                [left,top,right,bottom] = pos
                subject_imgs.append(cv2.putText(img[top:bottom,left:right], f"{sr_id} {frame}", (10,10), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA))
                # print(subject_img.shape)

        subject_img = subject_imgs[0]
        for si in subject_imgs[1:]:
            subject_img = cv2.hconcat([subject_img,si])

        cv2.imwrite(f'images/{sr_id}.jpg', subject_img)
