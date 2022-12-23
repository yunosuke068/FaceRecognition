from common_module import movie_func, my_func
import numpy as np
import cv2
import os, glob,sys
from tqdm import tqdm
import time
from operator import itemgetter

from face_recognition_module import sql_func

args = sys.argv

if len(args) < 1:
    # return 0
    sys.exit()

path = args[1]# 'db/split_db/FaceDB1.db'
sql = sql_func.FaceDB(path)


# 画像生成
storage_dir = 'temporarily_storage/split_images'

if not os.path.exists(storage_dir):
    os.mkdir(storage_dir)
else:
    for p in glob.glob(storage_dir+'/*'):
        os.remove(p)

mr = sql.GetRecords('Movies',['id','name','path'],{})[0]
movie = movie_func.Movie(mr['path'])

srs = sql.GetRecords('Subjects',['id','face_id'],{})

scope = 3

for sr in srs:
    sr_id = sr['id']
    frs = sql.GetRecords('Faces',['id','frame','bbox'],{'id':sr['face_id']})
    for record in frs:
        frames = [record['frame']]
        for i in range(1, scope+1):
            frames.insert(0,record['frame']-10)
            frames.append(record['frame']+10)
        subject_imgs = []
        for frame in frames:
            ret, img = movie.GetImage(frame)
            if ret:
                pos = [int(v) for v in record['bbox']]
                for i,v in enumerate(pos):
                    if v < 0:
                        pos[i] = 0
                [left,top,right,bottom] = pos
                time_str = my_func.seconds2time(frame/movie.fps)
                img = cv2.resize(img[top:bottom,left:right],dsize=None,fx=1.5,fy=1.5)
                img = cv2.putText(img, f"{sr_id} {frame}", (10,10), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA)
                img = cv2.putText(img, f"{time_str}", (10,30), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA)
                subject_imgs.append(img)
                # print(subject_img.shape)

        subject_img = subject_imgs[0]
        for si in subject_imgs[1:]:
            subject_img = cv2.hconcat([subject_img,si])

        cv2.imwrite(f'{storage_dir}/{sr_id}.jpg', subject_img)

# indexの結合表示
records = sorted(sql.GetRecords("Bonds",col=['subject_id_0','subject_id_1']),key=itemgetter('subject_id_0'))
print(records)
subject = {}
for r in tqdm(records,ncols=0):
    if r['subject_id_0'] in subject.keys():
        subject[r['subject_id_1']] = subject[r['subject_id_0']]
        subject[r['subject_id_1']].append(r['subject_id_1'])
        del subject[r['subject_id_0']]
    elif not r['subject_id_0'] in subject.keys():
        subject[r['subject_id_1']] = [r['subject_id_0'],r['subject_id_1']]

subjects = []
for k in subject.keys():
    print(subject[k][0],subject[k])
    subjects.extend(subject[k])
print(len(subject))

print('exists ',subjects)
print('not exists')
for r in sql.GetRecords('Subjects',['id']):
    if not r['id'] in subjects:
        print(r['id'])