import numpy as np
import cv2
import os, glob, sys
from tqdm import tqdm
import time

import movie_func, sql_func, my_func

from operator import itemgetter
from tabulate import tabulate

args = sys.argv

if len(args) < 2:
    # return 0
    sys.exit()

path = args[1]# 'db/split_db/FaceDB1.db'
sql = sql_func.FaceDB(path)

for movies in sql.GetMoviesProperty():
    movie_id = movies['id']
    print(movies)
    subject_records = sql.GetSubjects({'movie_id':movie_id})
    subject_records

    face_subjects = []
    if sql.GetCompletes(['movie_id','flag_bond'],{'movie_id':movie_id})[0]['flag_bond'] != 1:
        for subject_record in subject_records:
            subject_id = subject_record['id']
            fs_record = face_subject_record = sorted(sql.GetFaceSubjects(['id','face_id','subject_id'],{'subject_id':subject_id}), key=itemgetter('face_id'))
            if len(fs_record) != 0:
                face_id_first = fs_record[0]['face_id']
                face_id_last = fs_record[-1]['face_id']
                face_subjects.append({'subject_id':subject_id,
                                      'face_id_first':face_id_first,
                                      'frame_first':sql.GetFaces(['id','frame'],{'id':face_id_first})[0]['frame'],
                                      'face_id_last':face_id_last,
                                      'frame_last':sql.GetFaces(['id','frame'],{'id':face_id_last})[-1]['frame']})


        groups = []
        frame_scope = 10 # 3
        sim_thre = 0.50
        for i,face_subject_0 in tqdm(enumerate(face_subjects),ncols=0):
            subject_id_0 = face_subject_0['subject_id']
            facesubjects_records_0 = sql.GetFaceSubjects(['id','face_id','subject_id'],{'subject_id':subject_id_0})
            if len (facesubjects_records_0) > frame_scope:
                # face_record_0_last = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_0[-1*frame_scope]['face_id']})[0]
                face_record_0s = [sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_0[-1*c_i]['face_id']})[0] for c_i in range(frame_scope)]
            else:
                # face_record_0_last = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_0[-1]['face_id']})[0]
                face_record_0s = [sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_0[-1*c_i]['face_id']})[0] for c_i in range(len(facesubjects_records_0))]

            for l,face_subject_1 in enumerate(face_subjects[i+1:]):
                subject_id_1 = face_subject_1['subject_id']
                facesubjects_records_1 = sql.GetFaceSubjects(['id','face_id','subject_id'],{'subject_id':subject_id_1})
                if len (facesubjects_records_1) > frame_scope:
                    # face_record_1_first = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_1[frame_scope]['face_id']})[0]
                    face_record_1s = [sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_1[c_i]['face_id']})[0] for c_i in range(frame_scope)]
                else:
                    # face_record_1_first = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_1[0]['face_id']})[0]
                    face_record_1s = [sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_1[c_i]['face_id']})[0] for c_i in range(len(facesubjects_records_1))]

                # sim = my_func.ComputeSim(face_record_0_last['embedding'],face_record_1_first['embedding'])
                sims = []
                for fr0 in face_record_0s:
                    for fr1 in face_record_1s:
                        sims.append(my_func.ComputeSim(fr0['embedding'],fr1['embedding']))

                sim = max(sims)
                # if face_record_0_last['frame'] < face_record_1_first['frame']:
                if face_record_0s[-1]['frame'] < face_record_1s[0]['frame']:
                    if sim > sim_thre:
                        groups.append([subject_id_0,subject_id_1,sim])
                        sql.UpdateBonds({'subject_id_0':subject_id_0,'subject_id_1':subject_id_1,'similarity':sim,'frame_difference':(face_record_1s[0]['frame']-face_record_0s[-1]['frame'])})
                        break


        # subject_id_1で重複の発生するレコードを削除
        records = sql.GetBonds(['id','subject_id_0','subject_id_1','similarity','frame_difference'],{})
        dup_subject_id_1 = []
        for subject_id_1 in set([r['subject_id_1'] for r in records]):
            counts = len([r for r in records if r['subject_id_1'] == subject_id_1]) # subject_id_1の重複数
            if counts > 1: # 重複している場合
                dup_subject_id_1.append(subject_id_1)
                dup_records = {record['id']:record['similarity'] for record in [r for r in records if r['subject_id_1'] == subject_id_1]}
                for (idx, sim) in sorted(dup_records.items(), key=lambda x:x[1])[0:-1]:
                    sql.DeleteRecords('Bonds',{'id':idx})

        table = tabulate(tabular_data=groups, headers=['subject_id_0','subject_id_1','sim'])
        # print(table)
        sql.UpdateCompletes({'movie_id':movie_id,'flag_bond':True})
