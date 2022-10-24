import numpy as np
import cv2
import os, glob
from tqdm import tqdm
import time

import movie_func, sql_func, my_func

from operator import itemgetter
from tabulate import tabulate

path = 'db/FaceDB.db'
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
            fs_record = face_subject_record = sorted(sql.GetFaceSubjects({'subject_id':subject_id}), key=itemgetter('face_id'))
            if len(fs_record) != 0:
                face_id_first = fs_record[0]['face_id']
                face_id_last = fs_record[-1]['face_id']
                face_subjects.append({'subject_id':subject_id,
                                      'face_id_first':face_id_first,
                                      'frame_first':sql.GetFaces(['id','frame'],{'id':face_id_first})[0]['frame'],
                                      'face_id_last':face_id_last,
                                      'frame_last':sql.GetFaces(['id','frame'],{'id':face_id_last})[-1]['frame']})


        groups = []
        frame_scope = 3
        sim_thre = 0.50
        for i,face_subject_0 in enumerate(face_subjects):
            subject_id_0 = face_subject_0['subject_id']
            facesubjects_records_0 = sql.GetFaceSubjects({'subject_id':subject_id_0})
            if len (facesubjects_records_0) > frame_scope:
                face_record_0_last = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_0[-1*frame_scope]['face_id']})[0]
            else:
                face_record_0_last = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_0[-1]['face_id']})[0]

            for l,face_subject_1 in enumerate(face_subjects[i+1:]):
                subject_id_1 = face_subject_1['subject_id']
                facesubjects_records_1 = sql.GetFaceSubjects({'subject_id':subject_id_1})
                if len (facesubjects_records_1) > frame_scope:
                    face_record_1_first = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_1[frame_scope]['face_id']})[0]
                else:
                    face_record_1_first = sql.GetFaces(['id','frame','embedding'],{'id':facesubjects_records_1[0]['face_id']})[0]
                sim = my_func.ComputeSim(face_record_0_last['embedding'],face_record_1_first['embedding'])
                if face_record_0_last['frame'] < face_record_1_first['frame']:
                    if sim > sim_thre:
                        groups.append([subject_id_0,subject_id_1,sim])
                        sql.UpdateBonds(subject_id_0,subject_id_1,sim)
                        break
        table = tabulate(tabular_data=groups, headers=['subject_id_0','subject_id_1','sim'])
        # print(table)
        sql.UpdateCompletes({'movie_id':movie_id,'flag_bond':True})
