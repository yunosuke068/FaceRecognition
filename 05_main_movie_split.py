from common_module import movie_func, my_func
import numpy as np
import cv2
import os, glob,sys
from tqdm import tqdm
import time,datetime
from operator import itemgetter

from face_recognition_module import sql_func

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
    for path in glob.glob(f'split_movie/{movie_name}_*.mp4'):
        os.remove(path)
    movie = movie_func.Movie(movie_record['path'])
    # sql.GetSubjects({'movie_id':movie_id})
    if (sql.GetCompletes(['movie_id','flag_split'],{'movie_id':movie_id})[0]['flag_split'] != 1)&(sql.GetCompletes(['movie_id','flag_bond'],{'movie_id':movie_id})[0]['flag_bond'] == 1):
        subject_movies = {}
        subject_ids = {}
        # for r in sql.GetFaces(['id','bbox'],{'movie_id':movie_id,'frame':1}):
        #     subject_id = sql.GetFaceSubjects(['subject_id'],{'face_id':r['id']})[0]['subject_id']
        #     ret, img = movie.GetImage(1)
        #     bbox = my_func.CalcBox(img,r['bbox'])
        #     subject_ids[subject_id] = {'subject_id':subject_id,'face_id_last':sql.GetFaceSubjectsLastRecord(subject_id)[1],'bbox':bbox}

        fsrs = sql.GetRecords('FaceSubjects',['id','face_id','subject_id'],{})
        brs = sorted(sql.GetRecords('Bonds',['id','subject_id_0','subject_id_1'],{}),key=itemgetter('subject_id_0'))
        progressbar = tqdm(sorted(brs,key=itemgetter('subject_id_0'),reverse=True))
        progressbar.set_description(f"subject update")
        for br in progressbar:
            for i,fsr in enumerate(fsrs):
                if fsr['subject_id'] == br['subject_id_1']:
                    fsrs[i]['subject_id'] =  br['subject_id_0']
        print('fsrs')
        s_ids = list(set([fsr['subject_id'] for fsr in fsrs]))
        last_face_ids = {val:[fsr for fsr in fsrs if fsr['subject_id'] == val][-1]['face_id'] for val in list(set([fsr['subject_id'] for fsr in fsrs]))}
        print(s_ids,len(s_ids))

        start = frame_count = 1

        frs = face_records = sql.GetFaces(['id','frame','bbox'],{'movie_id':movie_id})
        progressbar = tqdm(enumerate(frs))
        progressbar.set_description(f"subject concat {len(frs)}")
        for i,fr in progressbar:
            frs[i]['subject_id'] = [r['subject_id'] for r in fsrs if r['face_id'] == fr['id']][0]
            frs[i]['face_id_last'] = last_face_ids[frs[i]['subject_id']]

        faces_last_frame = sql.GetFacesLastRecord(['id','movie_id','frame'],{'movie_id':movie_id})['frame'] # movie_record['frame']
        progressbar = tqdm(np.arange(start,faces_last_frame),ncols= 0)
        progressbar.set_description(f"movie_id:{movie_id}")

        for frame in progressbar:
            s = frame/30
            hours, remainder = divmod(s, 3600)
            minutes, seconds = divmod(remainder, 60)
            progressbar.set_description(f"movie_id:{movie_id} {'{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))} subject:{len(subject_ids.keys())}")
            if frame > faces_last_frame:
                break
            ret, img = movie.GetImage(frame)
            if frame_count == frame:
                records = [fr for fr in frs if fr['frame'] == frame_count]
                frame_count += 10
                for record in records:
                    bbox = my_func.CalcBox(img,record['bbox'])
                    subject_ids[record['subject_id']] = {'subject_id':record['subject_id'],'bbox':bbox,'face_id':record['id'],'face_id_last':record['face_id_last']}
                    # if record['subject_id'] in subject_ids.keys():
                    #     bbox = my_func.CalcBox(img,record['bbox'])
                    #     subject_ids[record['subject_id']]['bbox'] = bbox
                    # else:
                    #     bbox = my_func.CalcBox(img,record['bbox'])
                    #     subject_ids[record['subject_id']] = {'subject_id':record['subject_id'],'bbox':bbox,'face_id':record['face_id'],'face_id_last':record['face_id_last']}

            progressbar.set_description(f"movie_id:{movie_id} {'{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))} subject:{len(subject_ids.keys())}")
            # print('\n',frame,subject_ids)
            for k,subject_ids_row in zip(subject_ids.keys(), subject_ids.values()):
                subject_id_output = subject_ids_row['subject_id']
                if not os.path.exists(f'split_movie/{movie_name}_{subject_id_output}.mp4'):
                    subject_movies[subject_id_output] = cv2.VideoWriter(f'split_movie/{movie_name}_{subject_id_output}.mp4', fmt, frame_rate, size) # ライター作成
                if subject_ids_row['face_id'] < subject_ids_row['face_id_last']:
                    [left,top,right,bottom] = subject_ids_row['bbox']
                    subject_img = img[top:bottom,left:right]
                    if subject_img.shape[0] > subject_img.shape[1]: # 画像の幅の方が長い
                        magnification = size[0]/subject_img.shape[0]  # 倍率の計算
                        subject_img = cv2.resize(subject_img,dsize=None,fx=magnification,fy=magnification)
                        subject_img = cv2.copyMakeBorder(subject_img,0,0,0,size[1]-subject_img.shape[1], cv2.BORDER_CONSTANT, (0,0,0))

                    else: # 画像の高さの方が長い
                        magnification = size[1]/subject_img.shape[1]
                        subject_img = cv2.resize(subject_img,dsize=None,fx=magnification,fy=magnification)
                        subject_img = cv2.copyMakeBorder(subject_img,0,size[0]-subject_img.shape[0],0,0, cv2.BORDER_CONSTANT, (0,0,0))

                    cv2.putText(subject_img, f"movie:{movie_id} subject:{subject_id_output} subject2:{k} frame:{frame} {datetime.timedelta(seconds=frame/30)}", (10,450), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA)
                    # print(subject_img.shape)

                    subject_movies[subject_id_output].write(subject_img)

        for writer in subject_movies.values():
            writer.release()

        # sql.UpdateCompletes({'movie_id':movie_id,'flag_split':True})
        sql.UpdateRecords('Completes',{'movie_id':movie_id}, {'flag_split':True})

        # for frame in progressbar:
        #     if frame > faces_last_frame:
        #         break
        #     ret, img = movie.GetImage(frame)
        #     if frame_count == frame:
        #         # records = sql.GetFaces(['id','bbox'],{'movie_id':movie_id,'frame':frame_count})
        #         records = [r for r in face_records if r['frame'] == frame_count]
        #         frame_count += 10
        #         # print()
        #         # print('frame',frame)
        #         for record in records:
        #             face_id = record['id']
        #             subject_id = sql.GetFaceSubjects(['subject_id'],{'face_id':face_id})[0]['subject_id']
        #             # print("face_id",face_id,'subject_id',subject_id)
        #
        #             if subject_id in subject_ids.keys():
        #                 if subject_ids[subject_id]['face_id_last'] != face_id:
        #                     bbox = my_func.CalcBox(img,record['bbox'])
        #                     subject_ids[subject_id]['face_id_last'] = sql.GetFaceSubjectsLastRecord(subject_id)[1]
        #                     subject_ids[subject_id]['bbox'] = bbox
        #
        #                 elif subject_ids[subject_id]['face_id_last'] == face_id:
        #                     bbox = my_func.CalcBox(img,record['bbox'])
        #                     subject_ids[subject_id]['bbox'] = bbox
        #                     bonds_record = sql.GetRecords('Bonds',['subject_id_0','subject_id_1'],{'subject_id_0':subject_id})
        #                     if len(bonds_record) > 0:
        #                         subject_ids[bonds_record[0]['subject_id_1']] = subject_ids[subject_id]
        #                         del subject_ids[subject_id]
        #                     else:
        #                         del subject_ids[subject_id]
        #             else:
        #                 bbox = my_func.CalcBox(img,record['bbox'])
        #                 subject_ids[subject_id] = {'subject_id':subject_id,'face_id_last':sql.GetFaceSubjectsLastRecord(subject_id)[1],'bbox':bbox}
        #
        #     for k,subject_ids_row in zip(subject_ids.keys(), subject_ids.values()):
        #         subject_id_output = subject_ids_row['subject_id']
        #         if not os.path.exists(f'split_movie/{movie_name}_{subject_id_output}.mp4'):
        #             subject_movies[subject_id_output] = cv2.VideoWriter(f'split_movie/{movie_name}_{subject_id_output}.mp4', fmt, frame_rate, size) # ライター作成
        #
        #         [left,top,right,bottom] = subject_ids_row['bbox']
        #         subject_img = img[top:bottom,left:right]
        #         if subject_img.shape[0] > subject_img.shape[1]: # 画像の幅の方が長い
        #             magnification = size[0]/subject_img.shape[0]  # 倍率の計算
        #             subject_img = cv2.resize(subject_img,dsize=None,fx=magnification,fy=magnification)
        #             subject_img = cv2.copyMakeBorder(subject_img,0,0,0,size[0]-subject_img.shape[0], cv2.BORDER_CONSTANT, (0,0,0))
        #
        #         else: # 画像の高さの方が長い
        #             magnification = size[1]/subject_img.shape[1]
        #             subject_img = cv2.resize(subject_img,dsize=None,fx=magnification,fy=magnification)
        #             subject_img = cv2.copyMakeBorder(subject_img,0,size[0]-subject_img.shape[0],0,0, cv2.BORDER_CONSTANT, (0,0,0))
        #
        #         cv2.putText(subject_img, f"movie:{movie_id} subject:{subject_id_output} subject2:{k} frame:{frame} {datetime.timedelta(seconds=frame/30)}", (10,450), cv2.FONT_HERSHEY_PLAIN, 1, (0,0,255),1,cv2.LINE_AA)
        #
        #         subject_movies[subject_id_output].write(subject_img)
        #
        # for writer in subject_movies.values():
        #     writer.release()
        #
        # sql.UpdateCompletes({'movie_id':movie_id,'flag_split':True})
