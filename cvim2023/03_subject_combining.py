from common_module import my_func
import numpy as np
import yaml, random, time
from tqdm import tqdm

from face_recognition_module import sql_func

from tabulate import tabulate

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

sampling_record_count = 50
subject_similarity_threshould = 0.85
frame_difference_threshould = 30*60*3 # frame * seconds * minutes
loop_break_count_threshould = 50


for mcr in movie_complete_records:
    if (mcr['flag_subject']==1)&(mcr['flag_bond']==9):
        try:
            print(mcr)
            # movie_idを取得
            movie_id = mcr['id']

            # FaceDBを参照するsql
            face_db_sql = sql_func.FaceDB(f"{config['face_db_path']}/FaceDB{movie_id}.db")

            fsfrs = face_db_sql.cursor.execute(f'SELECT fs.face_id, fs.subject_id, f.frame, f.embedding FROM FaceSubjects fs INNER JOIN Faces f ON fs.face_id = f.id').fetchall()
            fsfrs = [{'face_id':r[0], 'subject_id':r[1], 'frame':r[2], 'embedding':r[3]} for r in fsfrs]

            
            match_subject_ids = []
            subjects_groups = []

            # Subjectsレコードを取得
            subject_ids = list(set([r['subject_id'] for r in fsfrs]))

            time_start = time.time()

            for idx, subject_id_target in enumerate(tqdm(subject_ids[0:])):
                if (time.time() - time_start)> 60*60*10: # seconds * minute * hour
                    raise TimeoutError
                # Subjectsの特徴量をすべて取得
                embedding_targets = [r['embedding'] for r in fsfrs if r['subject_id'] == subject_id_target]

                # frameをすべて取得
                target_frames = [r['frame'] for r in fsfrs if r['subject_id'] == subject_id_target]
                
                # 先頭フレーム番号を取得
                frame_head_target = target_frames[0]
                
                # 末尾フレーム番号を取得
                frame_tail_target = target_frames[-1]

                # ループ回数カウント
                loop_counter = 0
                # targets以降のSubjectsと特徴量をループで比較
                for subject_id_match in subject_ids[idx+1:]:
                    # すでにマッチングしているsubject_idをパス
                    if subject_id_match in match_subject_ids:
                        continue

                    # 比較Subjectsのframe取得
                    match_frames = [r['frame'] for r in fsfrs if r['subject_id'] == subject_id_match]
                    
                    # 先頭フレーム番号を取得
                    frame_head_match = match_frames[0]

                    # 末尾フレーム番号を取得
                    frame_tail_match = match_frames[-1]

                    if frame_head_match < frame_tail_target:
                        continue

                    # マッチするものが存在しない
                    loop_counter += 1
                    if (loop_counter == loop_break_count_threshould)or(frame_head_match - frame_tail_target > frame_difference_threshould):
                        subjects_groups.append({'subject_id_0':subject_id_target,'similarity':0., 'frame_difference':0})
                        break

                    # 比較するSubjectsの特徴量一覧を取得
                    embedding_matchs = [r['embedding'] for r in fsfrs if r['subject_id'] == subject_id_match]

                    # レコードがsampling_record_count以上の場合、ランダムで抽出
                    embedding_matchs = embedding_matchs if len(embedding_matchs) <= sampling_record_count else random.sample(embedding_matchs, sampling_record_count)

                    # 特徴量を比較した類似度一覧を取得
                    embedding_sims = np.array([[my_func.ComputeSim(et, em) for et in embedding_targets] for em in embedding_matchs])

                    # 類似度を取得
                    similarity = np.max(embedding_sims)

                    # 類似度がsubject_similarity_threshouldを超える場合一致
                    if similarity > subject_similarity_threshould:
                        # マッチしたsubjectsのグループ化
                        subjects_groups.append({'subject_id_0':subject_id_target,'subject_id_1':subject_id_match,'similarity':similarity, 'frame_difference':frame_head_match - frame_tail_target})

                        # マッチ済みidを保存
                        match_subject_ids.append(subject_id_match)
                        break

            # FaceDBに反映
            for sg in subjects_groups:
                face_db_sql.UpdateRecords('Bonds', {'subject_id_0':sg['subject_id_0']}, sg)
            
            # FaceDBのCompletesを更新
            face_db_sql.UpdateRecords('Completes',{'movie_id':movie_id}, {'flag_bond':1})
            face_db_cr = face_db_sql.GetRecords('Completes',['*'],{'movie_id':movie_id})[0]

            # MovieManageDBのCompletesを更新
            movie_manage_sql.UpdateRecords('Completes',{'movie_id':movie_id},face_db_cr)
        except Exception as e:
            print('Error:',e)

            # FaceDBのCompletesを更新
            face_db_sql.UpdateRecords('Completes',{'movie_id':movie_id}, {'flag_bond':4})
            face_db_cr = face_db_sql.GetRecords('Completes',['*'],{'movie_id':movie_id})[0]

            # MovieManageDBのCompletesを更新
            movie_manage_sql.UpdateRecords('Completes',{'movie_id':movie_id},face_db_cr)