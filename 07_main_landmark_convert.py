from skelton_estimation_module import sql_func as skelton_sql_func
from common_module import my_func
import itertools
import glob
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

# landmarkの正規化
for path in glob.glob('db/inspection_landmark_db/LandmarkDB*.db'):
    print(path)
    
    sql = skelton_sql_func.LandmarkDB(path)
    if len(sql.GetRecords('Movies')) > 0:
        print(sql.GetRecords('Movies')[0])
    else:
        continue
    nlrs = sql.GetRecords('NormLandmarks',['frame'],option={'sql_str':'ORDER BY frame DESC LIMIT 1'})
    start_frame = nlrs[0]['frame'] if len(nlrs) == 1 else 0
    print('start_frame',start_frame)
    # finish_frame = sql.GetRecords('Landmarks',['frame'],option={'sql_str':'WHERE face != "[]" ORDER BY frame DESC LIMIT 1'})[0]['frame']

    lr_counts = sql.cursor.execute('SELECT COUNT(*) FROM Landmarks WHERE face != "[]"').fetchone()[0]
    nlr_counts = sql.cursor.execute('SELECT COUNT(*) FROM NormLandmarks').fetchone()[0]
    # nlr_counts = 
    print("lr_counts",lr_counts)
    print("nlr_counts",nlr_counts)
    with tqdm(total=lr_counts) as pbar:
        _count = nlr_counts
        pbar.update(_count)
        while(lr_counts > _count):
            lrs = sql.GetRecords('Landmarks',option={'sql_str':f'WHERE frame > {start_frame} AND face != "[]" ORDER BY frame LIMIT 5000'})

            lr = lrs[0]
            insert_datas = []
            for lr in lrs:
                parts_concat = []
                exist_flags = {}
                for k in ['face','right_hand','left_hand']:
                    array_size = len(lr[k])
                    exist_flags[k] = [True, array_size] if array_size != 1 else [False, 0]
                    for row in lr[k]:
                        if len(row) > 3:
                            del row[3]
                    parts_concat += lr[k]
                flat_points = np.array(list(itertools.chain.from_iterable(parts_concat))) # 二次元リスト一次元array化

                # TO:DOリファクタリング
                # 相対座標に変換する過程で、座標が正規化される？ため、別の正規化は必要ないかも。検証が必要
                # flat_points = flat_points/np.array([lr['width'],lr['height',lr['width']]]*flat_points.shape[0]/3) #  スケーリング
                # flat_points = flat_points/np.full((flat_points.shape[0]), 500) # width = height = 500

                flat_points = my_func.ConvertLocalCoordinate(flat_points) # 相対座標系に変換
                
                # print(exist_flags)

                scope = 0
                insert_data = {'movie_id':lr['movie_id'],'frame':lr['frame'],'time':lr['time'],'pose':[]}
                for k in ['face','right_hand','left_hand']:
                    if exist_flags[k][0]:
                        arr = flat_points[scope:scope+exist_flags[k][1]*3]
                        insert_data[k] = arr.tolist()
                        scope += exist_flags[k][1]*3
                    else:
                        insert_data[k] = []

                # print(insert_data)
                insert_datas.append(insert_data)
                pbar.update(1)
                _count += 1
            start_frame = insert_data['frame']

            sql.BulkInsertRecords('NormLandmarks',insert_datas)


# face mesh と hand の距離計算
for path in glob.glob('db/inspection_landmark_db/LandmarkDB*.db')[0:]:
    print(path)
    print('distance')

    sql = skelton_sql_func.LandmarkDB(path)
    if len(sql.GetRecords('Movies')) > 0:
        print(sql.GetRecords('Movies')[0])
    else:
        continue

    # 距離計算が必要なレコードの合計
    total = sql.cursor.execute('SELECT count(frame) FROM NormLandmarks WHERE right_hand != "" OR left_hand != ""').fetchone()[0]

    sql.cursor.execute('DELETE FROM Distances;')
    sql.connect.commit()

    with tqdm(total=total) as pbar:
        # 現在までの処理が完了しているレコードの合計
        total = sql.cursor.execute('SELECT count(frame) FROM Distances').fetchone()[0]
        pbar.update(total) # プログレスバーを更新

        # Distancesテーブルのframeの値が最も高いレコードを1件抽出
        last_record = sql.cursor.execute("SELECT frame FROM Distances ORDER BY frame DESC LIMIT 1").fetchone()
        # ループの開始フレームの決定
        start_frame = last_record[0] if last_record else 0

        insert_vals = []
        _count = 0
        # hand landmarkが存在するレコードを5000件抽出
        nlrs = sql.GetRecords('NormLandmarks',option={'sql_str':f'WHERE frame > {start_frame} AND (right_hand != "" OR left_hand != "") ORDER BY frame LIMIT 5000'})
        while(len(nlrs) > 0): # 距離計算が必要なレコードが無くなったとき終了
            for nlr in nlrs:
                pbar.update(1) # プログレスバーを更新
                # Landmarkを二次元ndarray化
                landmark_2Darrs = {}
                for k in ['face','right_hand','left_hand']:
                    if nlr[k]:
                        landmark_arr2d = np.array(nlr[k]).reshape(int(len(nlr[k])/3),3)
                        landmark_2Darrs[k] = (True, landmark_arr2d)
                    else:
                        landmark_2Darrs[k] = (False, None)
                insert_val = {'movie_id':nlr['movie_id'],'frame':nlr['frame'],'time':nlr['time']}
                for hand_key in ['right_hand','left_hand']:
                    if landmark_2Darrs[hand_key][0]:
                        # face mesh landmarkに対するhand landmarksの距離と最も近いインデックスを計算
                        (face_hand, face_hand_idx) = my_func.CalcDistance(landmark_2Darrs['face'][1], landmark_2Darrs[hand_key][1])
                        insert_val[f"face_{hand_key}"] = face_hand.tolist()
                        insert_val[f"face_{hand_key}_idx"] = face_hand_idx.tolist()
                        # hand landmarkに対するface mesh landmarkの距離と最も近いインデックスを計算
                        (hand_face, hand_face_idx) = my_func.CalcDistance(landmark_2Darrs[hand_key][1], landmark_2Darrs['face'][1])
                        insert_val[f"{hand_key}_face"] = hand_face.tolist()
                        insert_val[f"{hand_key}_face_idx"] = hand_face_idx.tolist()
                    else:
                        insert_val[f"face_{hand_key}"] = []
                        insert_val[f"face_{hand_key}_idx"] = []
                        insert_val[f"{hand_key}_face"] = []
                        insert_val[f"{hand_key}_face_idx"] = []

                insert_vals.append(insert_val)
                _count += 1
                if _count % 5000 == 0:
                    sql.BulkInsertRecords('Distances', insert_vals)
                    insert_vals = []
                    _count = 0
            start_frame = nlr['frame']
            # hand landmarkが存在するレコードを5000件抽出
            nlrs = sql.GetRecords('NormLandmarks',option={'sql_str':f'WHERE frame > {start_frame} AND (right_hand != "" OR left_hand != "") ORDER BY frame LIMIT 5000'})
        if len(insert_vals) > 0:
            sql.BulkInsertRecords('Distances', insert_vals)