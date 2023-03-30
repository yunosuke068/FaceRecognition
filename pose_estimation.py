import argparse
import itertools
from tqdm import tqdm
import numpy as np
from faceTouch import skelton_detection, movie_reader
from faceTouch import format

from pathlib import Path
FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]
POSE_KEYS = ['pose','face','left_hand','right_hand']
BLOCK = 10000

def run(
        movie_source = '0',
        project = '0',
        database = '0',
        reset = False
        ):
    project.mkdir(exist_ok=True)
    database = project.joinpath(database)
    print(movie_source)
    print(database)
    print(f"Start PoseEstimation...")
    
    if reset:
        print('database reset...')

    detector = skelton_detection.SkeltonDetection()
    movie = movie_reader.Movie(movie_source)
    sql = skelton_detection.LandmarkDB(database)
    if reset:
        print('database reset...')
        for table in sql.tableNames():
            sql.cursor.execute(f'DROP TABLE IF EXISTS {table};')
        sql.connect.commit()
        sql = skelton_detection.LandmarkDB(database)
        print('database reset complete')

    # Landmarksテーブルframeカラムの最大値を取得
    last_record = sql.cursor.execute("SELECT frame FROM Landmarks ORDER BY frame DESC LIMIT 1").fetchone()
    begin_frame = last_record[0] + 1 if last_record else 1
    end_frame = movie.frame_count + 1

    if end_frame == 1:
        print("movie frame count: 0")
        return 0
    
    pbar_frame = tqdm(np.arange(begin_frame,end_frame)) # progress bar
    insert_data_list = [] # バルクインサートするデータリスト
    insert_data_list_norm = [] # バルクインサートする正規化データリスト
    insert_data_list_distance = [] # バルクインサートする距離データリスト
    for frame in pbar_frame:
        time_str = format.seconds2time(frame / movie.fps) # 00:00:00 の文字列化
        pbar_frame.set_description(f"time:{time_str}, frame:{int(frame)}") # progress barに説名を追記
        
        ret, img = movie.readImage(frame) # 動画から画像を読み込み
        if ret == False: # 画像が読み込めなかった場合に処理をスキップ
            continue

        # ==== 骨格検出 ====
        detector.setImage(img) # 画像をセット
        landmarks = {k:detector.landmarks(k) for k in POSE_KEYS} # 骨格検出ランドマークを取得
        # pose,face,handsが全て検出されなかった場合に処理をスキップ
        if sum([1 for v in landmarks.values() if v == [[]]]) == len(POSE_KEYS):
            continue
        insert_data = landmarks.copy()
        insert_data.update({'frame':frame.item(),
                            'time':time_str,
                            'width':detector.image_width,
                            'height':detector.image_height})
        insert_data_list.append(insert_data)
        if len(insert_data_list) % BLOCK == 0: # バルクインサート
            sql.bulkInsertRecords('Landmarks', insert_data_list)
            insert_data_list = []

        # ==== 座標の正規化 ====
        parts_concat = []
        exist_flags = {}
        mesh_keys = ['face','right_hand','left_hand']
        for k in mesh_keys:
            array_size = len(landmarks[k])
            exist_flags[k] = {'exist':True, 'size':array_size} if array_size != 1 else {'exist':False, 'size':0}
            parts_concat += np.array(landmarks[k])[:,0:3].tolist() # ランドマークの可視性の値をリストから削除。[x, y, z, visivility]
        flat_points = np.array(list(itertools.chain.from_iterable(parts_concat))) # 二次元リスト一次元array化
        if exist_flags['face']['exist'] == False: # 顔が検出できていないとき、以降の処理をスキップ
            continue
        flat_points = format.convertLocalCoordinate(flat_points) # 相対座標系に変換
        begin_scope = 0
        norm_insert_data = insert_data.copy()
        del norm_insert_data['width'], norm_insert_data['height'], norm_insert_data['pose']
        for k in ['face','right_hand','left_hand']:
            if exist_flags[k]['exist']:
                end_scope = begin_scope+exist_flags[k]['size']*3
                arr = flat_points[begin_scope:end_scope]
                norm_insert_data[k] = arr.tolist()
                begin_scope = end_scope
            else:
                norm_insert_data[k] = []
        insert_data_list_norm.append(norm_insert_data)
        if len(insert_data_list_norm) % BLOCK == 0: # 正規化データのバルクインサート
            sql.bulkInsertRecords('NormLandmarks', insert_data_list_norm)
            insert_data_list_norm = []

        # ==== face mesh と hands の距離計算 ====
        # Landmarkを二次元ndarray化
        landmark_2Darrs = {}
        for k in ['face','right_hand','left_hand']:
            if norm_insert_data[k]:
                landmark_arr2d = np.array(norm_insert_data[k]).reshape(int(len(norm_insert_data[k])/3),3)
                landmark_2Darrs[k] = {'exist':True, 'array':landmark_arr2d}
            else:
                landmark_2Darrs[k] = {'exist':False, 'array':None}
        if landmark_2Darrs['right_hand']['exist'] == False & landmark_2Darrs['left_hand']['exist'] == False:
            continue
        insert_data_distance = {'frame':frame, 'time':time_str}
        for hand_key in ['right_hand', 'left_hand']:
            if landmark_2Darrs[hand_key]['exist']:
                # face mesh landmarkに対するhand landmarksの距離と最も近いインデックスを計算
                (face_hand, face_hand_idx) = format.CalcDistance(landmark_2Darrs['face']
                ['array'], landmark_2Darrs[hand_key]['array'])
                # hand landmarkに対するface mesh landmarkの距離と最も近いインデックスを計算
                (hand_face, hand_face_idx) = format.CalcDistance(landmark_2Darrs[hand_key]['array'], landmark_2Darrs['face']['array'])
                insert_data_distance.update({f'face_{hand_key}':face_hand.tolist(),
                                             f'face_{hand_key}_idx':face_hand_idx.tolist(),
                                             f'{hand_key}_face':hand_face.tolist(),
                                             f'{hand_key}_face_idx':hand_face_idx.tolist()})
            else:
                insert_data_distance.update({f'face_{hand_key}':[],
                                             f'face_{hand_key}_idx':[],
                                             f'{hand_key}_face':[],
                                             f'{hand_key}_face_idx':[]})
        insert_data_list_distance.append(insert_data_distance)
        if len(insert_data_list_distance) % BLOCK == 0: # 距離データのバルクインサート
            sql.bulkInsertRecords('Distances', insert_data_list_distance)
            insert_data_list_distance = []


        
    sql.bulkInsertRecords('Landmarks', insert_data_list)
    sql.bulkInsertRecords('NormLandmarks', insert_data_list_norm)
    sql.bulkInsertRecords('Distances', insert_data_list_distance)
    print("Finish SkeltonDetection")



def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument('--movie_source', type=Path, default=ROOT / 'faceTouch/movie/sample.mp4', help='動画のソースファイル')
    parser.add_argument('--project', type=Path, default=ROOT / 'faceTouch/project', help='結果の保存先ディレクトリ')
    parser.add_argument('--database', type=str, default='Landmark.db', help='保存先データベースの名前')
    parser.add_argument('--reset', default=False, action='store_true', help='database reset')
    opt = parser.parse_args()
    return opt

def main(opt):
    run(**vars(opt))

if __name__ == "__main__":
    opt = parse_opt()
    main(opt)