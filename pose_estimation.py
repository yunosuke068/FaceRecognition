import argparse
from tqdm import tqdm
import numpy as np
from faceTouch import skelton_detection, movie_reader
from faceTouch import format

from pathlib import Path
FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]
POSE_KEYS = ['pose','face','left_hand','right_hand']
BLOCK = 20000

def run(
        movie_source = '0',
        project = '0',
        database = '0'
        ):
    project.mkdir(exist_ok=True)
    database = project.joinpath(database)
    print(movie_source)
    print(database)
    print(f"Start PoseEstimation...")
    

    detector = skelton_detection.SkeltonDetection()
    movie = movie_reader.Movie(movie_source)
    sql = skelton_detection.LandmarkDB(database)

    # Landmarksテーブルframeカラムの最大値を取得
    last_record = sql.cursor.execute("SELECT frame FROM Landmarks ORDER BY frame DESC LIMIT 1").fetchone()
    begin_frame = last_record[0] + 1 if last_record else 1
    end_frame = movie.frame_count + 1
    
    pbar_frame = tqdm(np.arange(begin_frame,end_frame)) # progress bar
    insert_data_list = [] # バルクインサートするデータリスト
    for frame in pbar_frame:
        time_str = format.seconds2time(frame / movie.fps) # 00:00:00 の文字列化
        pbar_frame.set_description(f"time:{time_str}, frame:{frame}") # progress barに説名を追記
        
        ret, img = movie.readImage(frame) # 動画から画像を読み込み
        if ret == False: # 画像が読み込めなかった場合に処理をスキップ
            continue

        detector.setImage(img) # 画像をセット
        landmarks = {k:detector.landmarks(k) for k in POSE_KEYS} # 骨格検出ランドマークを取得
        
        # 骨格検出されなかった場合に処理をスキップ
        if sum([1 for v in landmarks.values() if v == [[]]]) == len(POSE_KEYS):
            continue
        
        insert_data = landmarks.copy()
        insert_data.update({'frame':frame.item(),
                            'time':time_str,
                            'width':detector.image_width,
                            'height':detector.image_height})
        insert_data_list.append(insert_data)

        # バルクインサート
        if len(insert_data_list) % BLOCK == 0:
            sql.bulkInsertRecords('Landmarks', insert_data_list)
            insert_data_list = []
    sql.bulkInsertRecords('Landmarks', insert_data_list)
    print("Finish SkeltonDetection")



def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument('--movie_source', type=Path, default=ROOT / 'faceTouch/movie/sample.mp4', help='動画のソースファイル')
    parser.add_argument('--project', type=Path, default=ROOT / 'faceTouch/project', help='結果の保存先ディレクトリ')
    parser.add_argument('--database', type=str, default='Landmark.db', help='保存先データベースの名前')
    opt = parser.parse_args()
    return opt

def main(opt):
    run(**vars(opt))

if __name__ == "__main__":
    opt = parse_opt()
    main(opt)