import mediapipe as mp
from sklearn.metrics.pairwise import cosine_similarity, cosine_distances
import cv2

class SkeltonDetection:
    def __init__(self):
        # mediapipeの変数
        self.mp_holistic = mp.solutions.holistic
        self.holistic = self.mp_holistic.Holistic(static_image_mode=True, model_complexity=2)
        # self.holistic = self.mp_holistic.Holistic(min_detection_confidence=0.5,min_tracking_confidence=0.5)
        self.mp_drawing = mp.solutions.drawing_utils
        self.drawing_spec = self.mp_drawing.DrawingSpec(thickness=1, circle_radius=1)
        self.mp_drawing_styles = mp.solutions.drawing_styles
    
    def setImage(self,img):
        self.image_width = img.shape[1]
        self.image_height = img.shape[0]
        self.result = self.holistic.process(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))

    def landmarks(self,key): # key = pose || face || right_hand || left_hand
        if key == 'pose':
            if self.result.pose_landmarks: return self.landmarkList(self.result.pose_landmarks)
        elif key == 'face':
            if self.result.face_landmarks: return  self.landmarkList(self.result.face_landmarks)
        elif key == 'right_hand':
            if self.result.right_hand_landmarks: return self.landmarkList(self.result.right_hand_landmarks)
        elif key == 'left_hand':
            if self.result.left_hand_landmarks:return self.landmarkList(self.result.left_hand_landmarks)

        return [[]]

    def landmarkList(self,landmarks):
        return  [[row.x,row.y,row.z,row.visibility] for row in landmarks.landmark]
    

import sqlite3
def adapter(lst):
    # リスト型を文字列に変換
    return ";".join([str(i) for i in lst])

# 一次元リストの変換
def converter1d_f(bstr):
    # バイト文字列をリスト型に変換
    return [float(item.decode('utf-8'))  for item in bstr.split(bytes(b';'))]

def converter1d_i(bstr):
    # バイト文字列をリスト型に変換
    return [int(item.decode('utf-8'))  for item in bstr.split(bytes(b';'))]

# 二次元リストの変換
def converter2d(bstr):
    if bstr.split(bytes(b';'))==[b'[]']:
        return [[]]
    else:
        # バイト文字列をリスト型に変換
        return [[float(v) for v in item.decode('utf-8').replace('[','').replace(']','').split(', ')]  for item in bstr.split(bytes(b';'))]

sqlite3.register_adapter(list, adapter)
sqlite3.register_converter("ILIST1d", converter1d_i)
sqlite3.register_converter("FLIST1d", converter1d_f)
sqlite3.register_converter("FLIST2d", converter2d)

class DB:
    def __init__(self,db_path):
        self.connect = conn = sqlite3.connect(db_path,detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = cur = self.connect.cursor()
        
        # テーブル毎のカラム名
        self.table_column = {}

    def createUpdatedAtTrigger(self,table):
        self.cursor.execute(f"""CREATE TRIGGER IF NOT EXISTS trigger_{table}_updated_at AFTER UPDATE ON {table}
        BEGIN
            UPDATE {table} SET updated_at = DATETIME('now', 'localtime') WHERE rowid == NEW.rowid;
        END;""")

    def sqlToColumn(self,table_name):
        return [row[1] for row in self.cursor.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
    
    def tableNames(self):
        # 除外するテーブル。
        # sqlite_sequenceはsqliteが自動で生成するテーブルのため、除外
        exclusion = ['sqlite_sequence']
        
        names = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY NAME;").fetchall()
        
        names = [name[0] for name in names if not name[0] in exclusion]
        print(names)
        return names
    
    def createTable(self, table, column):
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table}({column})")
        self.table_column[table] = self.sqlToColumn(table)
    
    def bulkInsertRecords(self,table,values_list):
        values = values_list[0]
        col = "".join([f"{k}," for k in values.keys()])[:-1]
        val = "".join(["?," for v in values.values()])[:-1]
        insert_data = []
        for values in values_list:
            insert_data.append([v for v in values.values()])
        SQL = f"INSERT INTO {table}({col}) VALUES({val})"
        # print(SQL)
        self.cursor.executemany(SQL,insert_data)
        self.connect.commit()

class LandmarkDB(DB):
    def __init__(self,db_path):
        super().__init__(db_path)

        # create Movies table
        table = "Movies"
        column = """name VARCHER(32) unique,
                    fps NUMERIC(4,2),
                    frame INT,
                    path TEXT,
                    created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                    updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))"""
        self.createTable(table,column)
        

        # create Landmarks table
        table = "Landmarks"
        column = """movie_id INT,
                    frame INT,
                    time TEXT,
                    width FLOAT,
                    height FLOAT,
                    pose FLIST2d,
                    face FLIST2d,
                    right_hand FLIST2d,
                    left_hand FLIST2d"""
        self.createTable(table,column)

        # create NormLandmarks table
        table = "NormLandmarks"
        column = """movie_id INT,
                    frame INT,
                    time TEXT,
                    pose FLIST1d,
                    face FLIST1d,
                    right_hand FLIST1d,
                    left_hand FLIST1d"""
        self.createTable(table,column)

        # create Distances table
        table = "Distances"
        column = """movie_id INT,
                    frame INT,
                    time TEXT,
                    face_right_hand FLIST1d,
                    right_hand_face FLIST1d,
                    face_left_hand FLIST1d,
                    left_hand_face FLIST1d,
                    face_right_hand_idx ILIST1d,
                    right_hand_face_idx ILIST1d,
                    face_left_hand_idx ILIST1d,
                    left_hand_face_idx ILIST1d"""
        self.createTable(table,column)

        # create DistanceClass table
        table = "DistanceClass"
        column = """movie_id INT,
                    frame INT,
                    time TEXT,
                    right_class INT,
                    left_class INT"""
        self.createTable(table,column)

        # create LandmarkClass table
        table = "LandmarkClass"
        column = """movie_id INT,
                    frame INT,
                    time TEXT,
                    right_class INT,
                    left_class INT"""
        self.createTable(table,column)

        # updated_atのトリガーを追加
        for table in self.tableNames():
            self.createUpdatedAtTrigger(table)

        # データベースへコミット。これで変更を反映される
        self.connect.commit()