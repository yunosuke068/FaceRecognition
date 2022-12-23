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

    def CreateUpdatedAtTrigger(self,table):
        self.cursor.execute(f"""CREATE TRIGGER IF NOT EXISTS trigger_{table}_updated_at AFTER UPDATE ON {table}
        BEGIN
            UPDATE {table} SET updated_at = DATETIME('now', 'localtime') WHERE rowid == NEW.rowid;
        END;""")

    def SQLToColumn(self,table_name):
        return [row[1] for row in self.cursor.execute(f"PRAGMA table_info('{table_name}')").fetchall()]

    """ALL"""
    def GetRecords(self,table='',col=['*'],terms={},option={'sql_str':''}):
        if '*' in col:
            col = self.col_dic[table]
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        SQL = f"SELECT {col_str} FROM {table} {terms_SQL} {option['sql_str']}"
        records = self.cursor.execute(SQL).fetchall()
        return self.GenerateRecordsInDict(col,records)

    def InsertRecords(self,table,values):
        col = "".join([f"{k}," for k in values.keys()])[:-1]
        val = "".join(["?," for v in values.values()])[:-1]
        insert_data = [v for v in values.values()]
        SQL = f"INSERT INTO {table}({col}) VALUES({val})"
        # print(SQL)
        self.cursor.execute(SQL,insert_data)
        self.connect.commit()
    
    def BulkInsertRecords(self,table,values_list):
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

    def UpdateRecords(self,table,terms,values):
        cur = self.cursor
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        id = cur.execute(f"SELECT id FROM {table} {terms_SQL}").fetchone()
        if id:
            # for k in values.keys():
            #     if isinstance(values[k], str):
            #         s = values[k]#.replace("'",'').replace('"','')
            #         values[k] = f"'{s}'"
            #     if values[k] is None:
            #         values[k] = 'NULL'
            set_str = ''.join([f' {k} = ?,' for k in values.keys()])[:-1]
            SQL = f"UPDATE {table} SET{set_str} WHERE id = {id[0]}"
            insert_data = [v for v in values.values()]
            # print(SQL)
            cur.execute(SQL,insert_data)
            self.connect.commit()
        else:
            self.InsertRecords(table,values)

    def DeleteRecords(self,table,terms):
        cur = self.cursor
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        SQL = f"DELETE FROM {table} {terms_SQL}"
        self.cursor.execute(SQL)
        # self.connect.commit()

    def Commit(self):
        self.connect.commit()

    """Other"""
    # face_idが一致するFaceSubjectsを取得
    def GetSubjectIdentify(self,face_id):
        face_subject_records = self.GetFaceSubjects(['id','face_id','subject_id'],{'face_id':face_id})
        print(face_subject_records)
        if len(face_subject_records) > 0:
            subject_id = face_subject_records[0]['subject_id']
            subject_records = self.GetSubjects({'id':subject_id})
            if len(subject_records) > 0:
                subject_record = subject_records[0]
                movie_records = self.GetMovies(['name'],{'id':subject_record['movie_id']})
                #print('movie_records',movie_records)
                movie_name = movie_records[0]['name']

                return f"{movie_name}_{subject_record['order_number']}"
            else:
                return 0

    def GenerateRecordsInDict(self,col,records):
        output_records = []
        for record in records:
            record_dic = {}
            for i in range(len(col)):
                record_dic[col[i]] = record[i]
            output_records.append(record_dic)
        return output_records

    def GenerateWhereAndTerms(self,terms):
        if not isinstance(terms, dict):
            print('terms is not dict.')
            return 0
        terms_SQL = ""
        flag = True
        for k in terms.keys():
            if isinstance(terms[k], str):
                s = terms[k]#.replace("'",'').replace('"','')
                terms[k] = f"'{s}'"
            if flag:
                terms_SQL += f"WHERE {k} = {terms[k]} "
                flag = False
            else:
                terms_SQL += f"AND {k} = {terms[k]} "
        return terms_SQL

    def GenerateColumnStr(self,col):
        col_str = col[0]
        for c in col[1:]:
            col_str += f",{c}"
        return col_str

class MoviesDB(DB):
    def __init__(self,movie_path):
        super().__init__(movie_path)
        cur = self.cursor

        # create Movies table
        # self.movies_col = ['id','name', 'fps', 'frame', 'path', 'created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Movies(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name varcher(32) unique,
                        fps numeric(4,2),
                        frame int,
                        path text,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.movies_col = self.SQLToColumn('Movies')

        # create Completes table
        # self.completes_col = ['id','movie_id','flag_pose_esitmation','created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Completes(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        movie_id int,
                        flag_pose_esitmation INT,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.completes_col = self.SQLToColumn('Completes')

        # updated_atのトリガーを追加
        for table in ['Movies','Completes']:
            self.CreateUpdatedAtTrigger(table)

        # データベースへコミット。これで変更を反映される
        self.connect.commit()

        self.col_dic = {'Movies':self.movies_col,'Completes':self.completes_col}




class LandmarkDB(DB):
    def __init__(self,db_path):
        super().__init__(db_path)
        cur = self.cursor

        # create Movies table
        # self.movies_col = ['id','name', 'fps', 'frame', 'path', 'created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Movies(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name varcher(32) unique,
                        fps numeric(4,2),
                        frame int,
                        path text,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.movies_col = self.SQLToColumn('Movies')

        # create Completes table
        # self.completes_col = ['id','movie_id','flag_pose_esitmation','created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Completes(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        movie_id int,
                        flag_pose_esitmation INT,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.completes_col = self.SQLToColumn('Completes')

        # create Landmarks table
        # self.landmarks_col = ['movie_id','frame','time', 'width', 'height','pose','face','right_hand','left_hand','created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Landmarks(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        width FLOAT,
                        height FLOAT,
                        pose FLIST2d,
                        face FLIST2d,
                        right_hand FLIST2d,
                        left_hand FLIST2d,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.landmarks_col = self.SQLToColumn('Landmarks')

        # create NormLandmarks table
        # self.normlandmarks_col = ['movie_id','frame','time','pose','face','right_hand','left_hand','created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS NormLandmarks(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        pose FLIST1d,
                        face FLIST1d,
                        right_hand FLIST1d,
                        left_hand FLIST1d,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.normlandmarks_col = self.SQLToColumn('NormLandmarks')

        # create Distances table
        cur.execute("""CREATE TABLE IF NOT EXISTS Distances(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        face_right_hand FLIST1d,
                        right_hand_face FLIST1d,
                        face_left_hand FLIST1d,
                        left_hand_face FLIST1d,
                        face_right_hand_idx ILIST1d,
                        right_hand_face_idx ILIST1d,
                        face_left_hand_idx ILIST1d,
                        left_hand_face_idx ILIST1d,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.distances_col = self.SQLToColumn('Distances')

        # create DistanceClass table
        cur.execute("""CREATE TABLE IF NOT EXISTS DistanceClass(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        right_class INT,
                        left_class INT,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.distanceclass_col = self.SQLToColumn('DistanceClass')

        # create LandmarkClass table
        cur.execute("""CREATE TABLE IF NOT EXISTS LandmarkClass(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        right_class INT,
                        left_class INT,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.landmarkclass_col = self.SQLToColumn('LandmarkClass')

        # create KmeansClass table
        cur.execute("""CREATE TABLE IF NOT EXISTS KmeansClass(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        direction TEXT,
                        frame_group_idx INT,
                        class INT,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.kmeansclass_col = self.SQLToColumn('KmeansClass')

        # create KmeansLabels table
        cur.execute("""CREATE TABLE IF NOT EXISTS KmeansLabels(
                        kmeans_class INT,
                        touch_class INT,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.kmeanslabels_col = self.SQLToColumn('KmeansClass')

        # create RnnClass table
        cur.execute("""CREATE TABLE IF NOT EXISTS RnnClass(
                        movie_id INT,
                        frame INT,
                        time TEXT,
                        direction TEXT,
                        frame_group_idx INT,
                        class INT,
                        distances FLIST1D,
                        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
                        )""")
        self.rnnclass_col = self.SQLToColumn('RnnClass')

        # updated_atのトリガーを追加
        for table in ['Movies','Completes','Landmarks', 'NormLandmarks', 'Distances','DistanceClass','LandmarkClass','KmeansClass','KmeansLabels','RnnClass']:
            self.CreateUpdatedAtTrigger(table)

        # データベースへコミット。これで変更を反映される
        self.connect.commit()

        self.col_dic = {'Movies':self.movies_col,
                        'Completes':self.completes_col,
                        'Landmarks':self.landmarks_col,
                        'NormLandmarks':self.normlandmarks_col,
                        'Distances':self.distances_col,
                        'DistanceClass':self.distanceclass_col,
                        'LandmarkClass':self.landmarkclass_col,
                        'KmeansClass':self.kmeansclass_col,
                        'KmeansLabels':self.kmeanslabels_col,
                        'RnnClass':self.rnnclass_col}


def main():
    print('sql_func')

if __name__ == '__main__':

    main()

