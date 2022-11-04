import sqlite3

def adapter(lst):
    # リスト型を文字列に変換
    return ";".join([str(i) for i in lst])

# 一次元リストの変換
def converter1d(bstr):
    # バイト文字列をリスト型に変換
    return [float(item.decode('utf-8'))  for item in bstr.split(bytes(b';'))]

# 二次元リストの変換
def converter2d(bstr):
    # バイト文字列をリスト型に変換
    return [[float(v) for v in item.decode('utf-8').replace('[','').replace(']','').split(', ')]  for item in bstr.split(bytes(b';'))]

sqlite3.register_adapter(list, adapter)
sqlite3.register_converter("FLIST1d", converter1d)
sqlite3.register_converter("FLIST2d", converter2d)

class LandmarkDB:
    def __init__(self,db_path):
        self.connect = conn = sqlite3.connect(db_path,detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = cur = self.connect.cursor()

        # create Movies table
        self.movies_col = ['id','name', 'fps', 'frame', 'path', 'created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Movies(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name varcher(32) unique,
        fps numeric(4,2),
        frame int,
        path text,
        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
        )""")

        # create Completes table
        self.completes_col = ['id','movie_id','flag_pose_esitmation','created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Completes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id int,
        flag_pose_esitmation boolean,
        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
        )""")

        # create Landmarks table
        self.landmarks_col = ['id','movie_id','frame','time','pose','face','right_hand','left_hand','created_at','updated_at']
        cur.execute("""CREATE TABLE IF NOT EXISTS Landmarks(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id INT,
        frame INT,
        time TEXT,
        pose FLIST2d,
        face FLIST2d,
        right_hand FLIST2d,
        left_hand FLIST2d,
        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
        )""")

        # updated_atのトリガーを追加
        for table in ['Movies','Completes','Landmarks']:
            self.CreateUpdatedAtTrigger(table)

        # データベースへコミット。これで変更を反映される
        conn.commit()
    
    """ALL"""
    def GetRecords(self,table='',col=['*'],terms={}):
        if '*' in col:
            col = self.col_dic[table]
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        SQL = f"SELECT {col_str} FROM {table} {terms_SQL}"
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

    def UpdateRecords(self,table,terms,values):
        cur = self.cursor
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        id = cur.execute(f"SELECT id FROM {table} {terms_SQL}").fetchone()
        if id:
            for k in values.keys():
                if isinstance(values[k], str):
                    values[k] = f"'{values[k]}'"
                if values[k] is None:
                    values[k] = 'NULL'
            set_str = ''.join([f' {k} = {v},' for k,v in zip(values.keys(),values.values())])[:-1]
            cur.execute(f"UPDATE {table} SET{set_str} WHERE id = {id[0]}")
            self.connect.commit()
        else:
            self.InsertRecords(table,values)

    def DeleteRecords(self,table,terms):
        cur = self.cursor
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        SQL = f"DELETE FROM {table} {terms_SQL}"
        self.cursor.execute(SQL)
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
                terms[k] = f"'{terms[k]}'"
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

        
def main():
    print('sql_func')

if __name__ == '__main__':

    main()

