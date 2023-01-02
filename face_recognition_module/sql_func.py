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

class FaceDB:
    def __init__(self,db_path):

        self.connect = conn = sqlite3.connect(db_path,detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = cur = self.connect.cursor()

        # create Movies table
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
        cur.execute("""CREATE TABLE IF NOT EXISTS Completes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id int,
        flag_main boolean,
        flag_subject boolean,
        flag_bond boolean,
        flag_split boolean,
        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
        )""")

        # create Faces table
        SQL = """CREATE TABLE IF NOT EXISTS Faces(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id INTEGER,
        frame int,
        bbox FLIST1d,
        kps FLIST2d,
        det_score float,
        landmark_3d_68 FLIST2d,
        pose FLIST1d,
        landmark_2d_106 FLIST2d,
        gender int,
        age int,
        embedding FLIST1d,

        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),

        FOREIGN KEY (movie_id) REFERENCES Movies(id)
        )"""
        cur.execute(SQL)

        # create Subjects table
        SQL = """CREATE TABLE IF NOT EXISTS Subjects(
        id INTEGER,
        movie_id int,
        order_number int,
        face_id int,

        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        FOREIGN KEY (movie_id) REFERENCES Movies(id)
        )"""
        cur.execute(SQL)

        # create FaceSubjects table
        SQL = """CREATE TABLE IF NOT EXISTS FaceSubjects(
        id INTEGER,
        face_id int,
        subject_id int,

        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
        )"""
        cur.execute(SQL)

        # create Bonds table
        SQL = """CREATE TABLE IF NOT EXISTS Bonds(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id_0 int,
        subject_id_1 int,
        similarity float,
        frame_difference int,

        created_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime')),
        updated_at TEXT NOT NULL DEFAULT (DATETIME('now', 'localtime'))
        )"""
        cur.execute(SQL)

        # 外部キーを有効化
        cur.execute('PRAGMA foreign_keys=true')

        # updated_atのトリガーを追加
        for table in ['Faces','Movies','Completes','FaceSubjects','Subjects']:
            self.CreateUpdatedAtTrigger(table)

        # データベースへコミット。これで変更を反映される
        conn.commit()
        self.bonds_col = ['id','subject_id_0','subject_id_1','similarity','frame_difference','created_at','updated_at']
        self.movies_col = ['id','name', 'fps', 'frame', 'path', 'created_at','updated_at']
        self.completes_col = ['id','movie_id','flag_main','flag_subject','flag_bond','flag_split','created_at','updated_at']
        self.faces_col = ['id','movie_id', 'frame', 'bbox', 'kps', 'det_score', 'landmark_3d_68', 'pose', 'landmark_2d_106', 'gender', 'age', 'embedding', 'created_at','updated_at']
        self.subjects_col = ['id','movie_id','order_number','face_id','created_at','updated_at']
        self.face_subjects_col = ['id','face_id','subject_id','created_at','updated_at']
        self.col_dic = {'Movies':self.movies_col,'Bonds':self.bonds_col,'Completes':self.completes_col,'Faces':self.faces_col,'Subjects':self.subjects_col,'FaceSubjects':self.face_subjects_col}

    def DeleteTable(self):
        cur = self.cursor
        cur.execute('drop table Faces')
        cur.execute('drop table Movies')

    def CreateUpdatedAtTrigger(self,table):
        self.cursor.execute(f"""CREATE TRIGGER IF NOT EXISTS trigger_{table}_updated_at AFTER UPDATE ON {table}
        BEGIN
            UPDATE {table} SET updated_at = DATETIME('now', 'localtime') WHERE rowid == NEW.rowid;
        END;""")

    """Movies"""
    # Movies Table
    # Moviesのレコードの追加
    def InsertMovies(self,name,fps,frame,path):
        cur = self.cursor
        SQL = f"SELECT * FROM Movies WHERE name = '{name}'"
        if len(cur.execute(SQL).fetchall()) == 0:
            SQL = f"insert into Movies(name,fps,frame,path) values('{name}',{fps},{frame},'{path}') on conflict (name) do nothing"
            cur.execute(SQL)
            self.connect.commit()
        else:
            print(f'Error:Record with the name "{name}" exists in the Movies table.')

    # Movies Table
    # Moviesのレコードの追加または、fps,frameの更新
    def UpdateMovies(self,name,fps,frame,path):
        cur = self.cursor
        id = cur.execute(f"SELECT id FROM Movies WHERE name = '{name}'").fetchone()
        if id:
            SQLs = [f"update Movies set fps = {fps} WHERE id = {id[0]}",
                    f"update Movies set frame = {frame} WHERE id = {id[0]}",
                    f"update Movies set path = '{path}' WHERE id = {id[0]}"]
            for SQL in SQLs:
                cur.execute(SQL)
            self.connect.commit()
        else:
            self.InsertMovies(name,fps,frame,path)

    def GetMovies(self,col=['*'],terms={}):
        if '*' in col:
            col = ['id','name', 'fps', 'frame', 'path', 'created_at','updated_at']
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        SQL = f"SELECT {col_str} FROM Movies  {terms_SQL}"
        records = self.cursor.execute(SQL).fetchall()
        return self.GenerateRecordsInDict(col,records)

    # Movies Table
    # nameからデータベースにおけるidを取得
    def GetMoviesProperty(self):
        SQL = f"SELECT id,name,fps,frame,path FROM Movies"
        return [{'id':v[0],'name':v[1],'fps':v[2],'frame':v[3],'path':v[4]} for v in self.cursor.execute(SQL).fetchall()]

    """Completes"""
    def InsertCompletes(self,value):
        if not isinstance(value, dict):
            print('values is not dict.')
            return 0
        col = ''.join([f"{k}," for k in value.keys()])[0:-1]
        val = ''.join([f'{v},' for v in value.values()])[0:-1]
        SQL = f"insert into Completes({col}) values({val})"
        self.cursor.execute(SQL)
        self.connect.commit()

    # Completesのレコードの追加または、更新
    def UpdateCompletes(self,value):
        if not isinstance(value, dict):
            print('value is not dict.')
            return 0
        cur = self.cursor
        id = cur.execute(f"SELECT id FROM Completes WHERE movie_id = {value['movie_id']}").fetchone()
        if id:
            set_str = ''.join([f' {k} = {v},' for k,v in zip(value.keys(),value.values())])[:-1]
            SQLs = [f"update Completes set{set_str} WHERE id = {id[0]}"]
            for SQL in SQLs:
                cur.execute(SQL)
            self.connect.commit()
        else:
            self.InsertCompletes(value)

    def GetCompletes(self,col=['*'],terms={}):
        if '*' in col:
            col = ['id','movie_id','flag_main','flag_subject','flag_bond','flag_split','created_at','updated_at']
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        SQL = f"SELECT {col_str} FROM Completes  {terms_SQL}"
        records = self.cursor.execute(SQL).fetchall()
        return self.GenerateRecordsInDict(col,records)


    """Subjects"""
    def InsertSubjects(self,movie_id,face_id):
        records = self.GetSubjects({'movie_id':movie_id})
        if len(records) == 0:
            order_number = 1
        else:
            order_number = max([record['order_number'] for record in records])+1
        insert_data=[movie_id,order_number,face_id]
        SQL = "insert into Subjects(movie_id,order_number,face_id) values(?,?,?)"
        # self.connect.executemany(SQL,insert_data)
        self.connect.execute(SQL,insert_data)
        self.connect.commit()

    def GetSubjects(self,terms):
        if not isinstance(terms, dict):
            print('terms is not dict.')
            return 0
        terms_SQL = ""
        flag = True
        for k in terms.keys():
            if flag:
                terms_SQL += f"{k} = {terms[k]} "
                flag = False
            else:
                terms_SQL += f"AND {k} = {terms[k]} "
        SQL = "SELECT * FROM Subjects WHERE " + terms_SQL
        records = self.cursor.execute(SQL).fetchall()

        col = ['id','movie_id','order_number','face_id','created_at','updated_at']

        output_records = []
        for record in records:
            record_dic = {}
            for i in range(len(col)):
                record_dic[col[i]] = record[i]
            output_records.append(record_dic)
        return output_records

    """Faces"""
    def InsertFaces(self,movie_id,frame,args):
        insert_data = [movie_id,frame]
        for k in args.keys():
            arg = args[k]
            if k in ['bbox','kps','landmark_3d_68','pose','landmark_2d_106','embedding']:
                arg = arg.tolist()
            elif k in ['det_score']:
                arg = float(arg)
            elif k in ['gender','age']:
                arg = int(arg)

            insert_data.append(arg)
        # print(insert_data)
        SQL = "insert into Faces(movie_id,frame,bbox,kps,det_score,landmark_3d_68,pose,landmark_2d_106,gender,age,embedding) values(?,?,?,?,?,?,?,?,?,?,?)"
        # self.connect.executemany(SQL,insert_data)
        self.connect.execute(SQL,insert_data)
        self.connect.commit()

    # def GetFaces(self,id):
    #     SQL = f"SELECT * FROM Faces WHERE id = {id}"
    #     return self.cursor.execute(SQL).fetchone()

    def GetFaces(self,col,terms):
        if '*' in col:
            col = ['id','movie_id', 'frame', 'bbox', 'kps', 'det_score', 'landmark_3d_68', 'pose', 'landmark_2d_106', 'gender', 'age', 'embedding', 'created_at','updated_at']
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        SQL = f"SELECT {col_str} FROM Faces  {terms_SQL}"
        records = self.cursor.execute(SQL).fetchall()
        return self.GenerateRecordsInDict(col,records)


    def GetFacesLastFrame(self,movie_id):
        cur = self.cursor
        SQL = f"SELECT frame FROM Faces WHERE movie_id = {movie_id} order by frame"
        records = cur.execute(SQL).fetchall()
        if len(records) > 0:
            return records[-1][0]
        else:
            return 1

    def GetFacesLastRecord(self,col,terms):
        if '*' in col:
            col = ['id','movie_id', 'frame', 'bbox', 'kps', 'det_score', 'landmark_3d_68', 'pose', 'landmark_2d_106', 'gender', 'age', 'embedding', 'created_at','updated_at']
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        cur = self.cursor
        SQL = f"SELECT {col_str} FROM Faces  {terms_SQL} order by id desc limit 1"
        records = cur.execute(SQL).fetchall()
        return self.GenerateRecordsInDict(col,records)[0]

    def DeleteFacesByFrame(self,movie_id,frame):
        cur = self.cursor
        SQL = f"delete FROM Faces WHERE movie_id = {movie_id} AND frame = {frame}"
        cur.execute(SQL)
        self.connect.commit()


    """FaceSubjects"""
    face_subjects_col = ['id','face_id','subject_id','created_at','updated_at']
    def InsertFaceSubjects(self,face_id,subject_id):
        SQL = f"insert into FaceSubjects(face_id,subject_id) values({face_id},{subject_id})"
        self.cursor.execute(SQL)
        self.connect.commit()

    # FaceSubjectsのレコードの追加または、subject_idの更新
    def UpdateFaceSubjects(self,face_id,subject_id):
        cur = self.cursor
        id = cur.execute(f"SELECT id FROM FaceSubjects WHERE face_id = '{face_id}'").fetchone()
        if id:
            SQLs = [f"update FaceSubjects set subject_id = {subject_id} WHERE id = {id[0]}"]
            for SQL in SQLs:
                cur.execute(SQL)
            self.connect.commit()
        else:
            self.InsertFaceSubjects(face_id,subject_id)

    def GetFaceSubjects(self,col,terms):
        if '*' in col:
            col = face_subjects_col
        terms_SQL = self.GenerateWhereAndTerms(terms) if len(terms.keys()) > 0 else ""
        col_str = self.GenerateColumnStr(col)
        SQL = f"SELECT {col_str} FROM FaceSubjects  {terms_SQL}"
        records = self.cursor.execute(SQL).fetchall()
        return self.GenerateRecordsInDict(col,records)

    def GetFaceSubjectsLastRecord(self,subject_id):
        cur = self.cursor
        SQL = f"SELECT * FROM FaceSubjects WHERE subject_id = {subject_id} order by id desc limit 1"
        record = cur.execute(SQL).fetchone()
        return record


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
    # path = 'db/FaceDB.db'
    # #FaceDB(path).DeleteTable()
    # FaceDB(path)
    # print(f'create {path}')
    print('sql_func')

if __name__ == '__main__':
    main()
