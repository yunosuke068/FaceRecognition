|ファイル|実行内容|
|----|----|
|```python 01_face_recognition.py"```|顔認識処理。insightfaceで顔を検出し、FaceDBのFacesテーブルに矩形座標、特徴量、landmark座標を追加|
|```python 02_subject_grouping.py"```|人物のグループ化。FaceSubjectsテーブルにレコードを追加。FacesとSubjectsの紐づけ。連続して顔認識が行われている人物の結合|
|```python 03_subject_combining.py main_subject_bond.py "[DBファイルパス]"```|人物の結合処理。Subjectsどうしの類似度を計算し、Subjectsどうしを結合。顔認識処理が失敗した人物の結合|
|```python make_bonds_image.py "[DBファイルパス]"```|Subjectsの画像を生成|
|```python main_movie_split.py "[DBファイルパス]"```|動画を人物ごとに分割|
|```python .\movie_management.py 0```|movie_manageファイルをアップデート|
|```python .\movie_management.py 1```|FaceDBをアップデート|