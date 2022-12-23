|ファイル|実行内容|
|----|----|
|```python main.py "[DBファイルパス]"```|insightfaceで顔を検出し、FaceDBのFacesテーブルに矩形座標、特徴量、landmark座標を追加|
|```python main_subject.py "[DBファイルパス]"```|FaceSubjectsテーブルにレコードを追加。FacesとSubjectsの紐づけ|
|```python main_subject_bond.py "[DBファイルパス]"```|Subjectsどうしの類似度を計算し、Subjectsどうしを結合|
|```python make_bonds_image.py "[DBファイルパス]"```|Subjectsの画像を生成|
|```python main_movie_split.py "[DBファイルパス]"```|動画を人物ごとに分割|
|```python .\movie_management.py 0```|movie_manageファイルをアップデート|
|```python .\movie_management.py 1```|FaceDBをアップデート|