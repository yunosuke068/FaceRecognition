import numpy as np

# 秒を"00:00:00"に変換
def seconds2time(s):
    hours, remainder = divmod(s, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))

# 絶対座標系からローカル座標系に変換
def convertLocalCoordinate(row):
    R_GtoL, R_LtoG = rotationMatrix(row) # 絶対座標系からローカル座標系に変換する行列を取得
    local_row = []
    #print(np.arange(0,int(row.shape[0])))
    for idx in np.arange(0,int(row.shape[0]/3)):
        local_row.extend(np.sum(R_GtoL*np.append(getCoordinate(row, idx),1),axis=1)[0:3])
    return np.array(local_row)


def getCoordinate(row, index):
    return np.array([row[0+3*index], row[1+3*index], row[2+3*index]])

def rotationMatrix(row):
    origin = (getCoordinate(row,93) + getCoordinate(row,323))/2 #原点をface_meshランドマークの37番と267番の中点とする
    
    px = getCoordinate(row,93) - getCoordinate(row,323) # x軸はface_meshランドマークの37番から267番方向に
    
    py = getCoordinate(row,10) - origin # y軸はoriginからランドマークの0番方向に
    #py = test_data[152] - test_data[10]

    """それぞれの単位ベクトルを計算"""
    ex = px / np.linalg.norm(px) # 単位ベクトルは座標をベクトルの大きさ（長さ）で割ると求められる
    ez = np.cross(px, py) / np.linalg.norm(np.cross(px, py)) # np.crossは2つのベクトルの外積で、外積は入力した２つのベクトルに垂直なベクトル
    ey = np.cross(ez, ex)
    
    """変換行列 4x4 を作成"""
    #ローカル座標（犬から見た座標）を絶対座標に変換する行列
    R_LtoG = np.identity(4)
    R_LtoG[:3, :3] = np.array([ex, ey, ez]).T #回転行列を代入
    R_LtoG[:3, 3] = origin #移動ベクトルを代入
    

    #絶対座標からローカル座標（犬からみた座標）に変換する行列
    R_GtoL = np.identity(4)
    R_GtoL[:3, :3] = np.array([ex, ey, ez])
    R_GtoL[:3, 3] = - np.dot(np.array([ex, ey, ez]), origin)

    return (R_GtoL, R_LtoG)


def CalcDistance(landmarks1, landmarks2):
    dis_arrs = np.array([])
    dis_arg_arrs = np.array([], dtype=np.int64)
    for landmark1 in landmarks1:
        dis_arr = np.array([np.linalg.norm(landmark1-landmark2) for landmark2 in landmarks2])
        dis_arrs = np.append(dis_arrs,np.min(dis_arr))
        dis_arg_arrs = np.append(dis_arg_arrs, np.argmin(dis_arr))
    return (dis_arrs, dis_arg_arrs)