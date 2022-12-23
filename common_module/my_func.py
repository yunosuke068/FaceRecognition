import numpy as np
import matplotlib.pyplot as plt

def ComputeSim(feat1, feat2):
    return np.dot(feat1, feat2) / (np.linalg.norm(feat1) * np.linalg.norm(feat2))

def RecordsCombination(records_prev,records_current):
    r_ids = []
    rp_ids = []
    sims = []

    for r in records_current:
        sim_dic = {}
        for rp in records_prev:
            sim_dic[ComputeSim(r['embedding'],rp['embedding'])] = rp['id']
        sim = max(list(sim_dic.keys()))
        r_ids.append(r['id'])
        rp_ids.append(sim_dic[sim])
        sims.append(sim)

    rc_ids = [rc['id'] for rc in records_current]
    rp_ids = [rp['id'] for rp in records_prev]
    sim_matrix = []
    for  rc in records_current:
        sim_matrix.append([ComputeSim(rc['embedding'], rp['embedding']) for rp in records_prev])

    sim_matrix = np.array(sim_matrix)
    if len(rc_ids) == len(rp_ids):
        max_ids = np.argmax(sim_matrix, axis=1)
        if len(np.unique(max_ids)) == len(rc_ids):
            return [rc_ids,[rp_ids[max_id] for max_id in max_ids]]

        elif len(np.unique(max_ids)) < len(rc_ids): # 重複がある
            max_ids = np.argmax(sim_matrix, axis=1)
            (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
            max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素
            while len(max_ids_dup) > 0:
                dup_dic = {}
                for u_id in max_ids_dup:
                    dup_dic[u_id] = np.arange(len(max_ids))[max_ids==u_id].tolist()
                for rp_id_dup in dup_dic.keys():
                    del dup_dic[rp_id_dup][np.argmax(sim_matrix[dup_dic[rp_id_dup]],axis=0)[rp_id_dup]]
                    sim_matrix[dup_dic[rp_id_dup],[rp_id_dup]] = -1.0 # 重複している要素の類似度を0に
                max_ids = np.argmax(sim_matrix, axis=1)
                (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
                max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素
            return [rc_ids,[rp_ids[max_id] for max_id in max_ids]]

    elif len(rc_ids) > len(rp_ids): # Faceが増える
        sim_matrix[sim_matrix < 0] = -1
        max_ids = np.argmax(sim_matrix, axis=1)
        # print('max_ids',max_ids)
        (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
        max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素
        sim_arr = np.arange(len(sim_matrix))
        del_lis = []
        while len(max_ids_dup) > 0:
            # print()
            # print('sim_matrix')
            # print(sim_matrix)
            # print('max_ids_dup',max_ids_dup)

            dup_dic = {}
            for u_id in max_ids_dup:
                dup_dic[u_id] = np.arange(len(max_ids))[max_ids==u_id].tolist()
            # print('dup_dic',dup_dic)
            for rp_id_dup in dup_dic.keys():
                del dup_dic[rp_id_dup][np.argmax(sim_matrix[dup_dic[rp_id_dup]],axis=0)[rp_id_dup]]
                sim_matrix[dup_dic[rp_id_dup],[rp_id_dup]] = -1.0 # 重複している要素の類似度を0に

            # for del_idx in sim_arr[np.sum(sim_matrix,axis=1) <= -1.0]:
            #     sim_matrix = np.delete(sim_matrix,del_idx,0)
            #     del_lis.append(del_idx)
            # print('sim_arr',sim_arr)
            # print('dup_dic',dup_dic)
            # print('deldel',sim_arr[np.sum(sim_matrix,axis=1) <= -1.0])
            del_idxs = sim_arr[np.sum(sim_matrix,axis=1) <= -1.0]
            del_lis.extend(del_idxs.tolist()) # records_currentの元のレコード

            # print('sim_matrix')
            # print(sim_matrix)
            # print('del_idxs',del_idxs)
            del_matrix_idxs = np.arange(len(sim_matrix))[np.sum(sim_matrix,axis=1) <= -1.0]
            sim_matrix = np.delete(sim_matrix,del_matrix_idxs,0)
            sim_arr = np.delete(sim_arr,del_matrix_idxs,0)

            max_ids = np.argmax(sim_matrix, axis=1)
            (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
            max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素
            # print('max_ids',max_ids)
            # print('max_ids_dup',max_ids_dup)
        if len(max_ids) != 0:
            for ex_id in sorted(del_lis):
                max_ids = np.insert(max_ids, ex_id, max(max_ids)+1)
                rp_ids.append(0)
            return [rc_ids,[rp_ids[max_id] for max_id in max_ids]]
        else:
            for i,rc_id in enumerate(rc_ids):
                rp_ids.append(0)
            return [rc_ids,rp_ids]


    # elif len(rc_ids) < len(rp_ids): # Faceが減る
    elif len(rc_ids) < len(rp_ids):
        max_ids = np.argmax(sim_matrix, axis=1)
        (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
        max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素
        while len(max_ids_dup) > 0:
            dup_dic = {}
            for u_id in max_ids_dup:
                dup_dic[u_id] = np.arange(len(max_ids))[max_ids==u_id].tolist()
            for rp_id_dup in dup_dic.keys():
                del dup_dic[rp_id_dup][np.argmax(sim_matrix[dup_dic[rp_id_dup]],axis=0)[rp_id_dup]]
                sim_matrix[dup_dic[rp_id_dup],[rp_id_dup]] = -1.0 # 重複している要素の類似度を0に
            max_ids = np.argmax(sim_matrix, axis=1)
            (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
            max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素
        return [rc_ids,[rp_ids[max_id] for max_id in max_ids]]

# return 重複した値:[重複した値のindex]
def dup(list):
    dup = set([x for x in list if list.count(x) > 1])
    output = {}
    for d in dup:
        output[d] = [i for i,l in enumerate(list) if l == d]
    return output

def CalcBox(img,bbox):
    [left,top,right,bottom] = [int(v) for v in bbox]
    wi = int(abs(right-left)*0.8)
    he = int(abs(bottom-top)/2)
    top = (top-he) if (top-he)>0 else 0
    bottom = (bottom+he) if (bottom+he)<(img.shape[0]) else img.shape[0]
    left = (left-wi) if (left-wi)>0 else 0
    right = (right+wi) if (right+wi)<(img.shape[1]) else img.shape[1]

    return [left,top,right,bottom]

def seconds2time(s):
    hours, remainder = divmod(s, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))


# 絶対座標系からローカル座標系に変換
def ConvertLocalCoordinate(row):
    R_GtoL, R_LtoG = RotationMatrix(row) # 絶対座標系からローカル座標系に変換する行列を取得
    local_row = []
    #print(np.arange(0,int(row.shape[0])))
    for idx in np.arange(0,int(row.shape[0]/3)):
        local_row.extend(np.sum(R_GtoL*np.append(GetCoordinate(row, idx),1),axis=1)[0:3])
    return np.array(local_row)

def GetCoordinate(row, index):
    return np.array([row[0+3*index], row[1+3*index], row[2+3*index]])

def RotationMatrix(row):
    origin = (GetCoordinate(row,93) + GetCoordinate(row,323))/2 #原点をface_meshランドマークの37番と267番の中点とする
    
    px = GetCoordinate(row,93) - GetCoordinate(row,323) # x軸はface_meshランドマークの37番から267番方向に
    
    py = GetCoordinate(row,10) - origin # y軸はoriginからランドマークの0番方向に
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


def scatter3D(arr):
    x = [arr[i] for i in np.arange(0, len(arr), 3)]
    y = [arr[i] for i in np.arange(1, len(arr), 3)]
    z = [arr[i] for i in np.arange(2, len(arr), 3)]

    fig = plt.figure()
    ax = fig.add_subplot(projection='3d')
    # 軸ラベルの設定
    ax.set_xlabel("X-axis")
    ax.set_ylabel("Y-axis")
    ax.set_zlabel("Z-axis")

    ax.scatter(x, y, z, color='purple')
    plt.show()

def scatter2D(arr):
    x = [arr[i] for i in np.arange(0, len(arr), 3)]
    y = [arr[i] for i in np.arange(1, len(arr), 3)]
    z = [arr[i] for i in np.arange(2, len(arr), 3)]

    fig = plt.figure()
    ax = fig.add_subplot()
    ax.set_xlabel("X-axis")
    ax.set_ylabel("Y-axis")
    ax.scatter(x, y, color='purple')
    ax.grid(True)
    plt.show()

def CalcDistance(landmarks1, landmarks2):
    dis_arrs = np.array([])
    dis_arg_arrs = np.array([], dtype=np.int64)
    for landmark1 in landmarks1:
        dis_arr = np.array([np.linalg.norm(landmark1-landmark2) for landmark2 in landmarks2])
        dis_arrs = np.append(dis_arrs,np.min(dis_arr))
        dis_arg_arrs = np.append(dis_arg_arrs, np.argmin(dis_arr))
    return (dis_arrs, dis_arg_arrs)