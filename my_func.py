import numpy as np

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
        (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
        max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素

        sim_arr = np.arange(len(sim_matrix))
        del_lis = []
        while len(max_ids_dup) > 0:
            dup_dic = {}
            for u_id in max_ids_dup:
                dup_dic[u_id] = np.arange(len(max_ids))[max_ids==u_id].tolist()
            for rp_id_dup in dup_dic.keys():
                del dup_dic[rp_id_dup][np.argmax(sim_matrix[dup_dic[rp_id_dup]],axis=0)[rp_id_dup]]
                sim_matrix[dup_dic[rp_id_dup],[rp_id_dup]] = -1.0 # 重複している要素の類似度を0に

            # for del_idx in sim_arr[np.sum(sim_matrix,axis=1) <= -1.0]:
            #     sim_matrix = np.delete(sim_matrix,del_idx,0)
            #     del_lis.append(del_idx)
            del_idxs = sim_arr[np.sum(sim_matrix,axis=1) <= -1.0]
            del_lis.extend(del_idxs.tolist())
            sim_matrix = np.delete(sim_matrix,del_idxs,0)
            sim_arr = np.delete(sim_arr,del_idxs,0)

            max_ids = np.argmax(sim_matrix, axis=1)
            (unique_ids, unique_counts) = np.unique(max_ids, return_counts=True) # max_idsの固有の要素と要素の数
            max_ids_dup = unique_ids[unique_counts > 1] # 重複している要素

        for ex_id in sorted(del_lis):
            max_ids = np.insert(max_ids, ex_id, max(max_ids)+1)
            rp_ids.append(0)

        return [rc_ids,[rp_ids[max_id] for max_id in max_ids]]

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
