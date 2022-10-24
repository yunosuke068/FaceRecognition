import subprocess
import sys
import glob
import datetime
import os
import pandas as pd
import argparse

path = 'movie/movie_list.csv'
def addUrl(urls):
	df = pd.DataFrame(columns=["ID","URL","Append","DL"])
	if not glob.glob("movie"):
		os.mkdir("movie")
	if glob.glob(path):
		df = pd.read_csv(path)

	for input_url in urls:
		url_id = input_url.split("watch?v=")[-1]
		if not url_id in df["ID"].values:
			df = df.append({"ID":url_id,
							"URL":"https://www.youtube.com/watch?v=" +url_id,
							"Append":datetime.datetime.now(),
							"DL":""},ignore_index=True)
	df.to_csv(path, index=False)

def ytDL():
	if glob.glob(path):
		print('path',path)
		df = pd.read_csv(path)
		print(df)
		for idx in df.index:
			print(df["DL"][idx])
			if not df["DL"][idx]==df["DL"][idx]:
				print(df["ID"][idx])
				command = ["yt-dlp","-f","mp4","-o","movie/%(id)s.%(ext)s",df["URL"][idx]]
				proc = subprocess.Popen(command)
				proc.communicate()
				df["DL"][idx] = datetime.datetime.now()
		df.to_csv(path, index=False)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='youtubeのダウンロード用プログラム')

	parser.add_argument('-list', help='DLするyoutubeのurlリスト表示', action='store_true')
	parser.add_argument('-dl', help='リストのURLをDL', action='store_true')
	parser.add_argument('-add', type=str, help="DLするyoutubeのurlをリストに追加", nargs="*")
	args = parser.parse_args()    # 4. 引数を解析

	if not args.add == None:
		addUrl(args.add)

	if args.dl:
		ytDL()

	if args.list:
		proc = subprocess.Popen(["column","-s,","-t",path])
		proc.communicate()
