#!/usr/bin/env python3
"""
    Creates a collage of thumbnails with top rated videos from RNGTube Database.
"""
import os, glob, time, wget, mysql.connector
from PIL import Image, ImageFilter
from dotenv import load_dotenv
from os import getenv

def download_thumbnails(conn,dir_path,table_name='videos'):
    image_files = glob.glob(dir_path + "/images/*.jpg")
    width=320
    height=180
    query = "SELECT videoID FROM  videos ORDER BY likeCount DESC LIMIT 300"
    cur = conn.cursor()
    cur.execute(query);

    videoIDs = [videoID[0] for videoID in cur.fetchall()]
    cnt = 0
    for videoID in videoIDs:
        if cnt >= 100:
            break
        try:
            img_filename = wget.download("http://i.ytimg.com/vi/" + videoID + "/mqdefault.jpg", dir_path + "/images/" + videoID + ".jpg")

        except Exception as e:
            print("ERROR: IMAGE NOT DOWNLOADED, VideoID:", videoID)
            print(e)
        else:
            time.sleep(.2)
            print(img_filename)
            cnt += 1 

def create_background(dir_path, save_dir):
    image_files = glob.glob(dir_path + "/images/*.jpg")
    cnt=0
    for img_file in image_files:
        cnt += 1
    c_width = 320*5
    c_height=180*5
    image_files.sort(key=os.path.getmtime)
    main = Image.new("RGB",(c_width,c_height))
    width = 0
    height = 0
    cnt=0

    for img_file in image_files:
        cnt += 1
        img = Image.open(img_file)
        smll_img = img.resize((round(img.size[0]*0.5), round(img.size[1]*0.5)))
        smll_img = Image.eval(smll_img, lambda x: x/2) #pixle calcs
        main.paste(smll_img, (width,height))
        width += round(img.size[0]*0.5)
        if width >= c_width:
            width = 0 
            height += round(img.size[1]*0.5)
        if height >= c_height:
            break
    assert cnt == 100
    main.save(save_dir + '/toplikes.jpg')
    print('Saved collage to ' + save_dir + '/toplikes.jpg')

    for img_file in image_files:
        os.remove(img_file)
    print(dir_path + ' cleaned!')


def main():
    print("Generating RNGTUBE.COM BACKGROUND IMAGE".center(65,"+"))
    load_dotenv()
    dir_path = os.getcwd()
    save_dir = '/var/www/html/rngtube.com/images'
    usr = getenv("DBUSERNAME")
    usr_pwd = getenv("DBPASSWORD")
    database_name = 'RNGTube'
    conn = mysql.connector.connect(
            host="localhost",
            user=usr,
            password=usr_pwd,
            database=database_name
        )
    download_thumbnails(conn,dir_path)
    create_background(dir_path, save_dir)
    print("".center(65,"+"))
if __name__ == "__main__":
    main()