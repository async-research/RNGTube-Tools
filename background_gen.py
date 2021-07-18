#!/usr/bin/env python3
"""
    Creates a collage of thumbnails with top rated videos from RNGTube Database.
"""
import os, glob, time, mysql.connector, requests
from PIL import Image, ImageFilter
from dotenv import load_dotenv
from os import getenv
from tqdm import tqdm

PADDING = 100 


def download_file(url,addendum="", path="", pb=False):
    file_name = url.split("/")[-1]
    if addendum != "":
        file_name = file_name + "_" + addendum
    response = requests.get(url, stream=True)
    with open(f"{path}{file_name}","wb") as f:
        with tqdm(
            unit="B", unit_scale=True, desc=file_name, ncols=PADDING, disable=pb, 
            total=int(response.headers.get("content-length",0))
            ) as pbar:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
                    pbar.update(len(chunk))

def download_thumbnails(conn,dir_path,table_name='videos'):
    width=320
    height=180
    query = "SELECT videoID FROM  videos ORDER BY likeCount DESC LIMIT 256"
    cur = conn.cursor()
    cur.execute(query);
    videoIDs = [videoID[0] for videoID in cur.fetchall()]
   
    for i in tqdm(range(len(videoIDs)), ncols=PADDING):
        try:
            url = f"http://i.ytimg.com/vi/{videoIDs[i]}/mqdefault.jpg"
            download_file(url, videoIDs[i], path=dir_path+"/images/", pb=True)
            
        except Exception as e:
            print(f"\033[91m ERROR!: {videoIDs[i]}: {e}\033[00m".center(80," "))
          
        else:
            time.sleep(.2)
      
    

def create_background(dir_path, save_dir):
    image_files = glob.glob(dir_path + "/images/*.jpg")
    c_width = 320*5
    c_height=180*5
    image_files.sort(key=os.path.getmtime)
    main = Image.new("RGB",(c_width,c_height))
    width = 0
    height = 0
 
    for img_file in image_files:
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

    main.save(save_dir + '/toplikes.jpg')
    print()
    print(('Saved collage to ' + save_dir + '/toplikes.jpg').center(PADDING, " "))

    for img_file in image_files:
        os.remove(img_file)
    print((dir_path + ' cleaned!').center(PADDING, " "))


def main():
    print("Generating-RNGTUBE.COM-BACKGROUND-IMAGE".center(PADDING, "@"))
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
    print("".center(PADDING,"@"))
if __name__ == "__main__":
    main()