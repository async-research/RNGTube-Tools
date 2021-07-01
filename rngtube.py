#!/usr/bin/env python3
"""
    Surf YouTube for videos, add them to your Database. 
"""
import random
import mysql.connector
import argparse
import time
import textwrap
from dotenv import load_dotenv
from os import getenv
from youtube import YouTubeAPI 

def initialize_database(conn, usr, usr_pwd, database_name='RNGTube'):
    cursor = conn.cursor()
    query = "CREATE DATABASE IF NOT EXISTS " + database_name
    cursor.execute(query)
    conn = mysql.connector.connect(
            host="localhost",
            user=usr,
            password=usr_pwd,
            database=database_name
        )
    return conn, database_name

def initialize_table(conn, table_name='videos'):
    cursor = conn.cursor()
    query = "CREATE TABLE IF NOT EXISTS " + table_name + """(videoID VARCHAR(255), title VARCHAR(255), description VARCHAR(255),
            channelTitle VARCHAR(255), channelID VARCHAR(255), thumbnail VARCHAR(255), publishTime DATETIME, 
            category VARCHAR(255), viewCount INT, likeCount INT, dislikeCount INT, commentCount INT, rngQuery VARCHAR(255)
            )
            """
    cursor.execute(query)
    row_cnt = get_count(conn, table_name)
    return row_cnt, table_name

def insert_record(conn,df,rng):
    query = """
        INSERT INTO videos (videoID, title, description, channelTitle, channelID, thumbnail, publishTime, 
        category, viewCount, likeCount, dislikeCount, commentCount, rngQuery) 
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        
      """
    cursor = conn.cursor()
    vals = (df.loc[rng,'videoID'],df.loc[rng,'title'],df.loc[rng,'description'],df.loc[rng,'channelTitle'],df.loc[rng,'channelID'],\
           df.loc[rng,'thumbnail'],df.loc[rng,'publishTime'][:-1],df.loc[rng,'categoryID'],df.loc[rng,'viewCount'],df.loc[rng,'likeCount'],\
           df.loc[rng,'dislikeCount'],df.loc[rng,'commentCount'],df.loc[rng,'query'])
    cursor.execute(query, vals)
    conn.commit()

def remove_record(conn,table_name, videoID):
    query = "DELETE FROM " + table_name + " WHERE videoID= '" + videoID + "';"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        #return cursor.fetchall()

    except mysql.connector.Error as err:
         print("Something went wrong: {}".format(err))



def select_record(conn, videoID, table_name='videos'):
    query = "SELECT videoID, title, category, rngQuery FROM " + table_name + " WHERE videoID= '" + videoID + "';"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
        #for r in cursor.fetchall():
         #   print(r)

    except mysql.connector.Error as err:
         print("Something went wrong: {}".format(err))

def clear_table(conn, table_name='videos'):
    query = 'DROP TABLE ' +  table_name
    cursor = conn.cursor()
    cursor.execute(query)
    initialize_table(conn, table_name)
    print('**TABLE ' + table_name + ' INITALIZED**')

def python_to_sql(conn, cmd):
    try:
        cursor = conn.cursor()
        cursor.execute(cmd)
        for r in cursor.fetchall():
            print(r)

    except mysql.connector.Error as err:
         print("Something went wrong: {}".format(err))
   
def get_count(conn, table_name):
    cursor = conn.cursor()
    query = "SELECT COUNT(*) FROM " + table_name
    cursor.execute(query)
    return cursor.fetchall()[0][0]

def trim_table(conn, table_name='videos', percent=0.20):
    print("***TRIMMING TABLE***")
    cnt = get_count(conn, table_name)
    print("CURRENT COUNT: ", cnt)
    query = "SELECT videoID FROM " + table_name + ";"
    cursor = conn.cursor()
    cursor.execute(query)
    result =[ record[0] for record in cursor.fetchall()]
    for i in range(0, round(len(result)*percent)):
        videoID = random.choice(result)
        remove_record(conn,table_name,videoID)
    cnt = get_count(conn, table_name)
    print("UPDATED COUNT: ", cnt)
    print("********************")
    
def surf_youtube(conn,row_cnt,table_name):
    cnt = get_count(conn, table_name)
    # if cnt >= 9000:
    #     trim_table(conn,table_name)
    api_key = getenv("YTAPIKEY")
    yt = YouTubeAPI(api_key)
    words = [word.strip() for word in open('wordLists/discordServerWrds_memes.txt','r').readlines()]
    queries = ""
    success = 0 
    requests_max = 1000
  
    for i in range(0,requests_max):
        word = random.choice(words)  
        queries += word + " "
        print("             Word=> "+word, end="\r")
        
        try:
            df = yt.search(word,None,max_count=50)
        except:
            print("---------------------------RECEIPT-----------------------------")
            print('Successfully added (' + str(success) + ') new records to ' + table_name + '.')
            print('Record Total: ' + str(row_cnt+success))
            print("---------------------------------------------------------------")
            wrapper = textwrap.TextWrapper(width=50)
            print(wrapper.fill(text=queries))
            raise 
        else:
            for i in range(0,len(df.index)):
                try:
                    if select_record(conn,df.loc[i,'videoID']) == []:
                        insert_record(conn,df,i)
                except:
                    print("TEMP FIX, DATABASE ISSUE: Incorrect string value: '\xE0\xB0\x85\xE0\xB0\xA4...' for column 'title' at row 1")
                else:
                    success += 1
                    #time.sleep(.1)

    
def print_head(database_name, table_name):
    logo  = open('logo.txt','r').read()
    print(logo)
    report =''
    report += '===========| Database: ' + database_name + ' | ' + 'Table: '  + table_name + ' |=================\n' 
    print(report)

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--admin", help="LOAD ADMIN CONTROLLER", action="store_true")
    parser.add_argument("-t", "--trim", help="Trim Table", action="store_true")
    args = parser.parse_args()
    usr = getenv("DBUSERNAME")
    usr_pwd = getenv("DBPASSWORD")
    conn = mysql.connector.connect(host="localhost",user=usr,password=usr_pwd)
    conn, database_name = initialize_database(conn,usr,usr_pwd)
    row_cnt, table_name = initialize_table(conn)
    print_head(database_name,table_name)

    if args.admin:
        while True:
            try:
                cmd = input('admin:> ')
            except EOFError as e:
                print(e)

            if cmd == 'surf':
                surf_youtube(conn,row_cnt,table_name)

            elif cmd == 'select':
                print(select_record(conn, input('Enter ID: '), table_name='videos'))

            elif cmd == 'sql':
                while True:
                    try:
                        sql = input('sql:> ')
                    except EOFError as e:
                        print(e)
                    else:
                        if sql.lower() == 'exit':
                            break
                        else:
                            python_to_sql(conn, sql)

            elif cmd == 'exit':
                conn.close()
                print('GoodBye!')
                break
       
    elif args.trim:
        trim_table(conn,table_name)
    else:
        surf_youtube(conn,row_cnt,table_name)


if __name__ == '__main__':
    main()
