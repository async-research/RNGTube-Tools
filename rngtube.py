#!/usr/bin/env python3
"""
    Surf YouTube for videos, add them to your Database. 
"""
from os import getenv
from dotenv import load_dotenv
from youtube import YouTubeAPI
import databaseHelper as db
import random, time, datetime, argparse, mysql.connector,googleapiclient.errors, background_gen,textwrap

PADDING = 100


def surf_youtube(conn,row_cnt,table_name):
    queries = ""
    success = 0 
    requests_max = 1000
    yt = YouTubeAPI(getenv("YTAPIKEY"))
    cnt = db.get_count(conn, table_name)
    words = [word.strip() for word in open('wordLists/discordServerWrds_memes.txt','r').readlines()]
    aMonthago = datetime.datetime.now() - datetime.timedelta(hours=720, minutes=0)
    print("SEARCHING".center(PADDING,"%"))
    print()
    for i in range(0,requests_max): 
        q= random.choice(words) + "|" + random.choice(words) + "|" + random.choice(words)  + "|" + \
           random.choice(words) + "|" + random.choice(words) + "|" + random.choice(words) 
        queries += q + " "
        print(q.center(PADDING," "))
        
        try:
            df = yt.search(q,None,max_count=50)

        except Exception as err:
            print()
            print("".center(PADDING,"%"))
            report= ""
            report += "SUMMARY".center(PADDING, "*") + "\n"
            report += f"Successfully added ({str(success)}) new records to {table_name}".center(PADDING, " ") + "\n"
            report += f"Record Total: {str(row_cnt+success)}\n".center(PADDING) + "\n"
            report += "".center(PADDING,"*")
            print(report)
            print("END MESSAGE".center(PADDING,"!"))
            twrap = textwrap.TextWrapper(width = PADDING-20)
            info = twrap.wrap(str(err))
            print()
            for line in info:
                print(line.center(PADDING, " "))
            print()
            print("".center(PADDING,"!"))
            background_gen.main()
            print("DONE".center(PADDING," "))
            break

        else:
            for i in range(0,len(df.index)):
                if db.select_record(conn,df.loc[i,'videoID']) == []:
                    success += db.insert_record(conn,df,i)
  
def print_head(database_name, table_name):
    logo  = open('logo.txt','r').read()
    report =''
    report = "".center(PADDING,"=") + "\n"
    report += f'Database: {database_name}'.center(PADDING," ") + "\n"
    report += f'  Table: {table_name}'.center(PADDING," ") + "\n"
    print(logo)
    print(report)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sql", help="LOAD SQL WRAPPER", action="store_true")
    parser.add_argument("-r", "--record", help="SELECT A VIDEO RECORD", action="store_true")
    parser.add_argument("-t", "--trim", help="Trim Table", action="store_true")
    args = parser.parse_args()

    #DB STARTUP
    load_dotenv()
    usr = getenv("DBUSERNAME")
    usr_pwd = getenv("DBPASSWORD")
    conn = mysql.connector.connect(host="localhost",user=usr,password=usr_pwd)
    conn, database_name = db.initialize_database(conn,usr,usr_pwd)
    row_cnt, table_name = db.initialize_table(conn)
    print_head(database_name,table_name)
 
    if args.sql:
        while True:
            try:
                sql = input('sql:> ')
            except EOFError as e:
                print(e)
            else:
                if sql.lower() == 'exit':
                    break
                else:
                    db.python_to_sql(conn, sql)

    elif args.record:
        print(db.select_record(conn, input('Enter VIDEOID: '), table_name='videos'))
       
    elif args.trim:
        db.trim_table(conn,table_name)
    else:
        surf_youtube(conn,row_cnt,table_name)
       

if __name__ == '__main__':
    main()
