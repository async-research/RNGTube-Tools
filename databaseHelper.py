"""
    RNGTUBE DATABASE HELPER FUNCTIONS
"""
import mysql.connector, random

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
    query = "CREATE TABLE IF NOT EXISTS " + table_name + """(videoID VARCHAR(255), 
             title VARCHAR(255), description VARCHAR(255), channelTitle VARCHAR(255), 
             channelID VARCHAR(255), thumbnail VARCHAR(255), publishTime DATETIME, 
             category VARCHAR(255), viewCount INT, likeCount INT, dislikeCount INT, 
             commentCount INT, rngQuery VARCHAR(255)
            )
            """
    cursor.execute(query)
    row_cnt = get_count(conn, table_name)
    return row_cnt, table_name

def insert_record(conn,df,rng):
    query = """
        INSERT INTO videos (videoID, title, description, channelTitle, channelID, thumbnail, 
        publishTime, category, viewCount, likeCount, dislikeCount, commentCount, rngQuery) 
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        
      """
    cursor = conn.cursor()
    vals = (df.loc[rng,'videoID'], df.loc[rng,'title'], df.loc[rng,'description'],\
            df.loc[rng,'channelTitle'], df.loc[rng,'channelID'], df.loc[rng,'thumbnail'],\
            df.loc[rng,'publishTime'][:-1], df.loc[rng,'categoryID'], df.loc[rng,'viewCount'],\
            df.loc[rng,'likeCount'], df.loc[rng,'dislikeCount'], df.loc[rng,'commentCount'],\
            df.loc[rng,'query'])
    try:
        cursor.execute(query, vals)

    except mysql.connector.Error as err:
        print(f"\033[91m ERROR!: {err}\033[00m".center(80," "))
        return 0
    else:
        conn.commit()
        return 1

def remove_record(conn,table_name, videoID):
    query = "DELETE FROM " + table_name + " WHERE videoID= '" + videoID + "';"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))

def select_record(conn, videoID, table_name='videos'):
    query = "SELECT videoID, title, category, rngQuery FROM " + table_name +\
            " WHERE videoID= '" + videoID + "';"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
      
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
    print("***TRIMMING TABLE***".center(65," "))
    cnt = get_count(conn, table_name)
    print(f"CURRENT COUNT: {cnt}".center(65," "))

    query = "SELECT videoID FROM " + table_name + ";"
    cursor = conn.cursor()
    cursor.execute(query)
    result =[ record[0] for record in cursor.fetchall()]

    for i in range(0, round(len(result)*percent)):
        videoID = random.choice(result)
        remove_record(conn,table_name,videoID)
    cnt = get_count(conn, table_name)

    print(f"UPDATED COUNT: {cnt}".center(65," "))
    print("********************".center(65," "))
    