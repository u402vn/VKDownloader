﻿import argparse
import json
from datetime import datetime
from vk_auth import VKTokens, DatabaseConnectionString
import psycopg2
from vkcommon import set_access_token, getJsonValue, download_and_save_users, load_url_as_json, save_group_member, save_update_group, needPause
import subprocess
import sys
import time
import vkfriends
import ctypes

limit_post_count = 99
limit_comments_count = 99
limit_likes_count = 99
limit_members_count = 99

instanceIndex = 0
instanceCount = len(VKTokens)



def load_users_in_pause(conn):    
    while True:  # просто задержка, чтобы не было бана по загрузке комментариев и лайков
        doPause = needPause()
        if not doPause:
            break
        vkfriends.download_all_friend_for_users_with_comments(conn, instanceIndex, instanceCount, 1)
        #vkfriends.download_users_from_db_with_leaks(conn, instanceIndex, instanceCount, 5)
        #vkfriends.download_all_friend_for_users_with_comments(conn, instanceIndex, instanceCount, 5)
        #vkfriends.download_all_friend_for_users_from_belarus_phones(conn, instanceIndex, instanceCount, 5)
        


def save_like(cur, vk_user_id, vk_owner_id, vk_post_id, vk_comment_id):
    if vk_comment_id:
        cur.execute("""SELECT 1 FROM likes WHERE vk_user_id = %s AND vk_owner_id = %s AND vk_post_id IS NULL AND vk_comment_id = %s""", 
                    (vk_user_id, vk_owner_id, vk_comment_id) )
    else:
                cur.execute("""SELECT 1 FROM likes WHERE vk_user_id = %s AND vk_owner_id = %s AND vk_post_id = %s AND vk_comment_id IS NULL""", 
                    (vk_user_id, vk_owner_id, vk_post_id) )
    if cur.rowcount > 0:
        return

    cur.execute("""INSERT INTO likes (vk_user_id, vk_owner_id, vk_post_id, vk_comment_id) VALUES (%s, %s, %s, %s)""",
                (vk_user_id, vk_owner_id, vk_post_id, vk_comment_id) )



def download_and_save_comment_likes(conn, vk_owner_id, vk_comment_id):
    print(f"+ Загрузка лайков к комментарию {vk_comment_id}")
    cur = conn.cursor()
    userIds = []
    offset = 0;
    while True:
        time.sleep(1)
        url = f"https://api.vk.com/method/likes.getList?type=comment&owner_id={vk_owner_id}&item_id={vk_comment_id}&extended=1&count={limit_likes_count}&offset={offset}"
        src = load_url_as_json(url)
        like_json_data_collection = getJsonValue(src, 'response/items', None)
        for like_json_data in like_json_data_collection:
            vk_user_id = getJsonValue(like_json_data, "id", 0)
            sender_type = getJsonValue(like_json_data, "type")
            if sender_type == "profile":
                save_like(cur, vk_user_id, vk_owner_id, None, vk_comment_id)
                userIds.append(vk_user_id)        
        
        conn.commit()
        loadedLikesCount = len(like_json_data_collection)
        if loadedLikesCount < limit_likes_count:
            break
        offset += limit_likes_count

    cur.close()
    download_and_save_users(conn, userIds)

def download_and_save_post_likes(conn, vk_owner_id, vk_post_id):
    print(f"+ Загрузка лайков к посту {vk_post_id}")
    cur = conn.cursor()
    userIds = []
    offset = 0
    while True:
        url = f"https://api.vk.com/method/likes.getList?type=post&owner_id={vk_owner_id}&item_id={vk_post_id}&extended=1&count={limit_likes_count}&offset={offset}"
        src = load_url_as_json(url)
        like_json_data_collection = getJsonValue(src, 'response/items', None)
        if not like_json_data_collection:
            return
        for like_json_data in like_json_data_collection:
            vk_user_id = getJsonValue(like_json_data, "id", 0)
            sender_type = getJsonValue(like_json_data, "type")
            if sender_type == "profile":
                save_like(cur, vk_user_id, vk_owner_id, vk_post_id, None)
                userIds.append(vk_user_id)

        conn.commit()
        loadedLikesCount = len(like_json_data_collection)
        if loadedLikesCount < limit_likes_count:
            break
        offset += limit_likes_count
    
    cur.close()
    download_and_save_users(conn, userIds)


def download_likes_for_stored_comments(conn, count = 1):
    cur = conn.cursor()
    cur.execute(f"""select p.vk_owner_id, p.vk_id from posts p where p.like_Load_date is null and -p.vk_owner_id % {instanceCount} = {instanceIndex} limit {count}""")
    postResultSet = cur.fetchall()
    for vk_owner_id, vk_post_id in postResultSet:
        download_and_save_post_likes(conn, vk_owner_id, vk_post_id)
        cur.execute(f"""select c.vk_id from comments c where c.vk_owner_id = %s and c.post_id = %s""",  (vk_owner_id, vk_post_id) )
        commentResultSet = cur.fetchall()
        for vk_comment_id, in commentResultSet:
            download_and_save_comment_likes(conn, vk_owner_id, vk_comment_id)

        cur.execute(f"""update posts set like_Load_date = %s where vk_owner_id = %s and vk_id = %s""", (datetime.now(), vk_owner_id, vk_post_id) )
        conn.commit()

    

def download_and_save_comments(conn, communityId, vk_owner_id, post_vk_id, comment_id: int = 0):
    cur = conn.cursor()
    commentUserIds = []
    offset = 0;
    while True:
        if comment_id > 0:
            url = f"https://api.vk.com/method/wall.getComments?owner_id={vk_owner_id}&post_id={post_vk_id}&sort=asc&comment_id={comment_id}&count={limit_comments_count}&offset={offset}&need_likes=1"
            vk_parent_id = comment_id
        else:
            url = f"https://api.vk.com/method/wall.getComments?owner_id={vk_owner_id}&post_id={post_vk_id}&sort=asc&count={limit_comments_count}&offset={offset}&need_likes=1"
            vk_parent_id = None

        src = load_url_as_json(url)
        comment_json_data_collection = getJsonValue(src, 'response/items', None)

        for comment_json_data in comment_json_data_collection:
            comment_vk_id = getJsonValue(comment_json_data, "id")
            comment_post_id = post_vk_id
            comment_community_id = communityId
            thread_count = getJsonValue(comment_json_data, "thread/count", 0)
            comment_vk_from_id = getJsonValue(comment_json_data, "from_id", None)
            comment_date = datetime.fromtimestamp(comment_json_data["date"])
            #likes_count = getJsonValue(comment_json_data, "likes/count", 0)

            commentUserIds.append(comment_vk_from_id)
   
            cur.execute("""SELECT 1 FROM comments WHERE vk_id = %s AND post_id = %s AND vk_owner_id = %s""", 
                        (comment_vk_id, comment_post_id, vk_owner_id) )
            if cur.rowcount == 0:
                comment_text = getJsonValue(comment_json_data, "text")
                comment_reply_to_user = getJsonValue(comment_json_data, "reply_to_user", None)
                comment_reply_to_comment = getJsonValue(comment_json_data, "reply_to_comment", None)

                cur.execute("""INSERT INTO comments (vk_id, vk_owner_id, vk_from_id, date, text, reply_to_user, reply_to_comment, post_id, community_id, vk_parent_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                            (comment_vk_id, vk_owner_id, comment_vk_from_id, comment_date, comment_text, comment_reply_to_user, comment_reply_to_comment, 
                             comment_post_id, comment_community_id, vk_parent_id) )
                print(f"\tЗагружен и добавлен в БД комментарий: {comment_date}")
            else:
                print(f"\tЗагружен комментарий (без сохранения): {comment_date}")            

            #if likes_count > 0:
            #    download_and_save_comment_likes(cur, vk_owner_id, comment_vk_id)
            if thread_count > 0:
                download_and_save_comments(conn, communityId, vk_owner_id, post_vk_id, comment_vk_id)

        conn.commit()
        loadedCommentsCount = len(comment_json_data_collection)
        if loadedCommentsCount < limit_comments_count:
            break
        offset += limit_comments_count

    cur.close()
    download_and_save_users(conn, commentUserIds)

    
def download_and_save_posts(conn, community_id, community_name, offset):
    url = f"https://api.vk.com/method/wall.get?domain={community_name}&offset={offset}&count={limit_post_count}"
    src = load_url_as_json(url)

    earliest_post_date = None

    cur = conn.cursor()
    post_json_data_collection = getJsonValue(src, 'response/items', None)

    for post_json_data in post_json_data_collection:
        post_vk_id = getJsonValue(post_json_data, "id")
        post_vk_owner_id = getJsonValue(post_json_data, "owner_id")
        post_date =  datetime.fromtimestamp(post_json_data["date"])

        if (not earliest_post_date) or (earliest_post_date > post_date):
            earliest_post_date = post_date

        cur.execute("""SELECT 1 FROM posts WHERE community_id=%s AND vk_id=%s AND vk_owner_id=%s""", (community_id, post_vk_id, post_vk_owner_id) )
        if cur.rowcount == 0:
            post_vk_from_id = getJsonValue(post_json_data, "from_id")
            post_vk_created_by = getJsonValue(post_json_data, "created_by", None)
            post_vk_signer_id = getJsonValue(post_json_data, "signer_id", None)
            post_text = getJsonValue(post_json_data, "text")            

            cur.execute("""INSERT INTO posts (community_id, vk_id, vk_owner_id, vk_from_id, vk_created_by, vk_signer_id, date, text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (community_id, post_vk_id, post_vk_owner_id, post_vk_from_id, post_vk_created_by, post_vk_signer_id, post_date, post_text) )

            print(f"Загружен и добавлен в БД пост {post_vk_id} для {community_name}: {post_vk_id} от {post_date}")
        else:
            print(f"Загружен пост (без сохранения) {post_vk_id} для {community_name}: {post_vk_id} от {post_date}")
        
        likes_count = getJsonValue(post_json_data, "likes/count", 0)     
        if likes_count > 0:
            download_and_save_post_likes(conn, post_vk_owner_id, post_vk_id)

        comments_count = getJsonValue(post_json_data, "comments/count", 0)
        if comments_count > 0:
            print(f"+ Загрузка комментариев к посту")
            download_and_save_comments(conn, community_id, post_vk_owner_id, post_vk_id)
        conn.commit()

        load_users_in_pause(conn)
    return len(post_json_data_collection), earliest_post_date



def download_and_save_community_members(conn, vk_group_id):    
    cur = conn.cursor()
    print(f"+ Загрузка подписчиков группы {vk_group_id}")
    offset = 0;
    while True:
        url = f"https://api.vk.com/method/groups.getMembers?group_id={-vk_group_id}&offset={offset}&count={limit_members_count}"
        src = load_url_as_json(url)
        members_json_data_collection = getJsonValue(src, 'response/items', None)
        #error_code: 15, error_msg: 'Access denied: group hide members'
        if not members_json_data_collection:
            return

        download_and_save_users(conn, members_json_data_collection)
        for vk_user_id in members_json_data_collection:            
            save_group_member(cur, vk_user_id, vk_group_id)
        
        conn.commit()
        loadedMembersCount = len(members_json_data_collection)
        if loadedMembersCount < limit_members_count:
            break
        offset += limit_members_count
        load_users_in_pause(conn)



def download_and_save_community(conn, community_name):
    print(f"Начало загрузки паблика {community_name}")

    url = f"https://api.vk.com/method/groups.getById?group_id={community_name}"
    src = load_url_as_json(url)

    group_json_data_collection = getJsonValue(src, 'response/groups', None)
    if not group_json_data_collection or (len(group_json_data_collection) == 0):
        return # не нашли ?

    group_json_data = group_json_data_collection[0]
    description = getJsonValue(group_json_data, 'name')
    group_id = - getJsonValue(group_json_data, 'id', 0)

    cur = conn.cursor()
    save_update_group(cur, group_id, community_name, description)
    
    cur = conn.cursor()
    cur.execute("SELECT id, top_post_date FROM communities WHERE name = %s", (community_name,) )
    community_id, top_post_date = cur.fetchone()
    if not top_post_date:
        top_post_date = datetime(1900, 1, 1, 1, 1, 1)
    cur.execute("SELECT COUNT(*) FROM posts p WHERE p.vk_owner_id = %s AND p.date > %s ", (group_id, top_post_date) )
    offset, = cur.fetchone()

    somePostLoaded = offset > 0
    if not somePostLoaded:
        download_and_save_community_members(conn, group_id)
        conn.commit()

    offset = offset - 10; # перестраховываемся, начиная качать не с самого конца
    if offset < 0:
        offset = 0
    while True:
        loadedPostsCount, earliestPostDate = download_and_save_posts(conn, community_id, community_name, offset)
        conn.commit()
        if loadedPostsCount < limit_post_count:
            break
        if earliestPostDate < top_post_date:
            break
        offset += limit_post_count
        
        #download_likes_for_stored_comments(conn, 1) #интервал вызова функций надо сделать расчетным. Но - потом

    cur.execute("SELECT max(date), count(*) FROM posts WHERE vk_owner_id = %s", (group_id,) )
    top_post_date, post_count = cur.fetchone()

    last_update = datetime.now()
    cur.execute(f'UPDATE communities SET last_update = %s, top_post_date =  %s, post_count = %s, description = %s, vk_id = %s WHERE name = %s', 
                (last_update, top_post_date, post_count, description, group_id, community_name) )
    conn.commit()

    if somePostLoaded:
        download_and_save_community_members(conn, group_id)
        conn.commit()

    print(f"Загрузка паблика {community_name} завершена")



def download_and_save_communities(conn):
    cur = conn.cursor()
    sql = f"""SELECT name FROM communities WHERE ObservationInterval > 0 AND -vk_id % {instanceCount} = {instanceIndex}
        ORDER BY last_update ASC NULLS FIRST, id DESC"""
    cur.execute(sql)

    rows = cur.fetchall()
    for community_name, in rows:
        download_and_save_community(conn, community_name)


def startDownload():
    access_token = VKTokens[instanceIndex]
    set_access_token(access_token)
    while True:
        connectionString = DatabaseConnectionString
        if not ('application_name' in connectionString):
            connectionString = f"""{connectionString} application_name='VKDownloder{instanceIndex}'"""
        conn = psycopg2.connect(connectionString)
        conn.autocommit = False
        download_and_save_communities(conn)
    #vkfriends.main()



def main():
    global instanceIndex
    parser = argparse.ArgumentParser()
    parser.add_argument('--instanceindex', default = 0, type = int)
    parameters, unknownParameters  = parser.parse_known_args(sys.argv[1:])
    instanceIndex = parameters.instanceindex    

    ctypes.windll.kernel32.SetConsoleTitleW(f"VK Downloder #{instanceIndex}")

    if instanceIndex == 0:
        for i in range(1, instanceCount):
            command = f'"{sys.executable}" vkontakte.py --instanceindex {i}'
            subprocess.Popen(command, creationflags = subprocess.CREATE_NEW_CONSOLE)

    startDownload()    



if __name__ == '__main__':
	main()