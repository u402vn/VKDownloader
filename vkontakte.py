import json
from datetime import datetime
from vk_auth import auth_token, DatabaseConnectionString
import psycopg2
from vkcommon import getJsonValue, download_and_save_user, load_url_as_json, save_group_member
import subprocess
import sys
import time
import vkfriends

limit_post_count = 99
limit_comments_count = 99
limit_likes_count = 99
limit_members_count = 99



 # {'error_code': 5, 'error_msg': 'User authorization failed: access_token has expired.'}
 # данные приходят в формате json
def check_for_errors(datas, error_info) -> bool:
    if not "error" in datas:
        return True
    # декомпозируем код и текст ошибки
    error_info.append(datas["error"]["error_code"])
    error_info.append(datas["error"]["error_msg"])
    return False



def check_for_errors_with_exception(datas):
    error_info = []
    if not check_for_errors(datas, error_info):
        raise Exception(f"Code: {error_info[0]}, Message: '{error_info[1]}'")



def save_like(cur, vk_user_id, vk_owner_id, vk_post_id, vk_comment_id):
    if vk_comment_id:
        cur.execute("""SELECT 1 FROM likes WHERE vk_user_id = %s AND vk_owner_id = %s AND vk_post_id IS NULL AND vk_comment_id = %s""", 
                    (vk_user_id, vk_owner_id, vk_comment_id) )
    else:
                cur.execute("""SELECT 1 FROM likes WHERE vk_user_id = %s AND vk_owner_id = %s AND vk_post_id = %s AND vk_comment_id IS NULL""", 
                    (vk_user_id, vk_owner_id, vk_post_id) )
    if cur.rowcount > 0:
        return
    download_and_save_user(cur, auth_token, vk_user_id)
    cur.execute("""INSERT INTO likes (vk_user_id, vk_owner_id, vk_post_id, vk_comment_id) VALUES (%s, %s, %s, %s)""",
                (vk_user_id, vk_owner_id, vk_post_id, vk_comment_id) )



def download_and_save_comment_likes(cur, vk_owner_id, vk_comment_id):
    print(f"+ Загрузка лайков к комментарию {vk_comment_id}")
    offset = 0;
    while True:
        time.sleep(1)
        url = f"https://api.vk.com/method/likes.getList?type=comment&owner_id={vk_owner_id}&item_id={vk_comment_id}&extended=1&count={limit_likes_count}&offset={offset}&access_token={auth_token}&v=5.199"

        src = load_url_as_json(url)
        like_json_data_collection = getJsonValue(src, 'response/items', None)
        for like_json_data in like_json_data_collection:
            vk_user_id = getJsonValue(like_json_data, "id", 0)
            sender_type = getJsonValue(like_json_data, "type")
            if sender_type == "profile":
                save_like(cur, vk_user_id, vk_owner_id, None, vk_comment_id)

        loadedLikesCount = len(like_json_data_collection)
        if loadedLikesCount < limit_likes_count:
            break
        offset += limit_likes_count



def download_and_save_post_likes(cur, vk_owner_id, vk_post_id):
    print(f"+ Загрузка лайков к посту {vk_post_id}")
    offset = 0;
    while True:
        url = f"https://api.vk.com/method/likes.getList?type=post&owner_id={vk_owner_id}&item_id={vk_post_id}&extended=1&count={limit_likes_count}&offset={offset}&access_token={auth_token}&v=5.199"

        src = load_url_as_json(url)
        like_json_data_collection = getJsonValue(src, 'response/items', None)
        for like_json_data in like_json_data_collection:
            vk_user_id = getJsonValue(like_json_data, "id", 0)
            sender_type = getJsonValue(like_json_data, "type")
            if sender_type == "profile":
                save_like(cur, vk_user_id, vk_owner_id, vk_post_id, None)

        loadedLikesCount = len(like_json_data_collection)
        if loadedLikesCount < limit_likes_count:
            break
        offset += limit_likes_count



def download_and_save_comments(cur, communityId, vk_owner_id, post_vk_id, comment_id: int = 0):
    offset = 0;
    while True:
        if comment_id > 0:
            url = f"https://api.vk.com/method/wall.getComments?owner_id={vk_owner_id}&post_id={post_vk_id}&access_token={auth_token}&sort=asc&comment_id={comment_id}&count={limit_comments_count}&offset={offset}&need_likes=1&v=5.199"
            vk_parent_id = comment_id
        else:
            url = f"https://api.vk.com/method/wall.getComments?owner_id={vk_owner_id}&post_id={post_vk_id}&access_token={auth_token}&sort=asc&count={limit_comments_count}&offset={offset}&need_likes=1&v=5.199"
            vk_parent_id = None

        src = load_url_as_json(url)
        comment_json_data_collection = getJsonValue(src, 'response/items', None)

#        check_for_errors_with_exception(src)
        for comment_json_data in comment_json_data_collection:
            comment_vk_id = getJsonValue(comment_json_data, "id")
            comment_post_id = post_vk_id
            comment_community_id = communityId
            thread_count = getJsonValue(comment_json_data, "thread/count", 0)
            comment_vk_from_id = getJsonValue(comment_json_data, "from_id", None)
            comment_date = datetime.fromtimestamp(comment_json_data["date"])
            likes_count = getJsonValue(comment_json_data, "likes/count", 0)

            #todo check if exists. 2. Update if was changed
   
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

            download_and_save_user(cur, auth_token, comment_vk_from_id)

            if likes_count > 0:
                download_and_save_comment_likes(cur, vk_owner_id, comment_vk_id)
            if thread_count > 0:
                download_and_save_comments(cur, communityId, vk_owner_id, post_vk_id, comment_vk_id)

        loadedCommentsCount = len(comment_json_data_collection)
        if loadedCommentsCount < limit_comments_count:
            break
        offset += limit_comments_count


    
def download_and_save_posts(conn, community_id, community_name, offset):
    url = f"https://api.vk.com/method/wall.get?domain={community_name}&offset={offset}&count={limit_post_count}&access_token={auth_token}&v=5.199"
    src = load_url_as_json(url)

    error_info = []
    if not check_for_errors(src, error_info):
        raise Exception(f"Code: {error_info[0]}, Message: '{error_info[1]}'")

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
            download_and_save_post_likes(cur, post_vk_owner_id, post_vk_id)

        comments_count = getJsonValue(post_json_data, "comments/count", 0)
        if comments_count > 0:
            print(f"+ Загрузка комментариев к посту")
            download_and_save_comments(cur, community_id, post_vk_owner_id, post_vk_id)
        conn.commit()
    return len(post_json_data_collection), earliest_post_date



def download_and_save_community_members(conn, vk_group_id):    
    cur = conn.cursor()
    print(f"+ Загрузка подписчиков группы {vk_group_id}")
    offset = 0;
    while True:
        url = f"https://api.vk.com/method/groups.getMembers?group_id={-vk_group_id}&offset={offset}&count={limit_members_count}&access_token={auth_token}&v=5.199"
        src = load_url_as_json(url)

        members_json_data_collection = getJsonValue(src, 'response/items', None)
        for vk_user_id in members_json_data_collection:

            download_and_save_user(cur, auth_token, vk_user_id)
            save_group_member(cur, vk_user_id, vk_group_id)

            #cur.execute("""SELECT 1 FROM community_members WHERE vk_user_id = %s AND vk_owner_id = %s""", (vk_user_id, vk_group_id) )
            #if cur.rowcount == 0:
            #    download_and_save_user(cur, auth_token, vk_user_id)
            #    cur.execute("""INSERT INTO community_members (vk_user_id, vk_owner_id) VALUES (%s, %s)""", (vk_user_id, vk_group_id) )
        
        conn.commit()
        loadedMembersCount = len(members_json_data_collection)
        if loadedMembersCount < limit_members_count:
            break
        offset += limit_members_count



def download_and_save_community(conn, community_name):
    print(f"Начало загрузки паблика {community_name}")

    url = f"https://api.vk.com/method/groups.getById?group_id={community_name}&access_token={auth_token}&v=5.199"
    src = load_url_as_json(url)

    error_info = []
    if not check_for_errors(src, error_info):
        print(f"Code: {error_info[0]}, Message: '{error_info[1]}'")
        return

    group_json_data_collection = getJsonValue(src, 'response/groups', None)
    if not group_json_data_collection or (len(group_json_data_collection) == 0):
        return # не нашли ?

    group_json_data = group_json_data_collection[0]
    description = getJsonValue(group_json_data, 'name')
    group_id = - getJsonValue(group_json_data, 'id', 0)

    download_and_save_community_members(conn, group_id)
    conn.commit()
    
    cur = conn.cursor()
    cur.execute("SELECT id, top_post_date FROM communities WHERE name = %s", (community_name,) )
    community_id, top_post_date = cur.fetchone()
    if not top_post_date:
        top_post_date = datetime(1900, 1, 1, 1, 1, 1)
    cur.execute("SELECT COUNT(*) FROM posts p WHERE p.vk_owner_id = %s AND p.date > %s ", (group_id, top_post_date) )
    offset, = cur.fetchone()

    offset = offset - 100; # перестраховываемся, начиная качать не с самого конца
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

    cur.execute("SELECT max(date), count(*) FROM posts WHERE vk_owner_id = %s", (group_id,) )
    top_post_date, post_count = cur.fetchone()

    last_update = datetime.now()
    cur.execute(f'UPDATE communities SET last_update = %s, top_post_date =  %s, post_count = %s, description = %s, vk_id = %s WHERE name = %s', 
                (last_update, top_post_date, post_count, description, group_id, community_name) )
    conn.commit()
    print(f"Загрузка паблика {community_name} завершена")



def download_and_save_communities(conn):
    cur = conn.cursor()    
    cur.execute("SELECT name FROM communities WHERE ObservationInterval > 0 ORDER BY last_update ASC NULLS FIRST, id DESC")

    rows = cur.fetchall()
    for community_name, in rows:
        download_and_save_community(conn, community_name)





def main():
    #vkfriends.main()

    command = f'"{sys.executable}" vkfriends.py'
    subprocess.Popen(command, creationflags = subprocess.CREATE_NEW_CONSOLE)

    conn = psycopg2.connect(DatabaseConnectionString)
    download_and_save_communities(conn)



if __name__ == '__main__':
	main()