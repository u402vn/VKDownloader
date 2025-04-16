﻿import json
from datetime import datetime
from vk_auth import auth_token
import psycopg2
import requests
import urllib3

urllib3.disable_warnings()

limit_post_count = 30
limit_comments_count = 50

user_base_fields = "id,first_name,last_name,deactivated,is_closed,can_access_closed,about,activities,bdate,blacklisted,blacklisted_by_me,bookscan_post,can_see_all_post,scan_see_au         dio,can_send_friend_request,can_write_private_message,career,city,common_count,connections,contacts,counters,country,crop_photo,domain,education,exports,,followers_count,friend_status,games,has_mobile,has_photo,home_town,interests,is_favorite,is_friend,is_hidden_from_feed,is_no_index"
user_optional_fields_L_R = "last_seen,lists,maiden_name,military,movies,music,nickname,occupation,online,personal,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_id,photo_max,photo_max_orig,quotes,relatives,relation"
user_optional_fields_S_W = "schools,screen_name,sex,site,status,timezone,trending,tv,universities,verified,wall_default,is_verified"
user_all_fields = f"{user_base_fields},{user_optional_fields_L_R},{user_optional_fields_S_W}"

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

def load_url_as_json(url: str) -> str:
    try:    
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        req = requests.get(url, headers=headers, verify=False)         
        return req.json()
    except Exception as e:
        return f'Невозможно получить данные для {url}. Текст ошибки: {repr(e)}'  

     
def __getValue(json, path: str, defaultValue = ''):
    keys = path.split('/')
    key1 = keys[0]
    key2 = keys[1] if len(keys) > 1 else None
    value = defaultValue
    if not key1 in json:
        pass
    elif not key2:
        value = json[key1]
    else:
        node = json[key1]
        if key2 in node:
            value = node[key2]				
    return value


def download_and_save_user(cur, vk_user_id):
    if vk_user_id <= 0:
        return
    
    cur.execute("""SELECT 1 FROM users WHERE vk_num_id=%s""", (vk_user_id, ) )
    if cur.rowcount > 0:
        return # В Базе уже есть такая запись

    url = f"https://api.vk.com/method/users.get?user_ids={vk_user_id}&fields={user_all_fields}&access_token={auth_token}&v=5.199"
    src = load_url_as_json(url)

    #check_for_errors_with_exception(datas)
    user_json_data = src["response"][0] 
    
    user_vk_num_str = __getValue(user_json_data, "domain")
    user_vk_num_id = __getValue(user_json_data, "id") # должен быть равен vk_user_id
    user_vk_country_name = __getValue(user_json_data, "country/title")
    user_vk_city_name = __getValue(user_json_data, "city/title")

    user_vk_sex = __getValue(user_json_data, "sex")
    if user_vk_sex == 1:
        user_vk_sex = 'Ж'
    elif user_vk_sex == 2:
        user_vk_sex = 'М'
    else:
        user_vk_sex = ''

    try:
        user_date_of_birth = __getValue(user_json_data, "bdate")
        user_date_of_birth = datetime.strptime(user_date_of_birth, "%d.%m.%Y")
    except:
        user_date_of_birth = None

    user_first_name = __getValue(user_json_data, "first_name")    
    user_last_name = __getValue(user_json_data, "last_name")
    user_middle_name = __getValue(user_json_data, "middle_name")
    user_nickname = __getValue(user_json_data, "nickname")
    user_maiden_name = __getValue(user_json_data, "maiden_name")
    user_is_hidden = __getValue(user_json_data, "is_closed")
    user_photo_max_orig = __getValue(user_json_data, "photo_max_orig")

    json_data = json.dumps(user_json_data)
    cur.execute("""INSERT INTO users 
        (first_name, last_name, middle_name, nickname, maiden_name, 
        vk_city_name, vk_country_name, date_of_birth, vk_num_id, vk_str_id, photo_url, vk_sex, is_hidden, json_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
        (user_first_name, user_last_name, user_middle_name, user_nickname, user_maiden_name,
            user_vk_city_name, user_vk_country_name, user_date_of_birth, user_vk_num_id, user_vk_num_str, 
            user_photo_max_orig, user_vk_sex, user_is_hidden, json_data) )


def download_and_save_comments(cur, communityId, vk_owner_id, post_vk_id, comment_id: int = 0):
    offset = 0;
    while True:
        if comment_id > 0:
            url = f"https://api.vk.com/method/wall.getComments?owner_id={vk_owner_id}&post_id={post_vk_id}&access_token={auth_token}&sort=asc&comment_id={comment_id}&count={limit_comments_count}&offset={offset}&v=5.199"
            vk_parent_id = comment_id
        else:
            url = f"https://api.vk.com/method/wall.getComments?owner_id={vk_owner_id}&post_id={post_vk_id}&access_token={auth_token}&sort=asc&count={limit_comments_count}&offset={offset}&v=5.199"
            vk_parent_id = None

        src = load_url_as_json(url)
        comment_json_data_collection = __getValue(src, 'response/items')

#        check_for_errors_with_exception(src)
        for comment_json_data in comment_json_data_collection:
            comment_vk_id = __getValue(comment_json_data, "id")
            comment_post_id = post_vk_id
            comment_community_id = communityId
            thread_count = __getValue(comment_json_data, "thread/count", 0)
            comment_vk_from_id = __getValue(comment_json_data, "from_id", None)
            comment_date = datetime.fromtimestamp(comment_json_data["date"])

            #todo check if exists. 2. Update if was changed
   
            cur.execute("""SELECT 1 FROM comments WHERE vk_id = %s AND post_id = %s AND vk_owner_id = %s""", 
                        (comment_vk_id, comment_post_id, vk_owner_id) )
            if cur.rowcount == 0:
                comment_text = __getValue(comment_json_data, "text")
                comment_reply_to_user = __getValue(comment_json_data, "reply_to_user", None)
                comment_reply_to_comment = __getValue(comment_json_data, "reply_to_comment", None)
                #comment_parents_stack = __getValue(comment, "parents_stack") #??? что это ???                           

                cur.execute("""INSERT INTO comments (vk_id, vk_owner_id, vk_from_id, date, text, reply_to_user, reply_to_comment, post_id, community_id, vk_parent_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                            (comment_vk_id, vk_owner_id, comment_vk_from_id, comment_date, comment_text, comment_reply_to_user, comment_reply_to_comment, 
                             comment_post_id, comment_community_id, vk_parent_id) )
                print(f"\tЗагружен и добавлен в БД комментарий: {comment_date}")
            else:
                print(f"\tЗагружен комментарий (без сохранения): {comment_date}")

            download_and_save_user(cur, comment_vk_from_id)

            if (thread_count > 0):
                download_and_save_comments(cur, communityId, vk_owner_id, post_vk_id, comment_vk_id)

        loadedCommentsCount = len(comment_json_data_collection)
        offset += limit_comments_count
        if loadedCommentsCount < limit_comments_count:
            return

    
def download_and_save_posts(conn, community_id, community_name, offset):
    cur = conn.cursor()

    url = f"https://api.vk.com/method/wall.get?domain={community_name}&offset={offset}&count={limit_post_count}&access_token={auth_token}&v=5.199"
    src = load_url_as_json(url)

    error_info = []
    if not check_for_errors(src, error_info):
        raise Exception(f"Code: {error_info[0]}, Message: '{error_info[1]}'")

    post_json_data_collection = __getValue(src, 'response/items')
    for post_json_data in post_json_data_collection:
        post_vk_id = __getValue(post_json_data, "id")
        post_vk_owner_id = __getValue(post_json_data, "owner_id")
        post_date =  datetime.fromtimestamp(post_json_data["date"])

        cur.execute("""SELECT 1 FROM posts WHERE community_id=%s AND vk_id=%s AND vk_owner_id=%s""", (community_id, post_vk_id, post_vk_owner_id) )
        if cur.rowcount == 0:
            post_vk_from_id = __getValue(post_json_data, "from_id")
            post_vk_created_by = __getValue(post_json_data, "created_by", None)
            post_vk_signer_id = __getValue(post_json_data, "signer_id", None)
            post_text = __getValue(post_json_data, "text")            

            cur.execute("""INSERT INTO posts (community_id, vk_id, vk_owner_id, vk_from_id, vk_created_by, vk_signer_id, date, text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (community_id, post_vk_id, post_vk_owner_id, post_vk_from_id, post_vk_created_by, post_vk_signer_id, post_date, post_text) )

            print(f"Загружен и добавлен в БД пост {post_vk_id} для {community_name}: {post_vk_id} от {post_date}")
        else:
            print(f"Загружен пост (без сохранения) {post_vk_id} для {community_name}: {post_vk_id} от {post_date}")
        
        comments_count = __getValue(post_json_data, "comments/count", 0)
        if comments_count > 0:
            print(f"+ Загрузка комментариев к посту")
            download_and_save_comments(cur, community_id, post_vk_owner_id, post_vk_id)
    return len(post_json_data_collection)



def download_and_save_community(conn, community_id, community_name):
    print(f"Начало загрузки паблика {community_name}")
    cursor = conn.cursor()
    offset = 0;
    while True:
        loadedPostsCount = download_and_save_posts(conn, community_id, community_name, offset)
        conn.commit()
        offset += limit_post_count
        if loadedPostsCount < limit_post_count:
            return

    last_update = datetime.now()
    cursor.execute(f'UPDATE communities SET last_update = %s WHERE id = %s', (last_update, community_id) )        
    conn.commit()
    print(f"Загрузка паблика {community_name} завершена")


def download_and_save_coomunities(conn):
    cur = conn.cursor()    
    cur.execute("SELECT id, name FROM communities ORDER BY last_update, id ")

    rows = cur.fetchall()
    for community_id, community_name in rows:
        download_and_save_community(conn, community_id, community_name)


def main():
    conn = psycopg2.connect(database="vk", user = "postgres", password = "masterkey", host = "127.0.0.1", port = "5432")
    download_and_save_coomunities(conn)

if __name__ == '__main__':
	main()