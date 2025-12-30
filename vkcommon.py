import json
from datetime import datetime
import psycopg2
import requests
import urllib3
import time

urllib3.disable_warnings()

access_token = ''

user_base_fields = "id,first_name,last_name,deactivated,is_closed,can_access_closed,about,activities,bdate,blacklisted,blacklisted_by_me,bookscan_post,can_see_all_post,scan_see_audio,can_send_friend_request,can_write_private_message,career,city,common_count,connections,contacts,counters,country,crop_photo,domain,education,exports,,followers_count,friend_status,games,has_mobile,has_photo,home_town,interests,is_favorite,is_friend,is_hidden_from_feed,is_no_index,phone,email"
user_optional_fields_L_R = "last_seen,lists,maiden_name,military,movies,music,nickname,occupation,online,personal,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_id,photo_max,photo_max_orig,quotes,relatives,relation"
user_optional_fields_S_W = "schools,screen_name,sex,site,status,timezone,trending,tv,universities,verified,wall_default,is_verified"
user_all_fields = f"{user_base_fields},{user_optional_fields_L_R},{user_optional_fields_S_W}"


request_likes_count = 0
request_comments_count = 0
request_members_count = 0
intervalStart = time.time()
wasPauseNeeded = False

usersBatchSize = 20

requestInterval = 1.0
prevCallTime = time.time()
def load_url_as_json(url: str) -> str:
    global requestInterval, prevCallTime, request_likes_count, request_members_count, request_comments_count

    if 'likes.getList' in url:
        request_likes_count += 1
    elif 'wall.getComments' in url:
        request_comments_count += 1
    elif 'groups.getMembers' in url:
        request_members_count += 1

    url = f'{url}&access_token={access_token}&v=5.195'
    json_data = None
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    for i in range(5):
        try:
            currentTime = time.time()
            interval = currentTime - prevCallTime 
            if (interval < requestInterval):
                time.sleep(requestInterval - interval)
            prevCallTime = currentTime

            req = requests.get(url, headers=headers, verify=False)         
            json_data = req.json()
            errorCode = getJsonValue(json_data, 'error/error_code', 0)
            errorMsg = getJsonValue(json_data, 'error/error_msg', 0)

            if errorCode != 0:
                print(f"Ошибка VK при получении данных с кодом {errorCode}: {errorMsg}")

                if errorCode == 5:
                    print(f"Закончился срок действия токена")
                    break

                if errorCode in [6, 
                                 9, # Flood control
                                 29 # Rate limit reached
                                 ]:
                    print(f"request_likes_count={request_likes_count}, request_comments_count={request_comments_count}, request_members_count={request_members_count}\n{url}")

                if errorCode in [18,  # User was deleted or banned
                                 30,  # This profile is private
                                 ]:
                    return json_data

                if errorCode in [6, 9, 10]:
                    time.sleep(60)
                else:
                    time.sleep(10)
                
            else:
                 return json_data
        except Exception as e:
            print(f'Невозможно получить данные для {url}. Текст ошибки: {repr(e)}')
            time.sleep(10)
    return json_data



def set_access_token(token: str):
    global access_token 
    access_token = token


def needPause():
    global request_likes_count, request_comments_count, request_members_count, intervalStart, wasPauseNeeded

    intervalLength = time.time() - intervalStart

    def tooMatchRequestsPerInterval(count, dayCount):
        if intervalLength == 0:
            result = False
        else:
            result = (count * 3600 * 24) / intervalLength >= dayCount
        return result
    
    requestPerDayLimit = 20000

    needPause = tooMatchRequestsPerInterval(request_likes_count, requestPerDayLimit) \
        or tooMatchRequestsPerInterval(request_comments_count, requestPerDayLimit) \
        or tooMatchRequestsPerInterval(request_members_count, requestPerDayLimit)
    if not needPause and wasPauseNeeded and (intervalLength > 300):
        request_likes_count = 0
        request_comments_count = 0
        request_members_count = 0
        intervalStart = time.time()
        wasPauseNeeded = False
    elif needPause:
        wasPauseNeeded = True

    return needPause

     
def getJsonValue(json, path: str, defaultValue = ''):
    value = defaultValue
    
    if not json:
        return value

    keys = path.split('/')
    key1 = keys[0]
    key2 = keys[1] if len(keys) > 1 else None
    
    if not key1 in json:
        pass
    elif not key2:
        value = json[key1]
    else:
        node = json[key1]
        if key2 in node:
            value = node[key2]				
    return value



def download_and_save_users(conn, userIds):
    cur = conn.cursor()

    userIds = set(userIds)
    userIds = list(userIds)

    unloadedIds = []
    for vk_user_id in userIds:
        if vk_user_id <= 0:
            print(f"\t. Игнорирование аккаунта {vk_user_id} - не является идентификатором пользователя") # В Базе уже есть такая запись
            continue
        cur.execute("""SELECT 1 FROM users WHERE vk_num_id=%s""", (vk_user_id, ) )
        if cur.rowcount > 0:
            print(f"\t. Игнорирование аккаунта {vk_user_id}") # В Базе уже есть такая запись
        else:
            unloadedIds.append(vk_user_id)

    unloadedIdsBatches = [unloadedIds[i:i + usersBatchSize] for i in range(0, len(unloadedIds), usersBatchSize)]

    for unloadedIdsBatch in unloadedIdsBatches:
        unloadedIdsBatchStr = ','.join(map(str, unloadedIdsBatch))

        url = f"https://api.vk.com/method/users.get?user_ids={unloadedIdsBatchStr}&fields={user_all_fields}"
        src = load_url_as_json(url)


        user_json_data_collection = getJsonValue(src, 'response', None)
        if not user_json_data_collection:
            return
        for user_json_data in user_json_data_collection:
            user_vk_num_str = getJsonValue(user_json_data, "domain")
            user_vk_num_id = getJsonValue(user_json_data, "id")
            user_vk_country_name = getJsonValue(user_json_data, "country/title")
            user_vk_city_name = getJsonValue(user_json_data, "city/title")

            user_vk_sex = getJsonValue(user_json_data, "sex")
            if user_vk_sex == 1:
                user_vk_sex = 'Ж'
            elif user_vk_sex == 2:
                user_vk_sex = 'М'
            else:
                user_vk_sex = ''

            try:
                user_date_of_birth = getJsonValue(user_json_data, "bdate")
                user_date_of_birth = datetime.strptime(user_date_of_birth, "%d.%m.%Y")
            except:
                user_date_of_birth = None

            user_first_name = getJsonValue(user_json_data, "first_name")    
            user_last_name = getJsonValue(user_json_data, "last_name")
            user_middle_name = getJsonValue(user_json_data, "middle_name")
            user_nickname = getJsonValue(user_json_data, "nickname")
            user_maiden_name = getJsonValue(user_json_data, "maiden_name")
            user_is_hidden = getJsonValue(user_json_data, "is_closed")
            user_photo_max_orig = getJsonValue(user_json_data, "photo_max_orig")

            json_data = json.dumps(user_json_data)
    
            print(f"\t+ Загрузка и добавление в БД данных аккаунта {user_vk_num_id}")

            #ОШИБКА:  повторяющееся значение ключа нарушает ограничение уникальности "users_vk_num_id_pk"
            #DETAIL:  Ключ "(vk_num_id)=(29358724)" уже существует.
            cur.execute("""INSERT INTO users 
                (first_name, last_name, middle_name, nickname, maiden_name, 
                vk_city_name, vk_country_name, date_of_birth, vk_num_id, vk_str_id, photo_url, vk_sex, is_hidden, json_data)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s                 
                    WHERE NOT EXISTS (SELECT vk_num_id FROM users WHERE vk_num_id = %s) 
                ON CONFLICT (vk_num_id) DO NOTHING
                """, 
                (user_first_name, user_last_name, user_middle_name, user_nickname, user_maiden_name,
                    user_vk_city_name, user_vk_country_name, user_date_of_birth, user_vk_num_id, user_vk_num_str, 
                    user_photo_max_orig, user_vk_sex, user_is_hidden, json_data, 
                    user_vk_num_id) )
            """
            ОШИБКА:  обнаружена взаимоблокировка
DETAIL:  Процесс 15536 ожидает в режиме ShareLock блокировку "транзакция 5336387"; заблокирован процессом 4660.
Процесс 4660 ожидает в режиме ShareLock блокировку "транзакция 5336491"; заблокирован процессом 15536.


psycopg2.errors.DeadlockDetected: ОШИБКА:  обнаружена взаимоблокировка
DETAIL:  Процесс 21488 ожидает в режиме ShareLock блокировку "транзакция 11799653"; заблокирован процессом 16020.
Процесс 16020 ожидает в режиме ShareLock блокировку "транзакция 11799614"; заблокирован процессом 21488.
HINT:  Подробности запроса смотрите в протоколе сервера.
CONTEXT:  при добавлении кортежа индекса (281681,10) в отношении "users"


1. Передавать сюда коннекшен и делать коммиты
2. 
try

except

для блока for user_json_data in user_json_data_collection:

При ошибке - повторить все снова
            """
        conn.commit()

    conn.commit()
    cur.close()



def save_group_member(cur, vk_user_id, vk_group_id):
    cur.execute("""SELECT 1 FROM community_members WHERE vk_user_id = %s AND vk_owner_id = %s""", (vk_user_id, vk_group_id) )
    if cur.rowcount == 0:
        print(f"\t + Добавление в БД подписки пользователя {vk_user_id} на группу {vk_group_id}")
        cur.execute("""INSERT INTO community_members (vk_user_id, vk_owner_id) VALUES (%s, %s)""", (vk_user_id, vk_group_id) )



def save_update_group(cur, vk_group_id, screen_name, name):
    cur.execute("SELECT vk_id, coalesce(description, '') FROM communities WHERE name = %s", (screen_name,) )

    if cur.rowcount == 0:
        print(f"\t + Добавление группы в БД  {screen_name}")
        cur.execute("INSERT INTO communities (vk_id, name, description) VALUES (%s, %s, %s)", (vk_group_id, screen_name, name) )
        return

    stored_id, stored_name = cur.fetchone()
    if (stored_id != vk_group_id) or (name != stored_name):
        print(f"\t * Обновление группы в БД  {screen_name}")
        cur.execute(f'UPDATE communities SET description = %s, vk_id = %s WHERE name = %s',  (name, vk_group_id, screen_name) )
        return
    print(f"\t . Игнорирование группы {screen_name}")