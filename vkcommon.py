import json
from datetime import datetime
import psycopg2
import requests
import urllib3
import time

urllib3.disable_warnings()

user_base_fields = "id,first_name,last_name,deactivated,is_closed,can_access_closed,about,activities,bdate,blacklisted,blacklisted_by_me,bookscan_post,can_see_all_post,scan_see_au         dio,can_send_friend_request,can_write_private_message,career,city,common_count,connections,contacts,counters,country,crop_photo,domain,education,exports,,followers_count,friend_status,games,has_mobile,has_photo,home_town,interests,is_favorite,is_friend,is_hidden_from_feed,is_no_index"
user_optional_fields_L_R = "last_seen,lists,maiden_name,military,movies,music,nickname,occupation,online,personal,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_id,photo_max,photo_max_orig,quotes,relatives,relation"
user_optional_fields_S_W = "schools,screen_name,sex,site,status,timezone,trending,tv,universities,verified,wall_default,is_verified"
user_all_fields = f"{user_base_fields},{user_optional_fields_L_R},{user_optional_fields_S_W}"



prevCallTime = time.time()
def load_url_as_json(url: str) -> str:
    global prevCallTime
    currentTime = time.time()
    interval = currentTime - prevCallTime 
    if (interval < 0.4):
        time.sleep(0.4 - interval)
    prevCallTime = currentTime
  
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    for i in range(5):
        try:
            req = requests.get(url, headers=headers, verify=False)         
            json_data = req.json()
            errorCode = getJsonValue(json_data, 'error/error_code', 0)
            if errorCode == 5:
                print(f"Закончился срок действия токена")
                break
            if errorCode != 6:
                break
        except Exception as e:
            print(f'Невозможно получить данные для {url}. Текст ошибки: {repr(e)}')
        time.sleep(5)
    return json_data



     
def getJsonValue(json, path: str, defaultValue = ''):
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



def download_and_save_user(cur, auth_token, vk_user_id):
    if vk_user_id <= 0:
        return
    
    cur.execute("""SELECT 1 FROM users WHERE vk_num_id=%s""", (vk_user_id, ) )
    if cur.rowcount > 0:
        print(f"\t. Игнорирование аккаунта {vk_user_id}")
        return # В Базе уже есть такая запись
    
    print(f"\t+ Загрузка и добавление в БД данных аккаунта {vk_user_id}")

    url = f"https://api.vk.com/method/users.get?user_ids={vk_user_id}&fields={user_all_fields}&access_token={auth_token}&v=5.199"
    src = load_url_as_json(url)

    #check_for_errors_with_exception(datas) 
    user_json_data = src["response"][0] 
    
    user_vk_num_str = getJsonValue(user_json_data, "domain")
    user_vk_num_id = getJsonValue(user_json_data, "id") # должен быть равен vk_user_id
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
    cur.execute("""INSERT INTO users 
        (first_name, last_name, middle_name, nickname, maiden_name, 
        vk_city_name, vk_country_name, date_of_birth, vk_num_id, vk_str_id, photo_url, vk_sex, is_hidden, json_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
        (user_first_name, user_last_name, user_middle_name, user_nickname, user_maiden_name,
            user_vk_city_name, user_vk_country_name, user_date_of_birth, user_vk_num_id, user_vk_num_str, 
            user_photo_max_orig, user_vk_sex, user_is_hidden, json_data) )



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