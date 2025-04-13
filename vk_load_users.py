import json
import os
from datetime import datetime
from tabnanny import check

import vk
import vk_load
from vk_auth import auth_token
from vk_utils import check_for_errors_with_exception, null_date


def parse_country_info(datas):
    if "country" in datas:
        result = datas["country"]["id"]
        # info = datas["country"]
        # result = []
        # result.append(info["id"])
        # result.append(info["title"])
    else:
        result = None

def parse_city_info(datas):
    if "city" in datas:
        result = datas["city"]["id"]
        # info = datas["city"]
        # result = []
        # result.append(info["id"])
        # result.append(info["title"])
    else:
        result = None

    return result

def parse_sex_info(datas):
    result = 0

    if "sex" in datas:
        info = datas["sex"]
        if info is not None:
            result = info
        
    return result

def parse_date_of_birth_info(datas, vk_user_id):
    if "bdate" in datas:
        try:
            value = datas["bdate"]
            result = datetime.fromtimestamp(value)
        except:
            result = None
            # print(f"Некорректная дата рожденья {value} для пользователя {vk_user_id}")
    else:
        result = None
        
    return result

user_base_fields = "id,first_name,last_name,deactivated,is_closed,can_access_closed,about,activities,bdate,blacklisted,blacklisted_by_me,bookscan_post,can_see_all_post,scan_see_au         dio,can_send_friend_request,can_write_private_message,career,city,common_count,connections,contacts,counters,country,crop_photo,domain,education,exports,,followers_count,friend_status,games,has_mobile,has_photo,home_town,interests,is_favorite,is_friend,is_hidden_from_feed,is_no_index"
user_optional_fields_L_R = "last_seen,lists,maiden_name,military,movies,music,nickname,occupation,online,personal,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_id,photo_max,photo_max_orig,quotes,relatives,relation"
user_optional_fields_S_W = "schools,screen_name,sex,site,status,timezone,trending,tv,universities,verified,wall_default,is_verified"
user_all_fields = f"{user_base_fields},{user_optional_fields_L_R},{user_optional_fields_S_W}"

def parse_user_info(datas: str, user: vk.VKUser):
    check_for_errors_with_exception(datas)
    src = datas["response"][0]
    
    user.json_data = src
    user.vk_num_str = src["domain"]
    user.vk_num_id = src["id"]
    user.vk_country_id = parse_country_info(src)
    user.vk_city_id = parse_city_info(src)
    user.vk_sex_id = parse_sex_info(src)
    user.date_of_birth = parse_date_of_birth_info(src, user.vk_num_id)
    user.first_name = src["first_name"]
    user.last_name = src["last_name"]
    if "nickname" in src:
        user.middle_name = src["nickname"]
    else:
        user.middle_name = None
    user.is_hidden = src["is_closed"]
    if "maiden_name" in src:
        user.maiden_name = src["maiden_name"]
    else:
        user.maiden_name = None
    if "photo_max_orig" in src:
        user.photo_max_orig = src["photo_max_orig"]
    else:
        user.photo_max_orig = None

def get_user_info(user_id):

    # json_data_filename = f"users/{user_id}.json"
    # if os.path.exists(json_data_filename):
    #     with open(json_data_filename, "r",  encoding="utf-8") as json_data:
    #         src = json.load(json_data)
    #         json_data.close()    
    #     return src

    url = f"https://api.vk.com/method/users.get?user_ids={user_id}&fields={user_all_fields}&access_token={auth_token}&v=5.199"
    src = vk_load.load_url_as_json(url)
    # with open(json_data_filename, "w",  encoding='utf-8') as json_file: 
    #      json_string = json.dumps(src)
    #      json_file.write(json_string)           
    return src

def get_user_photo(url, user: vk.VKUser):
    if url is None:
        return

    # photo_filename = f"users/photo/{user.vk_num_id}.jpg"
    # if os.path.exists(photo_filename):
    #     with open(photo_filename, mode='rb') as file: # b is important -> binary
    #         user.photo = file.read()

    # else:
    request = vk_load.load_url(url)
    if request.status_code == 200:
        data_size = len(request.content)
        # картинка по умолчанию или пользователь удалён
        if data_size == 4320:
            print(f"! Фото по умолчанию для контакта {user.last_name} {user.first_name} (vk_id {user.vk_num_id})")
        elif data_size == 8783:
            print(f"! Нет фото так как контакт {user.last_name} {user.first_name} (vk_id {user.vk_num_id}) удалён")
        else:
            user.photo = request.content
        # try:
        #     file = open(photo_filename, "wb") 
        #     file.write(user.photo)
        # finally:
        #     file.close()
    else:
        print(f"! Невозможно загрузить фото для контакта {user.last_name} {user.first_name} (vk_id {user.vk_num_id})")
        return
