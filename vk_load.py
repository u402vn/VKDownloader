import requests
import os
import youtube_dl
import json
from vk_auth import auth_token
import urllib3
import vk

# from datetime import datetime
# import vk
# import vk_db
# import vk_utils

urllib3.disable_warnings()

def load_url(url: str) -> str:
    try:    
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        req = requests.get(url, headers=headers, verify=False)         
        return req
    except Exception as e:
        return f'Невозможно получить данные для {url}. Текст ошибки: {repr(e)}'  

def load_url_as_json(url: str) -> str:
    try:    
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        req = requests.get(url, headers=headers, verify=False)         
        return req.json()
    except Exception as e:
        return f'Невозможно получить данные для {url}. Текст ошибки: {repr(e)}'  


def download_img(url, post_id, group_name):
    data_path = f"{group_name}/files"

    if not os.path.exists(data_path):
        os.mkdir(data_path)      

    img_filename = f"{group_name}/files/{post_id}.jpg" 
    if os.path.exists(img_filename):
        return
    
    req = requests.get(url)

    with open(img_filename, "wb") as img_file: 
        img_file.write(req.content)    
        
def download_video(url, post_id, group_name):  
    data_path = f"{group_name}/video_files"

    if not os.path.exists(data_path):
        os.mkdir(data_path)      

    video_filename = f"{group_name}/video_files/{post_id}.mp4"
    if os.path.exists(video_filename):
        return
    
    try:
        ydl = youtube_dl.YoutubeDL({"nocheckcertificate": True,"outtmpl": f"{video_filename}"})

        video_info = ydl.extract_info(
            url,
            download=False # We just want to extract the info
        )
        video_duration = video_info["duration"]
        if video_duration > 300:
            print(f"Слишком долгое видео ({video_duration} секунд)\n")
        else:
            print(f"Видео длится: ({video_duration} секунд)\n")
            ydl.download([url])
    except Exception:
        print("Не удалось скачать видео...\n")
        

def get_wall_post_comments(group_name, owner_id, post_id) -> str:

    comments_filename = f"{group_name}/{post_id}_comments.json" 
    if os.path.exists(comments_filename):
        with open(comments_filename, "r",  encoding="utf-8") as json_data:
            src = json.load(json_data)
            json_data.close()    
        return src
    
    url = f"https://api.vk.com/method/wall.getComments?owner_id={owner_id}&post_id={post_id}&access_token={auth_token}&v=5.199"
    src = load_url_as_json(url)   
    with open(comments_filename, "w",  encoding='utf-8') as json_file: 
         json_string = json.dumps(src)
         json_file.write(json_string)           
    return src


def load_all_countries(offset: int = 0) -> str:

    if offset == 0:
        countries_filename = "countries.json" 
    else:
        countries_filename = f"countries_{offset}.json" 
        
    if os.path.exists(countries_filename):
        with open(countries_filename, "r",  encoding="utf-8") as json_data:
            src = json.load(json_data)
            json_data.close()    
        return src

    if offset == 0:
        url = f"https://api.vk.com/method/database.getCountries?lang=ru&need_all=1&access_token={auth_token}&v=5.199"
    else:
        url = f"https://api.vk.com/method/database.getCountries?lang=ru&need_all=1&access_token={auth_token}&offset={offset}&v=5.199"
        
    src = load_url_as_json(url)   
    datas = src["response"]
    with open(countries_filename, "w",  encoding='utf-8') as json_file: 
         json_string = json.dumps(datas)
         json_file.write(json_string)
    return datas;

def parse_countries(datas, countries):
    for item in datas:
        country = vk.VKCountry()
        country.vk_id = item["id"]
        country.name = item["title"]
        countries.append(country)    

def get_all_countries(countries):
    datas = load_all_countries()
    country_count = datas["count"]
    items = datas["items"]
    parse_countries(items, countries)
    
    offset = len(countries)
    while len(countries) < country_count:
        sub_datas = load_all_countries(offset)
        sub_items = sub_datas["items"]
        parse_countries(sub_items, countries)
        offset = len(countries)     

def load_all_cities(offset: int = 0) -> str:

    if offset == 0:
        cities_filename = "cities/cities.json" 
    else:
        cities_filename = f"cities/cities_{offset}.json" 
        
    if os.path.exists(cities_filename):
        with open(cities_filename, "r",  encoding="utf-8") as json_data:
            src = json.load(json_data)
            json_data.close()    
        return src

    if offset == 0:
        url = f"https://api.vk.com/method/database.getCities?lang=ru&need_all=1&access_token={auth_token}&v=5.199"
    else:
        url = f"https://api.vk.com/method/database.getCities?lang=ru&need_all=1&access_token={auth_token}&offset={offset}&v=5.199"
        
    src = load_url_as_json(url)   
    datas = src["response"]
    with open(cities_filename, "w",  encoding='utf-8') as json_file: 
         json_string = json.dumps(datas)
         json_file.write(json_string)
    return datas;

def parse_cities(datas, cities):
    for item in datas:
        city = vk.VKCity()
        city.vk_id = item["id"]
        city.name = item["title"]
        if "area" in item:
            city.area = item["area"]
        else:
            city.area = None
        if "region" in item:
            city.region = item["region"]
        else:
            city.region = None
        cities.append(city)    

def get_all_cities(cities):
    datas = load_all_cities()
    city_count = datas["count"]
    items = datas["items"]
    parse_cities(items, cities)
    
    offset = len(cities)
    while len(cities) < city_count:
        sub_datas = load_all_cities(offset)
        sub_items = sub_datas["items"]
        parse_cities(sub_items, cities)
        offset = len(cities)     