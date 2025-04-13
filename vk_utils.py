import requests
from datetime import date

null_date = date(1899, 12, 31)

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

def get_max_photo_size(json_data):
    max_item_size = 0
    max_item_url = ""
    
    for item in json_data:
        item_size = item["height"]
        if item_size > max_item_size:
            max_item_size = item_size
            item_url = item["url"]

    # return f"фото макс.высота {max_item_size}, ссылка {item_url}"
    return item_url

def get_video_url(url) -> str:
    result = ""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        req = requests.get(url, headers=headers, verify=False)
        res = req.json()
        result = res["response"]["items"][0]["player"]
    except Exception as e:
        result = f'Невозможно получить ссылку на видео {repr(e)}'        

    return result


