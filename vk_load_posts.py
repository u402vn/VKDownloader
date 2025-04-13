from ctypes.wintypes import BOOL
import json
from datetime import datetime
from vk_auth import auth_token
import vk
import vk_utils
import vk_load
import vk_db

max_post_count = 100
    
def get_wall_posts(conn, community: vk.VKCommunity, max_post_count, offset, result, exists_ids_dict: dict = None):

    url = f"https://api.vk.com/method/wall.get?domain={community.name}&offset={offset}&count={max_post_count}&access_token={auth_token}&v=5.199"
    src = vk_load.load_url_as_json(url)

    error_info = []
    if not vk_utils.check_for_errors(src, error_info):
        raise Exception(f"Code: {error_info[0]}, Message: '{error_info[1]}'")

    if result is not None:
        all_post_count = src["response"]["count"]
        result.clear()
        result.append(all_post_count - community.db_post_count);
        result.append(all_post_count)
        result.append(community.db_post_count);

    posts = src["response"]["items"]
    for post in posts:
        if "signer_id" in post:
            signer_id = post["signer_id"]

        vk_id = post["id"]

        post_already_exists = False
        if exists_ids_dict is not None: 
            post_already_exists = vk_id in exists_ids_dict
        else:
            post_already_exists = vk_db.check_post(conn, vk_id)

        if not post_already_exists:
            vk_post = vk.VKCommunityPost(community)
            vk_post.vk_id = vk_id
            vk_post.community = community
            vk_post.community_id = community.id
            vk_post.vk_owner_id = post["owner_id"]
            vk_post.vk_from_id = post["from_id"]
            if "created_by" in post:
                vk_post.vk_created_by = post["created_by"]
            if "signer_id" in post:
                vk_post.vk_signer_id = post["signer_id"]
            vk_post.text = post["text"]
            vk_post.date = datetime.fromtimestamp(post["date"])
            
            vk_db.create_post(conn, vk_post)
            vk_post.id = vk_db.find_post_id(conn, community.id, vk_id)
    
            community.posts.append(vk_post)

            # log_str = vk_post.text[:32].replace('\n', ' ').replace('\r', '')
            # print(f"\rДобавлен новый пост: {vk_post.date} '{log_str}'", end = "", flush = True)
            print(f"Добавлен новый пост: {vk_post.date}\n", end = "", flush = True)
        else:
            pass
            return


def check_new_wall_posts(conn, community: vk.VKCommunity, limit_post_count, result, exists_ids_dict: dict = None):
    get_wall_posts(conn, community, limit_post_count, 0, result, exists_ids_dict)    

    if len(community.posts) < limit_post_count or result[0] < limit_post_count:
        return

    offset = limit_post_count;
    while True:
        count_before = len(community.posts)
        get_wall_posts(conn, community, limit_post_count, offset, None, exists_ids_dict)    
        
        loaded = len(community.posts) - count_before
        if loaded < limit_post_count:
            break
        
        if offset + loaded == result[0]:
            pass
            break

        offset = offset + limit_post_count