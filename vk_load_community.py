# import time
from datetime import datetime
import vk
import vk_db
import vk_load_posts
import vk_load_comments
import vk_load_users

def create_new_comment_and_check_user(connection, comment: vk.VKComment, old_user_dict: dict, new_users_dict: dict):
    vk_db.create_comment(connection, comment)
    if comment.vk_from_id > 0:
        old_user_exists = False
        if comment.vk_from_id in old_user_dict:
            old_user_exists = True
        if old_user_exists == False:
            old_user_exists = vk_db.check_user(connection, comment.vk_from_id)
        if old_user_exists == True:
            old_user_dict[comment.vk_from_id] = comment.vk_from_id        
        else:
            new_users_dict[comment.vk_from_id] = comment.vk_from_id    
        #????

def load_all_new_comments(connection, community: vk.VKCommunity, new_users_dict: dict, posts_ids_dict: dict, posts_vk_id_dict: dict, posts_vk_owner_id_dict: dict):
    all_comment_count = 0
    old_user_dict = dict()
    new_users_dict.clear

    for post_id in posts_ids_dict.values():
        comments_count = vk_db.load_comments_count(connection, community.id, post_id)    
        print(f"post_id: {post_id} - {comments_count} comment(s)\n")
    
        if comments_count > 0:
            sdd = comments_count
            if sdd > 0:
               pass

        if comments_count == 0:
            print("Загружаем комментарии к постам...")
            
            post = vk.VKCommunityPost(community)
            post.id = post_id
            post.vk_id = posts_vk_id_dict[post_id]
            post.vk_owner_id = posts_vk_owner_id_dict[post_id]
            post.community_id = community.id
            vk_load_comments.load_all_post_comments(post)    
            all_comment_count += len(post.comments)        
            for comment in post.comments:
                create_new_comment_and_check_user(connection, comment, old_user_dict, new_users_dict)
                    
            print(f"Загружено {all_comment_count} комментариев                                  \n")

            
def load_all_new_users(connection, new_users_dict: dict):
    # print("Загружаем пользователей новых комментариев...")
    user_index = 1
    vk_user_ids = new_users_dict.values()
    for vk_user_id in vk_user_ids:
        # print(f"\rЗагрузка пользователей... {user_index} из {user_len}... ID: {vk_user_id}                                ", end = "", flush = True)
        data = vk_load_users.get_user_info(vk_user_id)
        user = vk.VKUser()    
        vk_load_users.parse_user_info(data, user)
        vk_db.create_user(connection, user)
        user_index += 1   
        

def community_check_new_posts(connection, community: vk.VKCommunity, exists_ids_dict: dict):
    result = []

    print("Загружаем новые посты...\n")
    vk_load_posts.check_new_wall_posts(connection, community, vk_load_posts.max_post_count, result, exists_ids_dict)
    print(f"Загружено {len(community.posts)} постов                          \n")
    
    if len(community.posts) == 0:
        return;

    posts_ids_dict = dict()
    posts_vk_id_dict = dict() 
    posts_vk_owner_id_dict = dict()
    for post in community.posts:
        posts_ids_dict[post.vk_id] = post.id
        posts_vk_id_dict[post.id] = post.vk_id
        posts_vk_owner_id_dict[post.id] = post.vk_owner_id

    new_users_dict = dict()    
    load_all_new_comments(connection, community, new_users_dict, posts_ids_dict, posts_vk_id_dict, posts_vk_owner_id_dict)
    load_all_new_users(connection, new_users_dict)
    
# функция проверки новых сообщений к определённому посту в сообществе
def post_check_updates(connection, community: vk.VKCommunity, post_id: int, post_vk_id: int, post_vk_owner_id: int):
    post_old = vk.VKCommunityPost(community)
    post_old.id = post_id
    # post_old.vk_id = post_vk_id
    # post_old.vk_owner_id = post_vk_owner_id
    vk_db.load_comments_lite(connection, post_old)
    old_ids = dict()
    for old_comment in post_old.comments:
        old_ids[old_comment.vk_id] = old_comment.vk_id
    
    post_new = vk.VKCommunityPost(community)
    post_new.id = post_id
    post_new.vk_id = post_vk_id
    post_new.vk_owner_id = post_vk_owner_id
    post_new.community_id = community.id
    vk_load_comments.load_all_post_comments(post_new)    

    new_users_dict = dict()      
    old_user_dict = dict()
    for new_comment in post_new.comments:
        if not new_comment.vk_id in old_ids:
            create_new_comment_and_check_user(connection, new_comment, old_user_dict, new_users_dict)
    load_all_new_users(connection, new_users_dict) #????
        
# функция проверки новых сообщений к постам в сообществе за определённый период           
def community_check_updates(connection, community: vk.VKCommunity, period_in_days: int):
    print(f'Загрузка существующих постов из базы за последние {period_in_days} дней...\n')

    posts_ids_dict = dict()
    posts_vk_id_dict = dict()
    posts_vk_owner_id_dict = dict()

    vk_db.load_posts_ids_by_period(connection, community.id, period_in_days, posts_ids_dict, posts_vk_id_dict, posts_vk_owner_id_dict)

    print(f'Поиск обновлений для постов ({len(posts_ids_dict)})...\n')
    for post_id in posts_ids_dict.values():
        post_check_updates(connection, community, post_id, posts_vk_id_dict[post_id], posts_vk_owner_id_dict[post_id])
        
def check_updates_for_community(connection, community: vk.VKCommunity, period_in_days: int):
    print(f'Загрузка и создание словаря идентификаторов постов из базы для <{community.name}>...\n')

    posts_ids_dict = dict()
    vk_db.load_posts_ids(connection, community.id, posts_ids_dict)

    print(f'В словаре {len(posts_ids_dict)} элементов\n')
    
    # Если в базе уже есть посты, то сначала проверим обновления
    if len(posts_ids_dict) > 0: 
        community_check_updates(connection, community, period_in_days)
        
    # Проверим есть ли новые посты
    community_check_new_posts(connection, community, posts_ids_dict)
    
    # Обновим дату синхронизации данных для сообщества
    vk_db.save_lastupdate_for_community(connection, community.id, datetime.now())
    

def community_force_load_comments(connection, community: vk.VKCommunity):
    print(f'Загрузка и создание словаря идентификаторов постов из базы для <{community.name}>...\n')
    
    posts_ids_dict = dict()
    posts_vk_id_dict = dict()
    posts_vk_owner_id_dict = dict()
    
    vk_db.load_posts_ids(connection, community.id, posts_ids_dict, posts_vk_id_dict, posts_vk_owner_id_dict)
    print(f'В словаре {len(posts_ids_dict)} элементов')
    
    new_users_dict = dict()    
    load_all_new_comments(connection, community, new_users_dict, posts_ids_dict, posts_vk_id_dict, posts_vk_owner_id_dict)
    load_all_new_users(connection, new_users_dict)
            
        