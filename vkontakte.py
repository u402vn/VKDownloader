import psycopg2
# from vk import VKPost
# import vk
import vk_db
# import vk_load
# import vk_load_posts
# import vk_load_comments
from prettytable import PrettyTable
# import time
# from datetime import date
# import vk_load_users
from vk_load_community import check_updates_for_community
from vk_load_community import community_force_load_comments

conn = psycopg2.connect(database="vk", user = "postgres", password = "masterkey", host = "127.0.0.1", port = "5432")

# раскомментировать если нужно наполнить таблицу с названиями стран
# countries = []
# vk_load.get_all_countries(countries)
# print(f"Количество стран: {len(countries)}")
# print(f"Сохраняем в базу данных...")
# vk_db.create_countries(conn, countries)

# НЕ ПОДХОДИТ !!! ТАК КАК ВОЗВРАЩАЕТ ГОРОДА ИЗ СТРАНЫ ПОЛЬЗОВАТЕЛЯ КОТОРЫЙ ВЫЗЫВАЕТ API
# раскомментировать если нужно наполнить таблицу с названиями городов
# cities = []
# vk_load.get_all_cities(cities)
# print(f"Количество городов: {len(cities)}")
# print(f"Сохраняем в базу данных...")
# vk_db.create_cities(conn, cities)


# city_codes = dict()
# vk_db.load_all_cities_codes(conn, city_codes)

# exist_users_dict = dict()

communities = []
vk_db.load_communities(conn, communities)

table = PrettyTable()
table.field_names = ['ИД', 'Название', 'Кол-во постов в БД', 'Последнее обновление']
for community in communities: 
   table.add_row([community.id, community.name, community.db_post_count, community.last_update])
   # print(f"{item.community_id}, '{item.name}'") 
print(table) 

for community in communities: 
    # if community.id == 2:
    #     community_force_load_comments(conn, community)  
    check_updates_for_community(conn, community, 5) # период N дней ???
    
print(f'Обновление данных завершено\n')

# # start = time.perf_counter()
# # print("Идёт загрузка постов...")
# community = communities[0]
# # vk_load_posts.check_new_wall_posts(conn, community, vk_load_posts.max_post_count, None)
# # end = time.perf_counter()
# # print(f"Загружено {len(community.posts)} новых постов за {end-start:0.4f} секунд")

# print("Идёт загрузка постов...")
# start = time.perf_counter()
# community.posts.clear
# vk_db.load_posts(conn, community)
# end = time.perf_counter()
# print(f"Загружено {len(community.posts)} постов за {end-start:0.4f} секунд")

# print("Идёт загрузка комментариев...")
# all_comment_count = 0
# all_post_no_comment_count = 0
# start = time.perf_counter()
# post_index = 1
# post_len = len(community.posts)

# users_dict = dict()

# for post in community.posts:
#     new_post = vk.VKCommunityPost(community)
#     post.copy_to(new_post)

#     # if post_index < 5079:
#     #     post_index += 1
#     #     continue

#     # if post_index == 5079:
#     #     pass

#     # if post.last_update != date(1899, 12, 31):
#     #     continue
    
#     vk_db.load_comments(conn, post)
    
#     for comment in post.comments:
#         if comment.vk_from_id > 0:
#             users_dict[comment.vk_from_id] = comment.vk_from_id

#     # if len(post.comments) == 0:
#     #     # print(f"Поиск комментариев... пост {post_index} из {post_len}... {post.date} {post.text[:64]}...")
#     #     vk_load_comments.load_all_post_comments(new_post)
#     #     print(f"Поиск комментариев... пост {post_index} из {post_len}... {post.date}...  {len(new_post.comments)}")
#     #     for comment in new_post.comments:
#     #         new_comment = vk.VKComment()
#     #         comment_exists = vk_db.load_comment(conn, comment.vk_id, new_comment)
#     #         if not comment_exists:
#     #              vk_db.create_comment(conn, comment)
#     # if len(post.comments) == 0:
#     #     all_post_no_comment_count += 1
#     # else:
#     #     all_comment_count += len(post.comments)
        
#     # post_index += 1
    
# # all_comment_countend = all_comment_count + len(new_post.comments)

# end = time.perf_counter()

# # users_dict[618128711] = 618128711

# user_len = len(users_dict)
# print(f"Количество уникальных пользователей: {user_len}")
# user_index = 1
# for vk_user_id in users_dict.values():
#     print(f"Поиск пользователея... {user_index} из {user_len}... ID: {vk_user_id}")
    
#     if user_index <= 61804:
#         user_index += 1
#         continue

#     data = vk_load_users.get_user_info(vk_user_id)

#     user = vk.VKUser()

#     vk_load_users.parse_user_info(data, user)
#     # vk_load_users.get_user_photo(user.photo_max_orig, user)

#     vk_db.create_user(conn, user)

#     # break
#     user_index += 1


# print(f"Загружено {all_comment_count} комментариев за {end-start:0.4f} секунд")
# print(f"Нет комментариев в {all_post_no_comment_count} постах")

conn.close()