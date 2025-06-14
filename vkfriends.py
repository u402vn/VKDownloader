from datetime import datetime
import psycopg2
from vkcommon import getJsonValue, download_and_save_users, load_url_as_json, save_update_group, save_group_member
from vk_auth import DatabaseConnectionString

limit_friends_count = 1000
limit_subscriptions_count = 200

def markUserLastReview(conn, userId):
    cur = conn.cursor()
    last_review = datetime.now()
    cur.execute("UPDATE Users SET last_review = %s WHERE vk_num_id = %s", (last_review, userId))
    conn.commit()



def download_user_friends(conn, userId):
    cur = conn.cursor()
    print(f"Загрузка данных друзей аккаунта {userId}")

    allFriends = []
    offset = 0
    while True:
        url = f"https://api.vk.com/method/friends.get?user_id={userId}&offset={offset}&count={limit_friends_count}"
        #url = f"https://api.vk.com/method/friends.get?user_id={userId}&offset={offset}"
        src = load_url_as_json(url)
        friend_ids_collection = getJsonValue(src, 'response/items', None)
        if not friend_ids_collection:
            break
        allFriends += friend_ids_collection
        if friend_ids_collection:
            for friendId in friend_ids_collection:                
                cur.execute("""SELECT 1 FROM UserFriends f WHERE (f.vk_user_id1 = %s AND f.vk_user_id2 = %s) or (f.vk_user_id1 = %s AND f.vk_user_id2 = %s)""", 
                    (userId, friendId, friendId, userId) )
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO UserFriends (vk_user_id1, vk_user_id2) VALUES (%s, %s)", (userId, friendId))

        conn.commit()
        loadedFriendsCount = len(friend_ids_collection) if friend_ids_collection else 0
        if loadedFriendsCount < limit_friends_count:
            break
        offset += limit_friends_count

    download_and_save_users(conn, allFriends)

    conn.commit()



def download_user_communities(conn, userId):
    print(f"Загрузка подписок аккаунта {userId}")

    url = f"https://api.vk.com/method/users.getSubscriptions?user_id={userId}"
    src = load_url_as_json(url)
    groups = getJsonValue(src, 'response/groups', None)
    if not groups:
        return
    groupIds = getJsonValue(groups, 'items', None)

    unknownGroupIds = []
    cur = conn.cursor()
    for vk_group_id in groupIds:
        save_group_member(cur, userId, vk_group_id)
        cur.execute("SELECT 1 FROM communities WHERE vk_id = - %s", (vk_group_id,) )
        if cur.rowcount == 0:
            unknownGroupIds.append(vk_group_id)
    conn.commit()

    groupIdsBatches = [unknownGroupIds[i:i + limit_subscriptions_count] for i in range(0, len(unknownGroupIds), limit_subscriptions_count)]

    for groupIdsBatch in groupIdsBatches:
        groupIdsStr = ','.join(map(str, groupIdsBatch))        
        url = f"https://api.vk.com/method/groups.getById?group_ids={groupIdsStr}"
        src = load_url_as_json(url)
        group_json_data_collection = getJsonValue(src, 'response/groups', None)

        if group_json_data_collection:
            for group_json_data in group_json_data_collection:            
                vk_group_id = - getJsonValue(group_json_data, 'id', 0)
                screen_name = getJsonValue(group_json_data, 'screen_name')
                name = getJsonValue(group_json_data, 'name')            
                save_update_group(cur, vk_group_id, screen_name, name)
                #save_group_member(cur, userId, vk_group_id)

        conn.commit()



def download_all_friend_for_users_with_comments(conn, instanceIndex: int, instanceCount: int, loadCount: int):
    cur = conn.cursor()

    cur.execute(f"""select u.vk_num_id from users u 
        where u.last_review is null and u.vk_num_id % {instanceCount} = {instanceIndex} and u.vk_country_name = 'Беларусь' limit {loadCount}""")
    rows = cur.fetchall()
    for userId, in rows:
        download_user_communities(conn, userId)
        download_user_friends(conn, userId)
        markUserLastReview(conn, userId)
    conn.commit()


def download_all_friend_for_users_from_belarus_phones(conn, instanceIndex: int, instanceCount: int, loadCount: int):
    cur = conn.cursor()

    cur.execute(f"""select l.vk_user_id from data_leaks l
        left outer join users u on l.vk_user_id = u.vk_num_id
        where (l.phone like '7529%' or l.phone like '7533%' or l.phone like '7544%' or l.phone like '7525%')
        and u.vk_num_id is null
        and l.vk_user_id % {instanceCount} = {instanceIndex}
        limit {loadCount}""")
    rows = cur.fetchall()

    userIds = [row[0] for row in rows]
    download_and_save_users(conn, userIds)
    conn.commit()

    for userId, in rows:
        download_user_communities(conn, userId)
        download_user_friends(conn, userId)
        markUserLastReview(conn, userId)
    conn.commit()



def download_users_from_db_with_leaks(conn, instanceIndex: int, instanceCount: int, loadCount: int):
    cur = conn.cursor()

    cur.execute(f"""select u.vk_num_id from users u
        left join data_leaks l on l.vk_user_id = u.vk_num_id
        where u.last_review is null and u.vk_num_id % {instanceCount} = {instanceIndex} limit {loadCount}""")
    rows = cur.fetchall()
    for userId, in rows:
        download_user_communities(conn, userId)
        download_user_friends(conn, userId)
        markUserLastReview(conn, userId)
    conn.commit()