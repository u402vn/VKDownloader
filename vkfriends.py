from datetime import datetime
import psycopg2
from vkcommon import getJsonValue, download_and_save_user, load_url_as_json, save_update_group, save_group_member
from vk_auth import auth_token2, DatabaseConnectionString

limit_friends_count = 30
limit_subscriptions_count = 200

def download_user_friends(conn, userId):
    cur = conn.cursor()
    print(f"Загрузка данных друзей аккаунта {userId}")
    offset = 0
    while True:
        url = f"https://api.vk.com/method/friends.get?user_id={userId}&offset={offset}&count={limit_friends_count}&access_token={auth_token2}&v=5.199"
        src = load_url_as_json(url)
        friend_ids_collection = getJsonValue(src, 'response/items', None)
        loadedCount = 0
        if friend_ids_collection:
            for friendId in friend_ids_collection:                
                cur.execute("""SELECT 1 FROM UserFriends f WHERE (f.vk_user_id1 = %s AND f.vk_user_id2 = %s) or (f.vk_user_id1 = %s AND f.vk_user_id2 = %s)""", 
                    (userId, friendId, friendId, userId) )
                if cur.rowcount == 0:
                    download_and_save_user(cur, auth_token2, friendId)
                    cur.execute("INSERT INTO UserFriends (vk_user_id1, vk_user_id2) VALUES (%s, %s)", (userId, friendId))
                    loadedCount += 1
                    if loadedCount % 10 == 0:
                        conn.commit()

        conn.commit()
        loadedFriendsCount = len(friend_ids_collection) if friend_ids_collection else 0
        if loadedFriendsCount < limit_friends_count:
            break
        offset += limit_friends_count

    last_review = datetime.now()
    cur.execute("UPDATE Users SET last_review = %s WHERE vk_num_id = %s", (last_review, userId))
    conn.commit()



def download_user_communities(conn, userId):
    print(f"Загрузка подписок аккаунта {userId}")

    url = f"https://api.vk.com/method/users.getSubscriptions?user_id={userId}&access_token={auth_token2}&v=5.199"
    src = load_url_as_json(url)
    groups = getJsonValue(src, 'response/groups', None)
    if not groups:
        return
    groupIds = getJsonValue(groups, 'items', None)

    groupIdsBatches = [groupIds[i:i + limit_subscriptions_count] for i in range(0, len(groupIds), limit_subscriptions_count)]

    for groupIdsBatch in groupIdsBatches:
        groupIdsStr = ','.join(map(str, groupIdsBatch))        
        url = f"https://api.vk.com/method/groups.getById?group_ids={groupIdsStr}&access_token={auth_token2}&v=5.199"
        src = load_url_as_json(url)
        group_json_data_collection = getJsonValue(src, 'response/groups', None)

        loadedCount = 0
        cur = conn.cursor()
        if group_json_data_collection:
            for group_json_data in group_json_data_collection:            
                vk_group_id = - getJsonValue(group_json_data, 'id', 0)
                screen_name = getJsonValue(group_json_data, 'screen_name')
                name = getJsonValue(group_json_data, 'name')
            
                save_update_group(cur, vk_group_id, screen_name, name)
                save_group_member(cur, userId, vk_group_id)

                loadedCount += 1
                if loadedCount % 10 == 0:
                    conn.commit()
            
        #id screen_name name - сделать общую функцию сохранения с download_and_save_community(conn, community_name):
        #сделать добавление подписчика в группу общее с download_and_save_community_members


def download_all_friend_for_users_with_comments(conn):
    cur = conn.cursor()
    while True:
        cur.execute("select u.vk_num_id from users u, comments c where u.vk_num_id = c.vk_from_id order by u.last_review asc nulls first limit 1")
        rows = cur.fetchall()
        for userId, in rows:
            download_user_communities(conn, userId)
            download_user_friends(conn, userId)



def download_all_friend_for_users_from_belarus_phones(conn, loadCount: int = -1):
    cur = conn.cursor()
    i = 0
    while (loadCount > 0) and (i < loadCount):
        i += 1
        cur.execute("""select distinct l.vk_user_id from data_leaks l
            left outer join users u on l.vk_user_id = u.vk_num_id
            where (l.phone like '7529%' or l.phone like '7533%' or l.phone like '7544%' or l.phone like '7525%')
            and u.vk_num_id is null
            limit 1""")
        rows = cur.fetchall()
        for userId, in rows:
            download_and_save_user(cur, auth_token2, userId)
            download_user_communities(conn, userId)
            download_user_friends(conn, userId)


def main():
    conn = psycopg2.connect(DatabaseConnectionString)
    download_all_friend_for_users_with_comments(conn)
    download_all_friend_for_users_from_belarus_phones(conn)    
    #todo download_all_friend_for_communities_members



if __name__ == '__main__':
	main()