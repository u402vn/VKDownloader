import psycopg2
import json
import vk
from datetime import datetime

def log_db_error(error):
    print(error)

def load_communities(conn, items):
    cur = conn.cursor()
    
    cur.execute("SELECT id, name, post_count, last_update FROM communities")
    rows = cur.fetchall()
    for row in rows:
       item = vk.VKCommunity()
       item.id = row[0]
       item.name = row[1]
       item.db_post_count = row[2]
       item.last_update = row[3]
       
       items.append(item)
       
    conn.commit()

def check_post(conn, vk_id) -> bool:
    cur = conn.cursor()
   
    result = False

    cur.execute(f"SELECT id FROM posts WHERE vk_id = %s", (vk_id, ) )
    rows = cur.fetchall()
    if len(rows) > 0:
        result = True
       
    conn.commit()
    
    return result

def __load_post(conn, post: vk.VKCommunityPost):
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT id, community_id, vk_id, vk_owner_id, vk_from_id, vk_created_by, vk_signer_id, date, text, last_update FROM posts WHERE vk_id = %s", (post.vk_id, ) )
    
    rows = cursor.fetchall()
    post.id, post.community_id, post.vk_id, post.vk_owner_id, post.vk_from_id, post.vk_created_by, post.vk_signer_id, post.date, post.text, post.last_update = rows[0]
    if len(rows) != 1:
        raise Exception(f"Не найден поста в БД c vk_id равным {post.vk_id}")

    conn.commit()    

def create_post(connection, post: vk.VKCommunityPost):
    try:
        cursor = connection.cursor()
    
        values = (post.community_id, post.vk_id, post.vk_owner_id, post.vk_from_id, post.vk_created_by, post.vk_signer_id, post.date, post.text)

        cursor.execute("INSERT INTO posts (community_id, vk_id, vk_owner_id, vk_from_id, vk_created_by, vk_signer_id, date, text) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", values)
    
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        connection.rollback
    finally:
        cursor.close()        

def find_post_id(connection, community_id: int, vk_id: int):
    try:
        result = None

        cursor = connection.cursor()
    
        cursor.execute(f"SELECT id FROM posts WHERE community_id = %s and vk_id = %s", (community_id, vk_id) )
        rows = cursor.fetchall()
        if len(rows) == 1:
            result = rows[0][0]
        else:
            raise Exception(f"Не найден ID поста в БД для сообщества {community_id} и vk_id равным {vk_id}")

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        connection.rollback
    finally:
        cursor.close()        
        return result

def load_posts(connection, community: vk.VKCommunity):
    try:
        cursor = connection.cursor()
   
        community.posts.clear

        cursor.execute(f"SELECT id, community_id, vk_id, vk_owner_id, vk_from_id, vk_created_by, vk_signer_id, date, text, last_update \
                    FROM public.posts WHERE community_id = %s ORDER BY date DESC", (community.id, ) )
        rows = cursor.fetchall()
        for row in rows:
           item = vk.VKCommunityPost(community)
           item.id = row[0]
           item.community_id = row[1]
           item.vk_id = row[2]
           item.vk_owner_id = row[3]
           item.vk_from_id = row[4]
           item.vk_created_by = row[5]
           item.vk_signer_id = row[6]
           item.date = row[7]
           item.text = row[8]
           item.last_update = row[9]
       
           community.posts.append(item)

        connection.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        connection.rollback
    finally:
        cursor.close()
    
def load_posts_ids(connection, community_id: int, ids_dict: dict, vk_id_dict: dict = None, vk_owner_id_dict: dict = None):
    try:
        cursor = connection.cursor()

        ids_dict.clear
    
        cursor.execute(f"SELECT id, vk_id, vk_owner_id FROM posts WHERE community_id = %s ORDER BY date DESC", (community_id, ) )
        rows = cursor.fetchall()
        for row in rows:
           post_id = row[0]
           post_vk_id = row[1]
           post_vk_owner_id = row[2]
      
           ids_dict[post_vk_id] = post_id
           
           if not vk_id_dict is None:
               vk_id_dict[post_id] = post_vk_id
               
           if not vk_owner_id_dict is None:
               vk_owner_id_dict[post_id] = post_vk_owner_id
       
        connection.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        connection.rollback
    finally:
        cursor.close()


def load_posts_ids_by_period(connection, community_id: int, period_in_days: int, ids_dict: dict, vk_id_dict: dict = None, vk_owner_id_dict: dict = None):
    try:
        cursor = connection.cursor()

        ids_dict.clear
    
        cursor.execute(f"SELECT id, vk_id, vk_owner_id FROM public.posts WHERE community_id = %s AND date > current_date - interval '{period_in_days}' day ORDER BY date DESC", (community_id, ) )
        rows = cursor.fetchall()
        for post_id, post_vk_id, post_vk_owner_id in rows:
           ids_dict[post_vk_id] = post_id
           
           if not vk_id_dict is None:
               vk_id_dict[post_id] = post_vk_id
               
           if not vk_owner_id_dict is None:
               vk_owner_id_dict[post_id] = post_vk_owner_id
       
        connection.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        connection.rollback
    finally:
        cursor.close()
    
def create_comment(conn, comment: vk.VKComment):
    cur = conn.cursor()
    
    values = (comment.vk_id, comment.vk_from_id, comment.date, comment.text, comment.reply_to_user, comment.reply_to_comment, comment.post_id, comment.community_id)

    try:
        cur.execute("INSERT INTO comments (vk_id, vk_from_id, date, text, reply_to_user, reply_to_comment, post_id, community_id) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", values)
    except Exception as e:
        log_db_error(e)
    
    conn.commit()
    
def __load_comment(conn, vk_id, comment: vk.VKComment) -> bool:
    result = False
    cur = conn.cursor()
    cur.execute(f"SELECT id, vk_id, vk_from_id, date, text, reply_to_user, reply_to_comment, post_id, community_id, last_update \
                FROM comments WHERE vk_id = %s", (vk_id, ) )
    rows = cur.fetchall()
    for row in rows:
       item = vk.VKComment()
       item.id = row[0]
       item.vk_id = row[1]
       item.vk_from_id = row[2]
       item.date = row[3]
       item.text = row[4]
       item.reply_to_user = row[5]
       item.reply_to_comment = row[6]
       item.post_id = row[7]
       item.community_id = row[8]
       item.last_update = row[9]
       result = True
       break
       
    # conn.commit()
    return result
    
def __load_comment_lite(conn, vk_id, comment: vk.VKComment) -> bool:
    result = False
    cur = conn.cursor()
    cur.execute(f"SELECT id, vk_id, vk_from_id, post_id, community_id \
                FROM comments WHERE vk_id = %s", (vk_id, ) )
    rows = cur.fetchall()
    for row in rows:
       item = vk.VKComment()
       item.id = row[0]
       item.vk_id = row[1]
       item.vk_from_id = row[2]
       item.post_id = row[3]
       item.community_id = row[4]
       result = True
       break
       
    conn.commit()
    return result

def load_comments(conn, post: vk.VKCommunityPost):
    cur = conn.cursor()
   
    post.comments.clear

    cur.execute(f"SELECT id, vk_id, vk_from_id, date, text, reply_to_user, reply_to_comment, post_id, community_id, last_update \
                FROM public.comments WHERE post_id = %s", (post.id, ) )
    rows = cur.fetchall()
    for row in rows:
       item = vk.VKComment()
       item.id = row[0]
       item.vk_id = row[1]
       item.vk_from_id = row[2]
       item.date = row[3]
       item.text = row[4]
       item.reply_to_user = row[5]
       item.reply_to_comment = row[6]
       item.post_id = row[7]
       item.community_id = row[8]
       item.last_update = row[9]
       
       post.comments.append(item)
       
    conn.commit()


def load_comments_lite(conn, post: vk.VKCommunityPost):
    cur = conn.cursor()
   
    post.comments.clear

    cur.execute(f"SELECT id, vk_id, vk_from_id, post_id, community_id FROM public.comments WHERE post_id = %s", (post.id, ) )
    rows = cur.fetchall()
    for row in rows:
       item = vk.VKComment()
       item.id = row[0]
       item.vk_id = row[1]
       item.vk_from_id = row[2]
       item.post_id = row[3]
       item.community_id = row[4]
       
       post.comments.append(item)
       
    conn.commit()
def load_comments_lite(conn, post: vk.VKCommunityPost):
    cur = conn.cursor()
   
    post.comments.clear

    cur.execute(f"SELECT id, vk_id, vk_from_id, post_id FROM public.comments WHERE post_id = {post.id}")
    rows = cur.fetchall()
    for row in rows:
       item = vk.VKComment()
       item.id = row[0]
       item.vk_id = row[1]
       item.vk_from_id = row[2]
       item.post_id = row[3]
       
       post.comments.append(item)
       
    conn.commit()
    
def load_comments_count(conn, community_id: int, post_id: int) -> int:
    cursor = conn.cursor()
   
    result = 0
    try:
        cursor.execute(f"SELECT COUNT(*) FROM public.comments c, public.posts p WHERE p.id = c.post_id and c.post_id = {post_id} and p.community_id = {community_id}")
        rows = cursor.fetchall()
        if len(rows) > 0:
            row = rows[0]
            result = row[0]
       
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        conn.rollback
    finally:
        cursor.close()
        
    return result
    
def SN(value: str) -> str:
    if value is None:
        return None
    
    if value.strip() == "":
        return None
    else:
        return value.strip()
   
def create_user(conn, user: vk.VKUser):
    cursor = conn.cursor()
    
    try:
        sql = "INSERT INTO users \
                    (first_name, last_name, middle_name, vk_city_id, vk_country_id, date_of_birth, vk_num_id, vk_str_id, photo, vk_sex_id, is_hidden, json_data) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        json_data = json.dumps(user.json_data)
        values = (user.first_name, user.last_name, SN(user.middle_name), user.vk_city_id, user.vk_country_id, user.date_of_birth, user.vk_num_id, user.vk_num_str, 
                  psycopg2.Binary(user.photo), user.vk_sex_id, user.is_hidden, json_data)
        
        cursor.execute(sql, values)
        
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"! Ошибка. Пользователь {user.vk_num_id}. {error}\n")
        conn.rollback
    finally:
        cursor.close()

# def load_user(conn, vk_num_id: int, user: vk.VKUser):
#     cur = conn.cursor()
    
#     cur.execute(f"SELECT first_name, last_name, middle_name, city_id, country_id, date_of_birth, vk_num_id, vk_str_id, photo \
#                 FROM users \
#                 WHERE vk_num_id = {vk_num_id}")
    
#     conn.commit()
    
#     pass


def create_countries(conn, countries):
    sql = 'INSERT INTO countries(vk_id, name) VALUES (%s, %s)'    
    try:
        cursor = conn.cursor()
        
        values = []
        for country in countries:
            row = (country.vk_id, country.name)
            values.append(row)
            
        cursor.executemany(sql, values)
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        conn.rollback
    finally:
        cursor.close()

def create_cities(conn, cities):
    sql = 'INSERT INTO cities(vk_id, name, area, region) VALUES (%s, %s, %s, %s)'    
    try:
        cursor = conn.cursor()
        
        values = []
        for city in cities:
            row = (city.vk_id, city.name, city.area, city.region)
            values.append(row)
            
        cursor.executemany(sql, values)
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        conn.rollback
    finally:
        cursor.close()

        
def load_all_cities_codes(conn, codes):
    sql = 'SELECT vk_id, name FROM cities'    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
           vk_id = row[0]
           name = row[1]
           codes[vk_id] = name
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        conn.rollback
    finally:
        cursor.close()


def check_user(conn, vk_id) -> bool:
    sql = f"SELECT id FROM users WHERE vk_num_id = {vk_id}"   
    try:
        result = False
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) > 0:
            result = True
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        conn.rollback
    finally:
        cursor.close()
    return result


def save_lastupdate_for_community(conn, community_id: int, last_update: datetime):    
    try:
        cursor = conn.cursor()        
        cursor.execute(f'UPDATE communities SET last_update = %s WHERE id = %s', (last_update, community_id) )        
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error:
        log_db_error(error)
        conn.rollback
    finally:
        cursor.close()

def load_post_by_id(conn, community_id: int, post_id: int, post: vk.VKCommunityPost):
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT id, community_id, vk_id, vk_owner_id, vk_from_id, vk_created_by, vk_signer_id, date, text, last_update \
                FROM posts \
                WHERE (community_id = {community_id}) and (id = {post_id})")
    
    rows = cursor.fetchall()
    row = rows[0]
    post.id = row[0]
    post.community_id = row[1]
    post.vk_id = row[2]
    post.vk_owner_id = row[3]
    post.vk_from_id = row[4]
    post.vk_created_by = row[5]
    post.vk_signer_id = row[6]
    post.date = row[7]
    post.text = row[8]
    post.last_update = row[9]
       
    conn.commit()    
