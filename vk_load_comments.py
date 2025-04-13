import os
import json
from datetime import datetime
from time import sleep
import vk_load
from vk_auth import auth_token
from vk import VKComment, VKCommunity, VKCommunityPost
from vk import VKCommentThread
from vk_utils import check_for_errors_with_exception, check_for_errors

load_comments_count = 100

def get_post_comments(post: VKCommunityPost, comment_id: int) -> str:
    print(f"get_post_comments. Community: {post.community_id}, Post: {post.vk_id}, Comment: {comment_id}")
    if comment_id > 0:
        url = f"https://api.vk.com/method/wall.getComments?owner_id={post.vk_owner_id}&post_id={post.vk_id}&access_token={auth_token}&sort=asc&comment_id={comment_id}&count={load_comments_count}&v=5.199"
    else:
        url = f"https://api.vk.com/method/wall.getComments?owner_id={post.vk_owner_id}&post_id={post.vk_id}&access_token={auth_token}&sort=asc&count={load_comments_count}&v=5.199"

    src = vk_load.load_url_as_json(url)

    return src


def get_post_comments_next(post: VKCommunityPost, comment_id: int, offset: int) -> str:
    print(f"get_post_comments_next. Community: {post.community_id}, Post: {post.vk_id}, Comment: {comment_id}, Offset {offset}")
    if comment_id > 0:
        url = f"https://api.vk.com/method/wall.getComments?owner_id={post.vk_owner_id}&post_id={post.vk_id}&access_token={auth_token}&sort=asc&comment_id={comment_id}&count=10&offset={offset}&v=5.199"
    else:
        url = f"https://api.vk.com/method/wall.getComments?owner_id={post.vk_owner_id}&post_id={post.vk_id}&access_token={auth_token}&sort=asc&count=10&offset={offset}&v=5.199"

    src = vk_load.load_url_as_json(url)

    return src


def parse_thread_info(datas, thread: VKCommentThread):
    thread.count = datas["count"]
    thread.can_post = datas["can_post"]
    thread.show_reply_button = datas["show_reply_button"]

def parse_comments(new_comments, post: VKCommunityPost):

    for comment in new_comments:
        item = VKComment()
        item.post_id = post.id
        item.community_id = post.community_id
        item.vk_id = comment["id"]
        item.vk_from_id = comment["from_id"]
        item.date = datetime.fromtimestamp(comment["date"])
        item.text = comment["text"]
        if "reply_to_user" in comment:
            item.reply_to_user = comment["reply_to_user"]
        if "reply_to_comment" in comment:
            item.reply_to_comment = comment["reply_to_comment"]
        if "parents_stack" in comment:
            item.parents_stack = comment["parents_stack"]

        if "thread" in comment:
            thread = comment["thread"]
            item.thread = VKCommentThread()
            parse_thread_info(thread, item.thread)
            thread_count = item.thread.count
        else:
            thread_count = 0

        post.comments.append(item)


def load_all_post_comments(post: VKCommunityPost, comment_id: int = 0):

    src = get_post_comments(post, comment_id)
    #check_for_errors_with_exception(src)
    error_info = []
    if not check_for_errors(src, error_info):
        return
    sleep(0.1)
    
    datas = src["response"]
    all_count = datas["count"]
    loaded_conmments = datas["items"]
    frame_count = len(loaded_conmments)
    count_before = len(post.comments)
    parse_comments(loaded_conmments, post)

    if frame_count < all_count:
        loaded_count = frame_count
        while loaded_count < all_count:
            src = get_post_comments_next(post, comment_id, loaded_count)
            if comment_id != 0:
                pass
            check_for_errors_with_exception(src)

            sleep(0.1)

            datas = src["response"]
            loaded_conmments = datas["items"]
            frame_count = len(loaded_conmments)
            # if frame_count == 0:
            #     break
            loaded_count = loaded_count + frame_count
            parse_comments(loaded_conmments, post)
            if frame_count < load_comments_count:
                break

    comment_ids = []
    counter = 0
    for comment in post.comments:
        if counter < count_before:
            continue

        if comment.thread is not None:
            if comment.thread.count > 0:
                comment_ids.append(comment.vk_id)
        counter = counter + 1

    for comment_id in comment_ids:
        load_all_post_comments(post, comment_id)


def get_post_comment_count(post: VKCommunityPost) -> int:
    src = get_post_comments(post, 0)
    check_for_errors_with_exception(src)

    datas = src["response"]
    result = datas["count"]
    
    return result
