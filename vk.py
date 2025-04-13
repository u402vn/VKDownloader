from enum import Enum
from datetime import date
from vk_utils import null_date

class VKCountry:
        def __init__(self):
            self.id = 0
            self.vk_id = 0
            self.name = ""
  
class VKCity:
        def __init__(self):
            self.id = 0
            # Идентификатор города
            self.vk_id = 0
            # Название города
            self.name = ""
            # Область
            self.area = ""
            # Регион
            self.region = ""

class VKCommentThread:
        def __init__(self):
            # количество комментариев в ветке
            self.count = 0
            # массив объектов комментариев к записи (только для метода wall.getComments)
            self.items = []
            # может ли текущий пользователь оставлять комментарии в этой ветке
            self.can_post = False
            # нужно ли отображать кнопку «ответить» в ветке
            self.show_reply_button = False
            # могут ли сообщества оставлять комментарии в ветке
            self.groups_can_post = False

class VKComment:
        def __init__(self):
            self.id = 0
            self.post_id = 0
            self.community_id = 0
            self.last_update = null_date
            # Идентификатор комментария
            self.vk_id = 0
            # Идентификатор автора комментария
            self.vk_from_id = 0
            # Дата создания комментария в формате Unixtime
            self.date = null_date
            # Текст комментария
            self.text = ""
            # Идентификатор пользователя или сообщества, в ответ которому оставлен текущий комментарий (если применимо)
            self.reply_to_user = 0
            # Идентификатор комментария, в ответ на который оставлен текущий (если применимо)
            self.reply_to_comment = 0
            # Массив идентификаторов родительских комментариев
            self.parents_stack = []
            # Информация о вложенной ветке комментариев, объект с полями
            self.thread = None

class VKCommunity:
    def __init__(self):
        self.id = 0
        # self.vk_id = 0
        self.name = ""
        self.posts = []
        self.db_post_count = -1 
        self.last_update = null_date

class VKCommunityPost:
    def __init__(self, community: VKCommunity):
        self.id = 0
        self.community: VKCommunity = None
        self.community_id = 0
        self.vk_id = 0
        self.vk_owner_id = 0
        self.vk_from_id = 0
        self.vk_created_by = 0
        self.vk_signer_id = 0
        self.date = 0
        self.text = ""
        self.comments = []
        self.users = []
        self.last_update = null_date
        # self.vk_comment_count = 0 # временное поле, исполльзуется во время загрузки поста со страницы ВК 
        
    def copy_to(self, dest):
        dest.id = self.id
        dest.community = self.community
        dest.community_id = self.community_id
        dest.vk_id = self.vk_id
        dest.vk_owner_id = self.vk_owner_id
        dest.vk_from_id = self.vk_from_id
        dest.vk_created_by = self.vk_created_by
        dest.vk_signer_id = self.vk_signer_id
        dest.date = self.date
        dest.text = self.text
        dest.comments.clear
        dest.users.clear
        dest.last_update = self.last_update

class VKUser:
    def __init__(self):
        self.id = 0
        self.first_name = ""
        self.last_name = ""
        self.middle_name = ""
        self.maiden_name = "" # Девичья фамилия
        self.vk_city_id = 0
        self.vk_country_id = 0
        self.vk_sex_id = 0
        self.date_of_birth = null_date
        self.vk_num_id = 0
        self.vk_num_str = "" # domain
        self.json_data = ""
        self.photo = None
        self.is_hidden = False
        
AttachmentType = Enum('AttachmentType', ['photo', 'link', 'audio', 'video'])

class VKAttachmentType(Enum):
    PHOTO = 1
    LINK = 2
    AUDIO = 3
    VIDEO = 4
    DOC = 5

def get_attachemnt_type(data):
    if data == "photo":
        return VKAttachmentType.PHOTO
    elif data == "link":
        return VKAttachmentType.LINK
    elif data == "audio":
        return VKAttachmentType.AUDIO
    elif data == "video":
        return VKAttachmentType.VIDEO
    elif data == "doc":
        return VKAttachmentType.DOC
    else:
        raise Exception(f"Неизвестный тип вложения: {data}")
        
class VKPost:
    def __init__(self, post_id, attachment_type, url):
        self.post_id = post_id
        self.attachment_type = VKAttachmentType.get_type(attachment_type)
        self.url = url
      