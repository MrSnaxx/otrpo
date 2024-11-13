import asyncio
import aiohttp
import logging
from neo4j import GraphDatabase
import argparse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Подключение к Neo4j
NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678910"
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# Запрос к API VK
async def handle_captcha(captcha_img_url):
    print(f"Откройте изображение капчи: {captcha_img_url}")
    captcha_key = input("Введите текст с капчи: ")
    return captcha_key


# Запрос к API VK с обработкой капчи
async def vk_request_async(method, params):
    url = f'https://api.vk.com/method/{method}'
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        await asyncio.sleep(0.5)
        try:
            async with session.get(url, params=params) as response:
                data = await response.json()
                if 'error' in data:
                    error_code = data['error'].get('error_code')
                    if error_code == 14:  # Капча
                        captcha_sid = data['error']['captcha_sid']
                        captcha_img = data['error']['captcha_img']
                        logger.warning(f"Необходима капча: {captcha_img}")
                        captcha_key = await handle_captcha(captcha_img)

                        # Повторный запрос с капчей
                        params['captcha_sid'] = captcha_sid
                        params['captcha_key'] = captcha_key
                        async with session.get(url, params=params) as retry_response:
                            retry_data = await retry_response.json()
                            if 'error' in retry_data:
                                logger.error(f"VK API Error after retry: {retry_data['error']}")
                            return retry_data
                    else:
                        logger.error(f"VK API Error: {data['error']}")
                        return None
                return data
        except Exception as e:
            logger.error(f"Ошибка при запросе к API VK: {e}")
            return None


# Функции для работы с базой данных Neo4j
def save_user(tx, user):
    tx.run(
        """
        MERGE (u:User {id: $id})
        SET u.screen_name = $screen_name, u.name = $name, u.sex = $sex, 
            u.home_town = COALESCE($home_town, $city)
        """,
        id=user['id'], screen_name=user.get('screen_name'),
        name=f"{user['first_name']} {user['last_name']}",
        sex=user.get('sex'), home_town=user.get('home_town'),
        city=user.get('city', {}).get('title')
    )


def save_follow_relation(tx, user_id, follower_id):
    tx.run(
        """
        MATCH (u1:User {id: $user_id})
        MATCH (u2:User {id: $follower_id})
        MERGE (u2)-[:Follow]->(u1)
        """,
        user_id=user_id, follower_id=follower_id
    )


# Получаем подписчиков пользователя
async def get_followers(user_id, token):
    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131',
        'count': 30
    }
    followers_data = await vk_request_async('users.getFollowers', params)
    return followers_data.get('response', {}).get('items', [])


async def get_friends(user_id, token):
    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131',
        'count': 30
    }
    followers_data = await vk_request_async('friends.get', params)
    return followers_data.get('response', {}).get('items', [])


# Получаем подписки пользователя
async def get_subscriptions(user_id, token):
    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131',
        'extended': 1,  # Получаем расширенную информацию о подписках
        'fields': 'screen_name,name',
        'count': 200
    }
    subscriptions_data = await vk_request_async('users.getSubscriptions', params)
    return subscriptions_data.get('response', {}).get('items', [])


# Сбор данных с использованием асинхронных запросов
async def collect_data_async(user_id, token, session, depth=2):
    if depth < 0:
        return

    user_data = await get_user_info(user_id, token)
    if not user_data:
        logger.error(f"Не удалось получить данные пользователя: {user_id}")
        return

    session.write_transaction(save_user, user_data)

    followers = await get_followers(user_id, token)
    subscriptions = await get_subscriptions(user_id, token)

    tasks = []
    for follower_id in followers:
        session.write_transaction(save_follow_relation, user_id, follower_id)
        tasks.append(collect_data_async(follower_id, token, session, depth - 1))

    for subscription in subscriptions:
        session.write_transaction(save_subscription_relation, user_id, subscription)

    await asyncio.gather(*tasks)


def save_subscription_relation(tx, user_id, subscription):
    sub_id = subscription['id']
    sub_name = subscription.get('name',
                                f"{subscription.get('first_name', '')} {subscription.get('last_name', '')}".strip())
    sub_screen_name = subscription.get('screen_name')
    sub_type = subscription.get('type')

    if sub_type == 'group':
        query = """
        MERGE (g:Group {id: $sub_id})
        SET g.name = $sub_name, g.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(g)
        """
    elif sub_type == 'page':
        query = """
        MERGE (p:Page {id: $sub_id})
        SET p.name = $sub_name, p.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(p)
        """
    elif sub_type == 'event':
        query = """
        MERGE (e:Event {id: $sub_id})
        SET e.name = $sub_name, e.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(e)
        """
    elif sub_type == 'profile':
        query = """
        MERGE (p:User {id: $sub_id})
        SET p.name = $sub_name, p.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(p)
        """
    else:
        logger.warning(f"Неизвестный тип подписки: {sub_type}")
        return

    # Выполняем запрос
    tx.run(query, user_id=user_id, sub_id=sub_id, sub_name=sub_name, sub_screen_name=sub_screen_name)


# Получаем информацию о пользователе
async def get_user_info(user_id, token):
    params = {
        'user_ids': user_id,
        'access_token': token,
        'v': '5.131',
        'fields': 'screen_name,first_name,last_name,sex,home_town,city'
    }
    user_info = await vk_request_async('users.get', params)
    return user_info.get('response', [{}])[0]


# Запросы для выборки данных из базы
def get_all_users(tx):
    return list(tx.run("MATCH (u:User) RETURN COUNT(u) AS user_count"))


def get_all_groups(tx):
    return list(tx.run("MATCH (g:Group) RETURN COUNT(g) AS group_count"))


def get_top_users_by_followers(tx):
    return list(tx.run("""
        MATCH (u:User)<-[:Follow]-(f:User)
WITH u, COUNT(f) AS followers_count
ORDER BY followers_count DESC
LIMIT 5
RETURN u.id AS user_id, u.name AS user_name, followers_count;

    """))


def get_top_groups_by_followers(tx):
    return list(tx.run("""
        MATCH (g:Group)<-[:Subscribe]-(u:User)
WITH g, COUNT(u) AS subscribers_count
ORDER BY subscribers_count DESC
LIMIT 5
RETURN g.id AS group_id, g.name AS group_name, subscribers_count;

    """))


def get_mutual_followers(tx):
    return list(tx.run("""
        MATCH (u1:User)-[:Follow]->(u2:User), (u2)-[:Follow]->(u1)
RETURN u1.id AS user1_id, u1.name AS user1_name, u2.id AS user2_id, u2.name AS user2_name;

    """))


async def get_numeric_user_id(username, token):
    params = {
        'user_ids': username,
        'access_token': token,
        'v': '5.131'
    }
    response = await vk_request_async('users.get', params)
    return response['response'][0]['id']


# Главная функция
async def main(args):
    token = args.token
    user_id = await get_numeric_user_id(args.username, token)
    logger.info(f"Numeric User ID for {args.username}: {user_id}")

    if args.generate:  # Если указано, что нужно генерировать данные
        logger.info("Начинаем генерацию данных...")
        with driver.session() as session:
            await collect_data_async(user_id, token, session, depth=args.depth)

    # Выполнение выборки
    with driver.session() as session:
        if args.query == "all_users":
            users = session.read_transaction(get_all_users)
            logger.info(f"All Users: {users}")
        elif args.query == "all_groups":
            groups = session.read_transaction(get_all_groups)
            logger.info(f"All Groups: {groups}")
        elif args.query == "top_users":
            top_users = session.read_transaction(get_top_users_by_followers)
            logger.info(f"Top 5 Users by Followers: {top_users}")
        elif args.query == "top_groups":
            top_groups = session.read_transaction(get_top_groups_by_followers)
            logger.info(f"Top 5 Groups by Followers: {top_groups}")
        elif args.query == "mutual_followers":
            mutual_followers = session.read_transaction(get_mutual_followers)
            logger.info(f"Mutual Followers: {mutual_followers}")
        else:
            logger.warning("Unknown query type.")


# Параметризация через аргументы командной строки
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VK Data Collection Script")
    parser.add_argument("username", type=str, default="burenyu", help="VK username")
    parser.add_argument("token", type=str, help="VK access token")
    parser.add_argument("--depth", type=int, default=2, help="Depth of the user collection")
    parser.add_argument("--query", type=str,
                        choices=["all_users", "all_groups", "top_users", "top_groups", "mutual_followers"],
                        default=None, help="Query type to execute (defaults to None, no query will run)")
    parser.add_argument("--generate", action="store_true", help="Generate data (default is False)")

    args = parser.parse_args()
    asyncio.run(main(args))
