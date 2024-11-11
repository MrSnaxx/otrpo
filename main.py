import requests
import json
import argparse


# Функция для обращения к VK API
def vk_request(method, params):
    url = f'https://api.vk.com/method/{method}'
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


# Функция для получения информации о пользователе
def get_user_info(user_id, token):
    params = {
        'user_ids': user_id,
        'fields': 'followers_count',
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('users.get', params)


# Функция для получения подписчиков
def get_followers(user_id, token):
    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('friends.get', params)


# Функция для получения подписок
def get_subscriptions(user_id, token):
    params = {
        'user_id': user_id,
        'extended': 1,
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('users.getSubscriptions', params)


# Функция для получения групп
def get_groups(user_id, token):
    params = {
        'user_id': user_id,
        'extended': 1,
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('groups.get', params)


# Функция для преобразования имени пользователя в числовой id
def get_numeric_user_id(username, token):
    params = {
        'user_ids': username,
        'access_token': token,
        'v': '5.131'
    }
    response = vk_request('users.get', params)
    return response['response'][0]['id']


# Основная функция с проверкой числового user_id
def main(user_id, output_path, token):
    if not user_id.isdigit():  # Если user_id не число, получаем числовой id
        user_id = get_numeric_user_id(user_id, token)

    data = {}
    data['user'] = get_user_info(user_id, token)
    data['followers'] = get_followers(user_id, token)
    data['subscriptions'] = get_subscriptions(user_id, token)
    data['groups'] = get_groups(user_id, token)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# Парсинг аргументов командной строки
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VK User Info Collector")
    parser.add_argument("--user_id", type=str, default="burenyu", help="ID пользователя VK")
    parser.add_argument("--output_path", type=str, default="vk_user_info.json", help="Путь к файлу результата")
    parser.add_argument("--vk_token", type=str, default="", help="ВК Токен")
    args = parser.parse_args()


    main(args.user_id, args.output_path, args.vk_token)
