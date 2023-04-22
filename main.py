import requests
import json
import time
from datetime import date, timedelta

# Путь к файлу txt c данными API:
API_DATA_FILE = 'D:\Dropbox\Python\API_data.txt'


#  Функция возвращает словарь с данными API из текстового файла
def get_api_data(file, *args):
    with open(file, 'r', encoding='utf-8') as apidata:
        apidata = apidata.readlines()
    res_data = {}
    for requested_data in args:
        for apidata_line in apidata:
            if requested_data in apidata_line:
                res_data[requested_data] = apidata_line.replace(requested_data+':', '').replace(' ', '').replace('\n', '')
                break
        else:
            print(f'[!] В файле {file} не найдены данные для {requested_data}. Каждое значение должно быть записано с новой строки в формате: "индекс: значение"')

    return(res_data)


# API links:
MS_DEMAND = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'  # Отгрузки
MS_PRODUCTS = 'https://online.moysklad.ru/api/remap/1.2/entity/product'  # Товары
MS_BUNDLES = 'https://online.moysklad.ru/api/remap/1.2/entity/bundle' # Наборы
MS_HEADER = {'Authorization': 'Bearer ' + get_api_data(API_DATA_FILE, 'Moy_Sklad_Token')['Moy_Sklad_Token']}
INSALES_LINK = get_api_data(API_DATA_FILE, 'Insales_API_link')['Insales_API_link'] + '@www.zeero.ru/admin/'


#  Function returns dict of products from "Insales"
#  key = Insales ID, value{'sku' = SKU}
def get_products_insales(skip):
    res = {}
    page = 1
    while True:
        print(f'[.] Выгружаю артикулы товаров из Insales. Обработано {(page - 1) * 100} товаров')
        url = f'{INSALES_LINK}products.json?per_page=100&page={page}'
        response = requests.get(url)
        data = json.loads(response.text)
        if len(data) == 0: break
        for row in data:
            if not row['variants'][0]['sku']: continue
            sku = row['variants'][0]['sku']
            if skip in sku:
                print('[i] Пропущен артикул', sku)
                continue
            res[row['id']] = {'sku':sku, 'old_orders':row['sort_weight']}
        page += 1
    return res


#  Function adds MoySklad ID
def add_moysklad_id(product_list):
    #  Выгружаем товары из МС
    url = MS_PRODUCTS
    params = {'filter': 'archived=false'}
    response = requests.get(url, headers=MS_HEADER, params=params)
    data = json.loads(response.text)
    products = data['rows']
    total_products = data['meta']['size']
    while len(products) < total_products:
        params['offset'] = len(products)
        response = requests.get(url, headers=MS_HEADER, params=params)
        data = json.loads(response.text)
        products += data['rows']

    #  Выгружаем наборы из МС
    url = MS_BUNDLES
    params = {'filter': 'archived=false'}
    response = requests.get(url, headers=MS_HEADER, params=params)
    data = json.loads(response.text)
    bundles = data['rows']
    total_bundles = data['meta']['size']
    while len(bundles) < total_bundles:
        params['offset'] = len(bundles)
        response = requests.get(url, headers=MS_HEADER, params=params)
        data = json.loads(response.text)
        bundles += data['rows']

    #  Добавляем наборы в список товаров
    products.extend(bundles)

    # Формируем словарь {sku: ms_id}
    ms_products = {}
    for product in products:
        if 'article' not in product: continue
        sku = product['article']
        if sku in ms_products:
            print(f'[!] Артикул {sku} встречается в Мой Склад более 1 раза')
        ms_products[sku] = product['id']

    # Добавляем в основной словарь поле 'ms_id'
    for product_info in product_list.values():
        sku = product_info['sku']
        if sku not in ms_products: continue
        product_info['ms_id'] = ms_products[sku]

    return product_list


#  Добавляем в словарь количество отгрузок товара из МС
def add_demands_count(products, start, end):
    counter = 0

    for product in products.values():
        counter += 1
        if 'ms_id' not in product: continue
        print(f'[.] Считаю продажи товара {counter} из {len(products)}')
        product_id = product['ms_id']
        url = f'{MS_DEMAND}?filter=moment>{start};moment<{end};assortment=https://online.moysklad.ru/api/remap/1.2/entity/product/{product_id}'
        response = requests.get(url, headers=MS_HEADER)
        orders = len(json.loads(response.text)['rows'])
        product['orders'] = orders


    return products


#  Выгружаем данные о продажах в Инсейлс
def update_popularity(products):
    count = 0
    api_request_counter = 0
    for id, product_info in products.items():
        count += 1
        if 'orders' not in product_info:
            orders = 0
        else:
            orders = product_info['orders']
        if orders == product_info['old_orders']: continue
        print(f'[.] Выгружаю товар {count} из {len(products)}')
        api_request_counter += 1
        if api_request_counter > 400:
            print('[.] Пауза 60 сек, превышено кол-во запросов')
            time.sleep(60)
            api_request_counter = 0
        body = {"product": {"sort_weight": orders}}
        url = f'{INSALES_LINK}products/{id}.json'
        response = requests.put(url, json=body)
        if response.status_code != 200:
            print (f'Что-то пошло не так. Код ответа: {response.status_code}\nЗаголовок:{response.headers}\nТекст:{response.text}')

    return


period_days = 180  # период за который считаем популярность
popular_collection_insales_id = 8006499 # id коллекции "Популярные товары" в Инсейлс
skip_sku = 'ZR99' # sku, содержащие эту строку не будут обрабатываться

end_date = date.today()
start_day = end_date - timedelta(days=period_days)

start = start_day.strftime('%Y-%m-%d 00:00:00')
end = end_date.strftime('%Y-%m-%d 23:59:59')

products = get_products_insales(skip_sku)

print(f'[+] Выгружено {len(products)} артикулов\n')
print('[+] Сращиваю артикулы с Мой Склад\n')
products = add_moysklad_id(products)
print(f'\n[+] Считаю продажи за {period_days} дней c {start} по {end}\n')
products = add_demands_count(products, start, end)
print('\n[+] Выгружаю количество продаж в Инсейлс\n')
with open("log.txt", "w") as file:
    json.dump(products, file)

update_popularity(products)

# TODO: Добавление/удаление в категорию Популярное - первые 120 товаров
# TODO: Вывод в консоль после выполнения 10 самых популярных с индексом
