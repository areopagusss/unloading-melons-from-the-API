import requests
import json
from datetime import datetime
import psycopg2


# Функция для выполнения запроса к API и обработки данных
def fetch_and_process_data(api_url, date_from, flag):
    headers = {'HeaderApiKey': ''}

    params = {'dateFrom': date_from, 'flag': flag}
    try:
        with requests.get(api_url, headers=headers, params=params) as response:
            response.raise_for_status()  # Проверка на наличие ошибок в ответе
            data = response.json()

            if data:
                # Обработка данных
                for order in data:
                    process_order(order)

                return data
            else:
                print("Error: Empty response data")
                return None
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None


# Функция для обработки отдельного заказа
def process_order(order):
    # Обработка данных
    print(f"Processing order: {order.get('srid', 'Unknown')}")


# Часть 2: Код для записи данных в БД PostgreSQL

# Конфигурация подключения к БД
db_config = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': '',
}


# Функция для записи данных в БД
def write_to_database(data):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Проверка наличия таблицы
        table_exists_query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders');"
        cursor.execute(table_exists_query)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # SQL-запрос для создания таблицы (закомментирован)
            create_table_query = """
                CREATE TABLE IF NOT EXISTS orders (
                    date TIMESTAMP,
                    last_change_date TIMESTAMP,
                    warehouse_name VARCHAR(50),
                    country_name VARCHAR(200),
                    oblast_okrug_name VARCHAR(200),
                    region_name VARCHAR(200),
                    supplier_article VARCHAR(75),
                    nm_id INTEGER,
                    barcode VARCHAR(30),
                    category VARCHAR(50),
                    subject VARCHAR(50),
                    brand VARCHAR(50),
                    tech_size VARCHAR(30),
                    income_id INTEGER,
                    is_supply BOOLEAN,
                    is_realization BOOLEAN,
                    total_price NUMERIC,
                    discount_percent INTEGER,
                    spp NUMERIC,
                    finished_price NUMERIC,
                    price_with_disc NUMERIC,
                    is_cancel BOOLEAN,
                    cancel_date TIMESTAMP,
                    order_type VARCHAR,
                    sticker VARCHAR,
                    g_number VARCHAR(50),
                    srid VARCHAR
                );
            """
            cursor.execute(create_table_query)
            logger.info("Table 'orders' created.")

        # Пример SQL-запроса для вставки данных в таблицу
        insert_query = """
            INSERT INTO orders (
                date, last_change_date, warehouse_name, country_name, oblast_okrug_name,
                region_name, supplier_article, nm_id, barcode, category, subject, brand,
                tech_size, income_id, is_supply, is_realization, total_price, discount_percent,
                spp, finished_price, price_with_disc, is_cancel, cancel_date, order_type,
                sticker, g_number, srid
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            );
        """

        # Пример вставки данных в таблицу
        # for order in data:
        #     cursor.execute(insert_query, (
        #         order['date'], order['lastChangeDate'], order['warehouseName'], order['countryName'],
        #         order['oblastOkrugName'], order['regionName'], order['supplierArticle'], order['nmId'],
        #         order['barcode'], order['category'], order['subject'], order['brand'], order['techSize'],
        #         order['incomeID'], order['isSupply'], order['isRealization'], order['totalPrice'],
        #         order['discountPercent'], order['spp'], order['finishedPrice'], order['priceWithDisc'],
        #         order['isCancel'], order['cancelDate'], order['orderType'], order['sticker'],
        #         order['gNumber'], order['srid']
        #     ))

        # Подтверждение изменений и закрытие соединения
        connection.commit()
        logger.info("Data successfully written to the database.")
    except psycopg2.Error as e:
        logger.error(f"Error: {e}")
    finally:
        if connection:
            connection.close()
