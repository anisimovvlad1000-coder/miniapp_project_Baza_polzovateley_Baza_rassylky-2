# db_manager.py
import sqlite3
import hashlib
import os

DB_MAIN = "miniapp.db"
DB_BROADCAST = "broadcast.db"

def get_conn(db_file):
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    return conn

def init_dbs():
    conn_main = get_conn(DB_MAIN)
    cursor = conn_main.cursor()

    # 1. Таблица регионов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS drilling_regions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')

    # 2. Таблица пользователей (Проверка схемы)
    # Сначала проверим, есть ли старая таблица без region_id и удалим её для пересоздания
    # Это критично для учебного проекта, чтобы поле добавилось точно
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        # Проверяем наличие колонки region_id
        cursor.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'region_id' not in columns:
            # Если колонки нет, удаляем таблицу для пересоздания
            cursor.execute("DROP TABLE users")
            table_exists = False

    if not table_exists:
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                first_name TEXT,
                username TEXT,
                comment TEXT,
                region_id INTEGER,
                subscribe_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(region_id) REFERENCES drilling_regions(id)
            )
        ''')
    conn_main.commit()
    conn_main.close()

    # 3. База рассылок
    conn_bcast = get_conn(DB_BROADCAST)
    conn_bcast.execute('''
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            recipient_type TEXT,
            user_ids TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn_bcast.commit()
    conn_bcast.close()

def save_user(user_id, first_name, username, comment, region_id=None):
    try:
        conn = get_conn(DB_MAIN)
        cursor = conn.cursor()
        
        # БЕЗОПАСНОЕ ПРЕОБРАЗОВАНИЕ
        r_id = None
        if region_id and str(region_id).isdigit():
            r_id = int(region_id)

        cursor.execute('''
            INSERT INTO users (user_id, first_name, username, comment, region_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            first_name=excluded.first_name, username=excluded.username, 
            comment=excluded.comment, region_id=excluded.region_id
        ''', (user_id, first_name, username, comment, r_id))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"DB Error save_user: {e}")
        return False

def save_broadcast(message, recipient_type, user_ids):
    try:
        conn = get_conn(DB_BROADCAST)
        # Сохраняем ID как строку через запятую для совместимости
        if isinstance(user_ids, list):
            ids_str = ",".join(map(str, user_ids))
        else:
            ids_str = str(user_ids)
            
        conn.execute('''
            INSERT INTO broadcast_log (message, recipient_type, user_ids)
            VALUES (?, ?, ?)
        ''', (message, recipient_type, ids_str))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"DB Error save_broadcast: {e}")
        return False

def manage_table(table_name, action, filters=None):
    db_file = DB_MAIN if table_name in ['users', 'drilling_regions'] else DB_BROADCAST
    conn = get_conn(db_file)
    result = []
    
    try:
        if action == 'get':
            if table_name == 'users':
                query = '''
                    SELECT u.*, dr.name as region_name 
                    FROM users u 
                    LEFT JOIN drilling_regions dr ON u.region_id = dr.id
                '''
                params = []
                if filters and filters.get('search'):
                    search_term = f"%{filters['search']}%"
                    query += " WHERE (u.user_id LIKE ? OR u.first_name LIKE ? OR u.username LIKE ? OR u.comment LIKE ? OR dr.name LIKE ?)"
                    params.extend([search_term] * 5)
                
                sort_by = 'u.id'
                order = 'DESC'
                if filters:
                    if filters.get('sort_by') and filters.get('sort_by') in ['id', 'user_id', 'first_name', 'subscribe_date']:
                        sort_by = f"u.{filters.get('sort_by')}"
                    if filters.get('order'): order = filters.get('order')
                query += f" ORDER BY {sort_by} {order}"

            elif table_name == 'drilling_regions':
                query = "SELECT * FROM drilling_regions ORDER BY name ASC"
                params = []

            else: # broadcast_log
                query = f"SELECT * FROM {table_name}"
                params = []
                if filters and filters.get('search'):
                    search_term = f"%{filters['search']}%"
                    query += " WHERE message LIKE ? OR user_ids LIKE ?"
                    params.extend([search_term] * 2)
                sort_by = filters.get('sort_by', 'id')
                order = filters.get('order', 'DESC')
                query += f" ORDER BY {sort_by} {order}"

            cursor = conn.cursor()
            if params: cursor.execute(query, params)
            else: cursor.execute(query)
            rows = cursor.fetchall()
            result = [dict(row) for row in rows]

        elif action == 'delete':
            ids = filters.get('ids', [])
            if ids:
                placeholders = ','.join(['?'] * len(ids))
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM {table_name} WHERE id IN ({placeholders})", ids)
                conn.commit()
                result = True

        elif action == 'insert':
            data = filters.get('data')
            if table_name == 'drilling_regions' and data:
                try:
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO drilling_regions (name) VALUES (?)", (data,))
                    conn.commit()
                    result = True
                except sqlite3.IntegrityError:
                    result = False
        
        elif action == 'update':
            data = filters.get('data')
            row_id = filters.get('id')
            if table_name == 'drilling_regions' and data and row_id:
                cursor = conn.cursor()
                cursor.execute("UPDATE drilling_regions SET name = ? WHERE id = ?", (data, row_id))
                conn.commit()
                result = True

    except Exception as e:
        print(f"DB Manager Error ({action}/{table_name}): {e}")
    finally:
        conn.close()
    
    return result

def get_regions():
    try:
        conn = get_conn(DB_MAIN)
        cursor = conn.execute("SELECT * FROM drilling_regions ORDER BY name ASC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except:
        return []

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()