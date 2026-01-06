# app.py
from datetime import datetime  # <--- ИСПРАВЛЕНО
import io
import csv
from flask import Flask, request, jsonify, render_template, send_file, make_response
import requests
import config
import db_manager as db

app = Flask(__name__, template_folder='templates')

# --- Telegram API Helper ---
def send_telegram_message(chat_id, text):
    if not config.BOT_TOKEN or config.BOT_TOKEN == "ВАШ_ТОКЕН_БОТА":
        print(f"--- SIMULATION: Sending to {chat_id} ---\n{text}\n---")
        return True
        
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
        return response.json()
    except Exception as e:
        print(f"Error sending: {e}")
        return None

# --- API User (Mini App) ---
@app.route('/api/get_regions', methods=['GET'])
def api_get_regions():
    regions = db.get_regions()
    return jsonify(regions)

@app.route('/api/subscribe', methods=['POST'])
def api_subscribe():
    try:
        data = request.json
        user_id = data.get('user_id')
        first_name = data.get('first_name')
        username = data.get('username')
        comment = data.get('comment')
        region_id = data.get('region_id')

        if not user_id:
            return jsonify({"status": "error", "message": "User ID missing"}), 400

        # Получаем название региона для уведомления (безопасно)
        region_name = "Не указано"
        if region_id:
            regions = db.get_regions()
            # Ищем строку где ID совпадает (приводим к int для надежности)
            found = next((r for r in regions if int(r['id']) == int(region_id)), None)
            if found: region_name = found['name']

        # Сохраняем пользователя
        save_success = db.save_user(
            user_id=user_id, 
            first_name=first_name, 
            username=username, 
            comment=comment, 
            region_id=region_id
        )

        if save_success:
            admin_msg = (f"✅ Новая заявка!\n"
                         f"От: {first_name} (@{username})\n"
                         f"ID: {user_id}\n"
                         f"Район: {region_name}\n"
                         f"Текст: {comment}")
            
            # Ответ пользователю
            send_telegram_message(user_id, f"✅ Заявка принята! Район: {region_name}. Менеджер свяжется.")
            
            # Лог для админа
            db.save_broadcast(admin_msg, 'admin_notify', [user_id])
            
            return jsonify({"status": "success"})
        else:
            return jsonify({"status": "error", "message": "DB error"}), 500

    except Exception as e:
        print(f"Subscribe Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- API Admin: Login & Password ---
@app.route('/api/admin/login', methods=['POST'])
def admin_login():
    data = request.json
    password = data.get('password')
    if not password: return jsonify({"status": "error"}), 400

    entered_hash = db.hash_password(password)

    if config.ADMIN_PASSWORD_HASH is None:
        config.ADMIN_PASSWORD_HASH = entered_hash
        return jsonify({"status": "new_password_set"})

    if entered_hash == config.ADMIN_PASSWORD_HASH:
        return jsonify({"status": "success"})
    
    return jsonify({"status": "error"}), 401

@app.route('/api/admin/change_password', methods=['POST'])
def admin_change_password():
    data = request.json
    old = data.get('old_password')
    new = data.get('new_password')
    if not old or not new: return jsonify({"status": "error"}), 400
    
    if db.hash_password(old) == config.ADMIN_PASSWORD_HASH:
        config.ADMIN_PASSWORD_HASH = db.hash_password(new)
        return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 401

# --- API Admin: Management ---
@app.route('/api/admin/manage/<table_name>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def admin_manage(table_name):
    allowed_tables = ['users', 'broadcast_log', 'drilling_regions']
    if table_name not in allowed_tables:
        return jsonify({"status": "error", "message": "Invalid table"}), 400
        
    if not config.ADMIN_PASSWORD_HASH:
        return jsonify({"status": "error", "message": "Not logged in"}), 401

    if request.method == 'GET':
        search = request.args.get('search')
        sort_by = request.args.get('sort_by')
        order = request.args.get('order')
        filters = {'search': search, 'sort_by': sort_by, 'order': order}
        data = db.manage_table(table_name, 'get', filters)
        return jsonify(data)

    if request.method == 'DELETE':
        data = request.json
        ids = data.get('ids')
        if not ids: return jsonify({"status": "error"}), 400
        db.manage_table(table_name, 'delete', {'ids': ids})
        return jsonify({"status": "success"})

    if request.method == 'POST':
        # Добавление региона
        if table_name == 'drilling_regions':
            name = request.json.get('name')
            if name:
                success = db.manage_table(table_name, 'insert', {'data': name})
                if success: return jsonify({"status": "success"})
                return jsonify({"status": "error", "message": "Уже существует"}), 400
        
        # Рассылка
        data = request.json
        message = data.get('message')
        target_ids = data.get('target_ids')

        recipients = []
        if target_ids:
            recipients = target_ids
        else:
            all_users = db.manage_table('users', 'get', {})
            recipients = [u['user_id'] for u in all_users]

        if not recipients: return jsonify({"status": "error", "message": "No recipients"}), 400

        sent_count = 0
        for uid in recipients:
            if send_telegram_message(uid, message):
                sent_count += 1
        
        type_str = 'all' if not target_ids else 'specific'
        db.save_broadcast(message, type_str, recipients)
        return jsonify({"status": "success", "sent_to": sent_count})

    if request.method == 'PUT':
        if table_name == 'drilling_regions':
            data = request.json
            row_id = data.get('id')
            name = data.get('name')
            if row_id and name:
                db.manage_table(table_name, 'update', {'id': row_id, 'data': name})
                return jsonify({"status": "success"})
        return jsonify({"status": "error"}), 400

# --- CSV Export ---
@app.route('/api/admin/export/<table_name>', methods=['GET'])
def export_data(table_name):
    if not config.ADMIN_PASSWORD_HASH:
        return jsonify({"error": "Auth required"}), 401

    try:
        search = request.args.get('search')
        sort_by = request.args.get('sort_by')
        order = request.args.get('order')
        filters = {'search': search, 'sort_by': sort_by, 'order': order}
        data = db.manage_table(table_name, 'get', filters)
        
        if not data: return make_response("No data")

        output = io.StringIO()
        fieldnames = data[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=';', restval='')
        writer.writeheader()
        writer.writerows(data)
        
        output.seek(0)
        csv_bytes = io.BytesIO(output.getvalue().encode('utf-8-sig'))
        
        # --- ИСПРАВЛЕННАЯ СТРОКА ---
        filename = f'export_{table_name}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        
        response = make_response(send_file(csv_bytes, mimetype='text/csv; charset=utf-8', as_attachment=True, download_name=filename))
        return response
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def index(): return render_template('index.html')
@app.route('/admin')
def admin(): return render_template('admin.html')

if __name__ == '__main__':
    db.init_dbs()
    app.run(host='0.0.0.0', port=8000, debug=True)