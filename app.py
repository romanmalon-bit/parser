# app.py
from flask import Flask
import threading
import time
import os
from parser import main as run_parser  # ← тут імпортуємо твою функцію парсингу

app = Flask(__name__)

# Функція, яка запускає парсер кожні 3 години
def scheduled_parser():
    while True:
        run_parser()                    # ← тут запускається твій парсинг
        print("Парсинг завершено, чекаю 3 години...")
        time.sleep(3 * 60 * 60)         # 3 години = 10800 секунд

@app.route('/')
def home():
    return "Парсер живий! Останній запуск: " + time.strftime("%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':
    # Запускаємо таймер у окремому потоці, щоб веб-сервер не блокувався
    threading.Thread(target=scheduled_parser, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)