
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import threading  # Добавлен новый импорт для работы с таймерами

# Настройка логирования для отслеживания ошибок
logging.basicConfig(level=logging.INFO)

def calculate_remaining_days(end_date):

    if pd.isna(end_date):
        return "Нет данных"
    try:
        # Конвертируем дату с учетом российского формата (день.месяц.год)
        end_date = pd.to_datetime(str(end_date).strip(), dayfirst=True, errors='coerce')
        today = datetime.today()
        delta = end_date - today
        
        if delta.days < 0:
            return "Просрочено"
        
        # Рассчитываем месяцы и дни
        months = delta.days // 30
        days = delta.days % 30
        return f"{months} мес. {days} дн."
    except Exception as e:
        logging.error(f"Ошибка при обработке даты: {e}")
        return "Ошибка даты"

    

# Проверка аргументов командной строки
if len(sys.argv) < 2:
    print("Ошибка: необходимо передать код точки.")
    sys.exit(1)

key = sys.argv[1].strip().lower()

# Словарь для соответствия кодов точек и их полных названий
locations = {
    "kievskaya": "Моремания Киевская",
    "dolgoprudny": "Моремания Долгопрудный",
    "dmitrovka": "Моремания Дмитровка",
}

if key not in locations:
    print("Ошибка: указанная точка не найдена.")
    sys.exit(1)

location_name = locations[key]

# Пути к файлам и настройка выходной директории
script_dir = os.path.dirname(__file__)  # Получаем путь к директории скрипта
tables_dir = os.path.join(script_dir, 'tables')  # Папка с таблицами рядом со скриптом

source_file = os.path.join(tables_dir, 'все сотрудники в штате  по моремании.xlsx')
registration_file = os.path.join(tables_dir, 'регистрация.xlsx')
patent_file = os.path.join(tables_dir, 'патент.xlsx')
output_folder = os.path.join(os.getcwd(), 'выгрузки')
os.makedirs(output_folder, exist_ok=True)

try:
    # Чтение основного файла с сотрудниками без заголовков
    df = pd.read_excel(source_file, sheet_name="TDSheet", header=None)
except Exception as e:
    logging.error(f"Ошибка при чтении файла {source_file}: {e}")
    sys.exit(1)

# Поиск позиции точки в таблице
start_idx = df[df[0] == location_name].index
if not start_idx.empty:
    start_idx = start_idx[0] + 1
else:
    logging.error(f"Ошибка: не найдено название точки '{location_name}' в таблице.")
    sys.exit(1)

# Сбор сотрудников до следующего упоминания "Моремания"
employees = []
for i in range(start_idx, len(df)):
    value = df.iloc[i, 0]
    if pd.isna(value):
        continue
    if "Моремания" in str(value):
        break
    if len(str(value).split()) >= 2:
        employees.append(value.strip())

if not employees:
    logging.error("Ошибка: не найден список сотрудников.")
    sys.exit(1)

try:
    # Чтение данных регистраций и патентов
    reg_df = pd.read_excel(registration_file, header=None)
    patent_df = pd.read_excel(patent_file, header=None)
except Exception as e:
    logging.error(f"Ошибка при чтении регистрационных или патентных данных: {e}")
    sys.exit(1)

# Ключевые слова для исключения определенных должностей
EXCLUDE_KEYWORDS = [
    "Клининг", "Помощник", "Эксперт", "Наставник", 
    "Повар", "шеф", "Шеф"
]

output_data = []
for emp in employees:
    try:
        emp = emp.strip()
        # Фильтрация по исключаемым должностям
        if any(excl_word.lower() in emp.lower() for excl_word in EXCLUDE_KEYWORDS):
            continue

        # Поиск в обеих таблицах
        reg_match = reg_df[reg_df[0].astype(str).str.strip() == emp]
        patent_match = patent_df[patent_df[0].astype(str).str.strip() == emp]

        # Пропускаем сотрудников без записей в таблицах
        if reg_match.empty and patent_match.empty:
            continue

        # Извлечение данных из найденных записей
        reg_number = reg_match.iloc[0, 1] if not reg_match.empty and pd.notna(reg_match.iloc[0, 1]) else "Нет данных"
        reg_expiry = reg_match.iloc[0, 3] if not reg_match.empty else None

        patent_number = patent_match.iloc[0, 1] if not patent_match.empty and pd.notna(patent_match.iloc[0, 1]) else "Нет данных"
        patent_expiry = patent_match.iloc[0, 3] if not patent_match.empty else None

        output_data.append([
            emp,
            reg_number,
            calculate_remaining_days(reg_expiry),
            patent_number,
            calculate_remaining_days(patent_expiry)
        ])
    except Exception as e:
        logging.error(f"Ошибка при обработке сотрудника {emp}: {e}")

# Сохранение результата
output_file = os.path.join(output_folder, f"выгрузка_по_точке_{key}.xlsx")
try:
    pd.DataFrame(
        output_data,
        columns=["Сотрудник", "Рег. номер", "Рег. срок", "Патент номер", "Патент срок"]
    ).to_excel(output_file, index=False)
    
    # Вывод результата для бота
    print(f"SUCCESS:{output_file}")
    
    
except Exception as e:
    logging.error(f"Ошибка при сохранении файла: {e}")
    sys.exit(1)
