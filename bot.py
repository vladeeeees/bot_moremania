import os
import logging
import asyncio
import subprocess
from dotenv import load_dotenv
import pandas as pd

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandStart
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup 
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = os.getenv("ADMIN_IDS", "").split(",")

# ============= Конфигурация локаций =============
locations = {
    "kievskaya": {
        "display_name": "Моремания.Киевская",
        "password_env": "PASSWORD_KIEVSKAYA",
        "chat_id_env": "CHAT_ID_KIEVSKAYA"
    },
    "dmitrovka": {
        "display_name": "Моремания.Дмитровка",
        "password_env": "PASSWORD_DMITROVKA",
        "chat_id_env": "CHAT_ID_DMITROVKA"
    }
}

scheduled_jobs = {
    "kievskaya": {"cron": {"hour": 14, "minute": 24, "day_of_week": "fri"}},
    "dmitrovka": {"cron": {"hour": 10, "minute": 2, "day_of_week": "mon"}}
}

# ============= Функции для работы с скриптом =============
async def execute_script(location_key: str, password: str):
    """Общая функция для выполнения скрипта"""
    try:
        script_path = os.path.join(
            os.path.dirname(__file__),
            "script.py"
        )

        result = subprocess.run(
            ["python", script_path, location_key],
            capture_output=True,
            text=True
        )

        output = result.stdout.strip()
        if result.returncode != 0:
            raise Exception(result.stderr)

        output_lines = output.splitlines()
        last_line = output_lines[-1].strip()

        if last_line.startswith("SUCCESS:"):
            file_path = last_line.replace("SUCCESS:", "").strip()
        else:
            raise Exception(f"Неожиданный формат вывода: {last_line}")

        return file_path

    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта для {location_key}: {e}")
        raise

async def send_results_to_chat(location_key: str, file_path: str, chat_id: str, 
                              is_auto: bool, user_message: types.Message = None):
    try:
        file_name = os.path.basename(file_path)
        df = pd.read_excel(file_path)
        
        # Формируем сообщение
        prefix = "🔄 Автоматическая выгрузка" if is_auto else "📊 Ручная выгрузка"
        stats = (
            f"{prefix}\n"
            f"Локация: {locations[location_key]['display_name']}\n"
            f"Записей: {len(df)}\n"
            f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                 )
        # Отправка в чат
        if chat_id and chat_id.strip():
            await bot.send_message(
                chat_id=int(chat_id),
                text=stats
            )
            await bot.send_document(
                chat_id=int(chat_id),
                document=types.FSInputFile(file_path),
                caption=f"Файл: {file_name}"
            )

        

            long_message = ""
            for _, row in df.iterrows():
                сотрудник = row['Сотрудник']
                рег_срок = row['Рег. срок']
                патент_срок = row['Патент срок']

                message_text = (
                    f"{сотрудник}\n"
                    f"\tСрок действия: {рег_срок}\n"
                    f"\tСрок действия патента: {патент_срок}\n\n"
                )
                long_message += message_text
        
       
        if chat_id and chat_id.strip():
            await bot.send_message(
                chat_id=int(chat_id),
                text=long_message
            )
        
        
        # Отправка пользователю (для ручного запуска)
        if not is_auto and user_message:
            await user_message.answer_document(
                types.FSInputFile(file_path),
                caption=f"✅ Готово! Файл отправлен в чат {chat_id}"
            )
    

    except Exception as e:
        error_msg = f"❌ Ошибка отправки в чат {chat_id}: {str(e)}"
        logger.error(error_msg)
        if user_message:
            await user_message.answer(error_msg)

# ============= Автоматические задачи =============
async def scheduled_task(location_key: str):
    try:
        location_config = locations.get(location_key)
        password = os.getenv(location_config["password_env"])
        chat_id = os.getenv(location_config["chat_id_env"])
        
        file_path = await execute_script(location_key, password)
        await send_results_to_chat(
            location_key=location_key,
            file_path=file_path,
            chat_id=chat_id,
            is_auto=True
        )

    except Exception as e:
        error_msg = f"❌ Автозадача {location_key}: {str(e)}"
        logger.error(error_msg)
        for admin_id in ADMIN_IDS:
            if admin_id.strip():
                await bot.send_message(int(admin_id), error_msg)

async def setup_scheduler():
    scheduler = AsyncIOScheduler()
    for location_key, params in scheduled_jobs.items():
        trigger = CronTrigger(**params["cron"])
        scheduler.add_job(
            scheduled_task,
            trigger=trigger,
            args=[location_key],
            timezone="Europe/Moscow"
        )
    scheduler.start()

# ============= Основной код бота =============
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

class Form(StatesGroup):
    waiting_for_location = State()
    waiting_for_password = State()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def is_admin(user_id: int) -> bool:
    return str(user_id) in ADMIN_IDS

def validate_location(location_key: str) -> bool:
    return location_key in locations

def create_main_button(keyboard):
    main_button = KeyboardButton(text="На главную")
    return ReplyKeyboardMarkup(
        keyboard=[*keyboard.keyboard, [main_button]],
        resize_keyboard=True
    )

@dp.message(CommandStart())
@dp.message(Command("help"))
async def send_welcome(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Начать работу")]],
        resize_keyboard=True
    )
    await message.answer("Привет! Нажмите 'Начать работу' для запуска.", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Начать работу")
async def choose_location(message: types.Message, state: FSMContext):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=loc["display_name"])] for loc in locations.values()],
        resize_keyboard=True
    )
    await message.answer(
        "Выберите локацию:",
        reply_markup=create_main_button(keyboard)
    )
    await state.set_state(Form.waiting_for_location)

@dp.message(Form.waiting_for_location)
async def process_location(message: types.Message, state: FSMContext):
    if message.text == "На главную":
        await send_welcome(message)
        await state.clear()
        return

    location_key = next((key for key, loc in locations.items() if loc["display_name"] == message.text), None)
    
    if not location_key:
        await message.answer("⚠️ Пожалуйста, выберите локацию из списка")
        return

    await state.update_data(location=location_key)
    await message.answer(
        "Введите пароль:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="На главную")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Form.waiting_for_password)

@dp.message(Form.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    if message.text == "На главную":
        await send_welcome(message)
        await state.clear()
        return

    user_data = await state.get_data()
    location_key = user_data.get("location")
    
    location_config = locations.get(location_key)
    if not location_config:
        await message.answer("❌ Ошибка конфигурации локации")
        await state.clear()
        return

    correct_password = os.getenv(location_config["password_env"])
    if message.text == correct_password:
        await message.answer("⏳ Пароль верный. Идет подготовка данных...")
        
        try:
            file_path = await execute_script(location_key, correct_password)
            chat_id = os.getenv(location_config["chat_id_env"])
            
            await send_results_to_chat(
                location_key=location_key,
                file_path=file_path,
                chat_id=chat_id,
                is_auto=False,
                user_message=message
            )

            df = pd.read_excel(file_path)

            long_message = ""
            for _, row in df.iterrows():
                сотрудник = row['Сотрудник']
                рег_номер = row['Рег. номер']
                рег_срок = row['Рег. срок']
                патент_номер = row['Патент номер']
                патент_срок = row['Патент срок']

                message_text = (
                    f"{сотрудник}\n"
                    f"\tСрок действия регистрации: {рег_срок}\n"
                    f"\tСрок действия патента: {патент_срок}\n\n"
                )
                long_message += message_text

            await message.answer(long_message)


            
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await message.answer(f"❌ Ошибка: {str(e)}")
        
        await state.clear()
    else:
        await message.answer("❌ Неверный пароль. Попробуйте снова.")

async def main():
    await setup_scheduler()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())