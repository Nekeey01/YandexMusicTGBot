import os
import time
import json
import threading
import datetime as dt
import re
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv
import telebot
from yandex_music import Client

import time
import random
from telebot.apihelper import ApiTelegramException

def safe_send_message(bot, chat_id: int, text: str, **kwargs) -> bool:
    """
    Надежная отправка: переживает 502/429/таймауты.
    Возвращает True если доставлено, False если нет.
    """
    max_attempts = 6
    base_delay = 1.0

    for attempt in range(1, max_attempts + 1):
        try:
            bot.send_message(chat_id, text, **kwargs)
            return True

        except ApiTelegramException as e:
            # e.error_code есть почти всегда
            code = getattr(e, "error_code", None)

            # 502/500/503/504: временные серверные/сетевые штуки
            if code in (500, 502, 503, 504):
                delay = base_delay * (2 ** (attempt - 1)) + random.random()
                time.sleep(min(delay, 30))
                continue

            # 429 Too Many Requests: Telegram просит подождать
            if code == 429:
                # иногда telegram кладёт retry_after в json, но telebot не всегда удобно отдаёт
                # поэтому ждём экспоненциально
                delay = base_delay * (2 ** (attempt - 1)) + random.random()
                time.sleep(min(delay, 60))
                continue

            # 403/400 и т.п. обычно не лечатся ретраями (бот заблокирован, чат недоступен)
            return False

        except Exception:
            # Таймауты/сетевые ошибки requests и прочее
            delay = base_delay * (2 ** (attempt - 1)) + random.random()
            time.sleep(min(delay, 30))
            continue

    return False

STATE_FILE = os.getenv("STATE_FILE", "state.json")

# y0__xCUk7a_Ahje-AYg44_tixbO5GnjC0NZ2tNCT9R04rk4dHeTFw

def now_str() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_ts() -> int:
    return int(time.time())


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def parse_interval_to_seconds(raw: str) -> int:
    """
    Поддержка:
      - "30" (секунды)
      - "30s", "45sec"
      - "2m", "10min"
      - "1h", "3hour"
      - "1d", "2day"
    """
    s = raw.strip().lower()

    # просто число = секунды
    if re.fullmatch(r"\d+", s):
        secs = int(s)
        return secs

    m = re.fullmatch(r"(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|с|сек|секунды|секунда|секунд|м|мин|мины|минута|минуты|ч|час|часа|часов|д|день|дня|дней)", s)
    if not m:
        raise ValueError("Непонятный формат интервала")

    n = int(m.group(1))
    unit = m.group(2)

    if unit in {"s", "sec", "secs", "second", "seconds", "с", "сек", "секунды", "секунда", "секунд"}:
        return n
    if unit in {"m", "min", "mins", "minute", "minutes", "м", "мин", "мины", "минута", "минуты" }:
        return n * 60
    if unit in {"h", "hr", "hrs", "hour", "hours", "ч", "час", "часа", "часов" }:
        return n * 3600
    if unit in {"d", "day", "days", "д", "день", "дня", "дней"}:
        return n * 86400

    raise ValueError("Неподдерживаемая единица времени")


def fmt_interval(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600 and seconds % 60 == 0:
        return f"{seconds // 60}m"
    if seconds < 86400 and seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    return f"{seconds}s"


def track_display(track) -> str:
    title = getattr(track, "title", None) or "<без названия>"

    artists = None
    if hasattr(track, "artists_name"):
        artists = track.artists_name()
    elif hasattr(track, "artistsName"):
        artists = track.artistsName()

    if artists:
        return f"{", ".join(artists)} — {title}"
    return title


def resolve_owner_uid(client: Client) -> Optional[int]:
    me = getattr(client, "me", None)
    if callable(me):
        try:
            me = me()
        except Exception:
            me = None

    account = getattr(me, "account", None)
    uid = getattr(account, "uid", None)
    if uid:
        return int(uid)
    return None


def _fetch_tracks_by_ids(client: Client, track_ids: list) -> Dict[str, str]:
    if not track_ids:
        return {}

    # API ожидает список строк вида "<track_id>:<album_id>"
    ids = [str(track_id) for track_id in track_ids]

    tracks = []
    try:
        tracks = client.tracks(ids)
    except Exception:
        try:
            tracks = client.tracks(track_ids=ids)
        except Exception:
            return {}

    result: Dict[str, str] = {}

    count = 0
    for tr in tracks or []:
        if count < 10:
            count += 1
        if tr is None:
            continue
        tid = getattr(tr, "id", None)
        albums = getattr(tr, "albums", None)
        try:
            album_id = getattr(albums[0], "id", None)
        except Exception:
            continue
        if tid is None or album_id is None:
            continue
        key = f"{tid}:{album_id}"
        result[key] = track_display(tr)
    return result


def fetch_snapshot(client: Client, owner_uid: Optional[int] = None) -> Dict[str, str]:
    if owner_uid is not None:
        try:
            likes = client.users_likes_tracks(user_id=owner_uid)
        except TypeError:
            likes = client.users_likes_tracks(owner_uid)
    else:
        likes = client.users_likes_tracks()
    snap: Dict[str, str] = {}
    missing_ids = []
    for ts in likes:
        tid = ts.track_id
        if getattr(ts, "track", None) is not None:
            snap[tid] = track_display(ts.track)
        else:
            missing_ids.append(tid)

    if missing_ids:
        resolved = _fetch_tracks_by_ids(client, missing_ids)
        for tid in missing_ids:
            snap[tid] = resolved.get(tid, f"<track_id={tid}>")
    return snap


class MultiWatcher:
    def __init__(self, bot: telebot.TeleBot, default_poll_seconds: int):
        self.bot = bot
        self.default_poll_seconds = default_poll_seconds

        self._lock = threading.Lock()
        self.state = load_state()
        self.state.setdefault("users", {})

        self._threads: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}

    def _ensure_user(self, tg_user_id: str) -> dict:
        self.state["users"].setdefault(tg_user_id, {})
        u = self.state["users"][tg_user_id]

        u.setdefault("watch", {
            "is_running": False,
            "started_at_ts": None,
            "removed_count": 0,
            "added_count": 0,
        })
        u.setdefault("snapshot", {})
        u.setdefault("poll_seconds", self.default_poll_seconds)
        return u

    def set_chat_id(self, tg_user_id: str, chat_id: int) -> None:
        with self._lock:
            u = self._ensure_user(tg_user_id)
            u["chat_id"] = chat_id
            save_state(self.state)

    def set_token(self, tg_user_id: str, ym_token: str) -> None:
        with self._lock:
            u = self._ensure_user(tg_user_id)
            u["ym_token"] = ym_token
            save_state(self.state)

    def set_interval(self, tg_user_id: str, seconds: int) -> None:
        # защита от “каждую миллисекунду”
        if seconds < 10:
            raise ValueError("Слишком часто. Минимум 10 секунд.")
        # и от “раз в 5 лет”
        if seconds > 7 * 86400:
            raise ValueError("Слишком редко. Максимум 7 дней.")
        with self._lock:
            u = self._ensure_user(tg_user_id)
            u["poll_seconds"] = seconds
            save_state(self.state)

    def get_interval(self, tg_user_id: str) -> int:
        with self._lock:
            u = self._ensure_user(tg_user_id)
            return int(u.get("poll_seconds") or self.default_poll_seconds)

    def stop(self, tg_user_id: str) -> None:
        with self._lock:
            u = self._ensure_user(tg_user_id)
            u["watch"]["is_running"] = False
            save_state(self.state)

            flag = self._stop_flags.get(tg_user_id)
            if flag:
                flag.set()

    def start(self, tg_user_id: str) -> Tuple[bool, str]:
        with self._lock:
            u = self._ensure_user(tg_user_id)

            chat_id = u.get("chat_id")
            if not chat_id:
                return False, "Сначала /start, чтобы я знал, куда писать."

            ym_token = u.get("ym_token")
            if not ym_token:
                return False, "Нет YM_TOKEN. Пришли: /settoken <токен>"

            if u["watch"].get("is_running"):
                return True, "Мониторинг уже запущен."

            u["watch"]["is_running"] = True
            u["watch"]["started_at_ts"] = utc_ts()
            u["watch"]["removed_count"] = 0
            u["watch"]["added_count"] = 0
            save_state(self.state)

            stop_flag = threading.Event()
            self._stop_flags[tg_user_id] = stop_flag

            poll_seconds_initial = int(u.get("poll_seconds") or self.default_poll_seconds)

        def loop():
            try:
                client = Client(ym_token).init()
            except Exception as e:
                safe_send_message(self.bot, chat_id, f"⚠️ [{now_str()}] Не смог инициализировать Я.Музыку: {e!r}")
                self.stop(tg_user_id)
                return

            try:
                owner_uid = resolve_owner_uid(client)
                if owner_uid is None:
                    raise RuntimeError(
                        "Не удалось определить ownerUid. Проверьте, что токен получен из music.yandex.ru "
                        "и принадлежит вашему аккаунту."
                    )
                prev = fetch_snapshot(client, owner_uid=owner_uid)
            except Exception as e:
                safe_send_message(self.bot, chat_id, f"⚠️ [{now_str()}] Не смог снять первичный слепок: {e!r}")
                self.stop(tg_user_id)
                return

            with self._lock:
                u2 = self._ensure_user(tg_user_id)
                u2["snapshot"] = prev
                save_state(self.state)

            safe_send_message(
                self.bot,
                chat_id,
                f"✅ [{now_str()}] Мониторинг запущен.\n"
                f"- Треков в 'Мне нравится': {len(prev)}\n"
                f"- Интервал: {fmt_interval(poll_seconds_initial)}"
            )

            while not stop_flag.is_set():
                # берем актуальный интервал на каждом цикле (чтобы /setinterval применялся сразу)
                with self._lock:
                    u3 = self._ensure_user(tg_user_id)
                    if not u3["watch"].get("is_running"):
                        break
                    poll_seconds = int(u3.get("poll_seconds") or self.default_poll_seconds)
                    chat_id_local = u3.get("chat_id", chat_id)

                # ждем, но с возможностью быстро остановиться
                if stop_flag.wait(timeout=poll_seconds):
                    break

                try:
                    curr = fetch_snapshot(client, owner_uid=owner_uid)
                except Exception as e:
                    safe_send_message(self.bot,chat_id_local, f"⚠️ [{now_str()}] Ошибка запроса Яндекс.Музыки: {e!r}")
                    continue

                prev_ids = set(prev.keys())
                curr_ids = set(curr.keys())

                # added = sorted(curr_ids - prev_ids)
                removed = sorted(prev_ids - curr_ids)

                if not removed:
                    continue

                lines = [f"Изменения в 'Мне нравится':"]
                # if added:
                #     lines.append("➕ Добавлено:")
                #     for tid in added[:50]:
                #         lines.append(f"  + {curr.get(tid, tid)}")
                #     if len(added) > 50:
                #         lines.append(f"  …и еще {len(added) - 50}")

                if removed:
                    lines.append("➖ Удалено:")
                    for tid in removed[:50]:
                        lines.append(f"  - {prev.get(tid, tid)}")
                    if len(removed) > 50:
                        lines.append(f"  …и еще {len(removed) - 50}")

                safe_send_message(self.bot,chat_id_local, "\n".join(lines))

                with self._lock:
                    u4 = self._ensure_user(tg_user_id)
                    # u4["watch"]["added_count"] += len(added)
                    u4["watch"]["removed_count"] += len(removed)
                    u4["snapshot"] = curr
                    save_state(self.state)

                prev = curr

            with self._lock:
                u5 = self._ensure_user(tg_user_id)
                u5["watch"]["is_running"] = False
                save_state(self.state)

        t = threading.Thread(target=loop, daemon=True)
        self._threads[tg_user_id] = t
        t.start()

        return True, f"Запустил. Интервал: {fmt_interval(self.get_interval(tg_user_id))}"

    def status_text(self, tg_user_id: str) -> str:
        with self._lock:
            u = self._ensure_user(tg_user_id)
            has_token = bool(u.get("ym_token"))
            running = bool(u["watch"].get("is_running"))
            started = u["watch"].get("started_at_ts")
            poll_seconds = int(u.get("poll_seconds") or self.default_poll_seconds)

            started_str = (
                dt.datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S")
                if started else "не запускался"
            )

            snap_len = len(u.get("snapshot", {}))

            return (
                f"📌 Статус\n"
                f"- YM_TOKEN: {'есть' if has_token else 'нет'}\n"
                f"- Мониторинг: {'работает' if running else 'остановлен'}\n"
                f"- Старт: {started_str}\n"
                f"- Треков в последнем слепке: {snap_len}\n"
                f"- Интервал: {fmt_interval(poll_seconds)}"
            )

    def stats_text(self, tg_user_id: str) -> str:
        with self._lock:
            u = self._ensure_user(tg_user_id)
            started = u["watch"].get("started_at_ts")
            removed = int(u["watch"].get("removed_count") or 0)
            added = int(u["watch"].get("added_count") or 0)

        if not started:
            return "📊 Статистика пуста. Сначала /watch."

        started_dt = dt.datetime.fromtimestamp(started)
        delta = dt.datetime.now() - started_dt
        total_seconds = int(delta.total_seconds())
        hours = total_seconds // 3600
        mins = (total_seconds % 3600) // 60
        secs = total_seconds % 60
        dur = f"{hours}ч {mins}м {secs}с" if hours else f"{mins}м {secs}с"

        return (
            f"📊 Статистика с момента /watch\n"
            f"- Старт: {started_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- Прошло: {dur}\n"
            f"- Удалено треков: {removed}\n"
            f"- Добавлено треков: {added}"
        )


def main():
    load_dotenv()
    tg_token = os.getenv("TG_BOT_TOKEN")
    default_poll_seconds = int(os.getenv("POLL_SECONDS", "300"))

    if not tg_token:
        raise SystemExit("Нет TG_BOT_TOKEN в .env")

    bot = telebot.TeleBot(tg_token)
    watcher = MultiWatcher(bot=bot, default_poll_seconds=default_poll_seconds)

    @bot.message_handler(commands=["start"])
    def on_start(message):
        tg_user_id = str(message.from_user.id)
        watcher.set_chat_id(tg_user_id, message.chat.id)

        safe_send_message(
            bot,
            message.chat.id,
            "👋 Я мониторю 'Мне нравится' в Яндекс.Музыке.\n\n"
            "Команды:\n"
            "/settoken <YM_TOKEN> (только в ЛС боту)\n"
            "/setinterval <30s|1m|1d|300>\n"
            "/watch\n"
            "/stop\n"
            "/status\n"
            "/stats\n"
        )

    @bot.message_handler(commands=["settoken"])
    def on_settoken(message):
        tg_user_id = str(message.from_user.id)
        watcher.set_chat_id(tg_user_id, message.chat.id)

        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            safe_send_message(bot, message.chat.id, "Формат: /settoken <YM_TOKEN>")
            return

        ym_token = parts[1].strip()
        watcher.set_token(tg_user_id, ym_token)
        safe_send_message(bot, message.chat.id, "✅ Токен сохранен. Теперь можно /watch")

    @bot.message_handler(commands=["setinterval"])
    def on_setinterval(message):
        tg_user_id = str(message.from_user.id)
        watcher.set_chat_id(tg_user_id, message.chat.id)

        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            safe_send_message(bot,
                message.chat.id,
                "Формат: /setinterval <30s|1m|1d|300>\n"
                "Примеры: /setinterval 30s, /setinterval 1m, /setinterval 86400"
            )
            return

        raw = parts[1].strip()
        try:
            secs = parse_interval_to_seconds(raw)
            watcher.set_interval(tg_user_id, secs)
        except Exception as e:
            safe_send_message(bot,message.chat.id, f"❌ Не получилось: {e}")
            return

        safe_send_message(bot,
            message.chat.id,
            f"✅ Интервал обновлен: {fmt_interval(watcher.get_interval(tg_user_id))}\n"
            "Если мониторинг уже запущен, изменения применятся на следующем цикле."
        )

    @bot.message_handler(commands=["watch"])
    def on_watch(message):
        tg_user_id = str(message.from_user.id)
        watcher.set_chat_id(tg_user_id, message.chat.id)

        ok, msg = watcher.start(tg_user_id)
        safe_send_message(bot,message.chat.id, ("✅ " if ok else "❌ ") + msg)

    @bot.message_handler(commands=["stop"])
    def on_stop(message):
        tg_user_id = str(message.from_user.id)
        watcher.stop(tg_user_id)
        safe_send_message(bot, message.chat.id, "🛑 Остановил мониторинг для тебя.")

    @bot.message_handler(commands=["status"])
    def on_status(message):
        tg_user_id = str(message.from_user.id)
        watcher.set_chat_id(tg_user_id, message.chat.id)
        safe_send_message(bot, message.chat.id, watcher.status_text(tg_user_id))

    @bot.message_handler(commands=["stats"])
    def on_stats(message):
        tg_user_id = str(message.from_user.id)
        watcher.set_chat_id(tg_user_id, message.chat.id)
        safe_send_message(bot, message.chat.id, watcher.stats_text(tg_user_id))

    print(f"[{now_str()}] Bot started.")
    while True:
        try:
            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
                skip_pending=True,
                allowed_updates=None
            )
        except Exception as e:
            print(f"[{now_str()}] polling crashed: {e!r}")
            time.sleep(5)


if __name__ == "__main__":
    main()
