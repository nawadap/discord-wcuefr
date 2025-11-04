# guild_storage.py
import json, os, asyncio
_locks = {}

def guild_file(base_dir: str, guild_id: int, name: str) -> str:
    d = os.path.join(base_dir, str(guild_id))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)

def lock_for(path: str) -> asyncio.Lock:
    _locks.setdefault(path, asyncio.Lock())
    return _locks[path]

async def read_json(path: str, default):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        await write_json(path, default)
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

async def write_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
