# guild_config.py
import json, os
from typing import Optional, Dict

DEFAULT_CFG = {
    "channels": {
        "shop_log": None,
        "admin_log": None,
        "invite_log": None,
    },
    "roles": {
        "bronze": None,
        "argent": None,
        "or": None,
    },
    "params": {
        "invite_reward_points": 20
    }
}

def cfg_path(base_dir: str, guild_id: int) -> str:
    d = os.path.join(base_dir, str(guild_id))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "config.json")

def load_cfg(base_dir: str, guild_id: int) -> Dict:
    path = cfg_path(base_dir, guild_id)
    if not os.path.exists(path):
        save_cfg(base_dir, guild_id, DEFAULT_CFG.copy())
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_cfg(base_dir: str, guild_id: int, data: Dict) -> None:
    path = cfg_path(base_dir, guild_id)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
