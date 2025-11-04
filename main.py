# --- BOT POINTS + BOUTIQUE (SANS VOCAUX) ---

import asyncio, json, logging, os, tempfile, random
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, List, Optional
import discord
from discord import Intents, app_commands
from discord.ext import commands
from discord.ui import View, Select
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

if not logging.getLogger().handlers: 
    logging.basicConfig(
        level=logging.INFO,  
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.app_commands").setLevel(logging.WARNING)
    
# ---------- Chargement config ----------
load_dotenv()
# --- Token et guilde ---
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN manquant dans .env")

# --- Fichiers de données ---
POINTS_DB_PATH = os.getenv("POINTS_DB_PATH", "data/points.json")
SHOP_DB_PATH = os.getenv("SHOP_DB_PATH", "data/shop.json")
PURCHASES_DB_PATH = os.getenv("PURCHASES_DB_PATH", "data/purchases.json")
INVITES_DB_PATH = os.getenv("INVITES_DB_PATH", "data/invites.json")
DAILY_DB_PATH = os.getenv("DAILY_DB_PATH", "data/daily.json")
INVITE_REWARDS_DB_PATH = os.getenv("INVITE_REWARDS_DB_PATH", "data/invites_rewards.json")
QUESTS_DB_PATH = os.getenv("QUESTS_DB_PATH", "data/quests.json")            
QUESTS_PROGRESS_DB_PATH = os.getenv("QUESTS_PROGRESS_DB_PATH", "data/quests_progress.json")  

# --- Salons de logs ---
SHOP_LOG_CHANNEL_ID = int(os.getenv("SHOP_LOG_CHANNEL_ID", "0"))
ADMIN_LOG_CHANNEL_ID = int(os.getenv("ADMIN_LOG_CHANNEL_ID", "0"))
INVITE_LOG_CHANNEL_ID = int(os.getenv("INVITE_LOG_CHANNEL_ID", "0"))

# --- Paramètres ---
INVITE_REWARD_POINTS = int(os.getenv("INVITE_REWARD_POINTS", "20"))
BRONZE = int(os.getenv("BRONZE_ROLE_ID", "0"))
ARGENT = int(os.getenv("ARGENT_ROLE_ID", "0"))
OR     = int(os.getenv("OR_ROLE_ID", "0"))

POINTS_MULTIPLIERS = {BRONZE: 1.10, ARGENT: 1.25, OR: 1.50}
DAILY_FLAT_BONUS   = {BRONZE: 1, ARGENT: 2, OR: 4}
SHOP_DISCOUNT      = {BRONZE: 0.05, ARGENT: 0.10, OR: 0.15}
POINTS_BONUS_CAP   = 1.50  # sécurité : max +50%

# --- Verrous (internes, pas dans .env) ---
_points_lock = asyncio.Lock()
_shop_lock = asyncio.Lock()
_purchases_lock = asyncio.Lock()
_invites_lock = asyncio.Lock()
_daily_lock = asyncio.Lock()
_invite_rewards_lock = asyncio.Lock()
_quests_lock = asyncio.Lock()
_quests_progress_lock = asyncio.Lock()

_voice_sessions: dict[tuple[int, int], int] = {}
# ---------- Intents & client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True   
intents.voice_states = True  

bot = commands.Bot(
    command_prefix=commands.when_mentioned, 
    intents=intents,
    help_command=None
)

tree = bot.tree

TARGET_GUILDS = [discord.Object(id=GUILD_ID)] if GUILD_ID else None
def guilds_decorator():
    return app_commands.guilds(*TARGET_GUILDS) if TARGET_GUILDS else (lambda f: f)

# ---------- Logs boutique (salon staff) ----------
async def _send_shop_log(guild: discord.Guild, user: discord.User | discord.Member,
                         item_name: str, cost: int, remaining: int,
                         role_name: str | None = None, note: str = ""):
    if not SHOP_LOG_CHANNEL_ID:
        return
    channel = guild.get_channel(SHOP_LOG_CHANNEL_ID)
    if channel is None:
        try:
            channel = await guild.fetch_channel(SHOP_LOG_CHANNEL_ID)  # type: ignore
        except Exception:
            return

    embed = discord.Embed(
        title="🛒 Achat boutique",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Membre", value=f"{user.mention} (`{user.id}`)", inline=False)
    embed.add_field(name="Article", value=item_name, inline=True)
    embed.add_field(name="Coût", value=f"{cost} pts", inline=True)
    embed.add_field(name="Points restants", value=str(remaining), inline=True)
    if role_name:
        embed.add_field(name="Rôle", value=role_name, inline=True)
    if note:
        embed.add_field(name="Note", value=note, inline=False)
    try:
        await channel.send(embed=embed)
    except Exception:
        pass

# ---------- Logs admin (salon dédié) ----------
async def _send_admin_log(
    guild: discord.Guild,
    actor: discord.User | discord.Member,
    action: str,
    **details: str | int | None
):
    """
    Envoie un embed de log admin dans ADMIN_LOG_CHANNEL_ID (si défini).
    action: identifiant court (ex: 'shopadmin.add_item', 'addpoints', ...)
    details: paires clé/valeur affichées en champs (converties en str).
    """
    if not ADMIN_LOG_CHANNEL_ID:
        return  # pas de fallback pour bien séparer des achats

    channel = guild.get_channel(ADMIN_LOG_CHANNEL_ID)
    if channel is None:
        try:
            channel = await guild.fetch_channel(ADMIN_LOG_CHANNEL_ID)  # type: ignore
        except Exception:
            return

    embed = discord.Embed(
        title="🔧 Action admin",
        description=f"**{action}**",
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Par", value=f"{actor.mention} (`{actor.id}`)", inline=False)
    for k, v in details.items():
        if v is None:
            continue
        embed.add_field(name=str(k), value=str(v), inline=True)

    try:
        await channel.send(embed=embed)
    except Exception:
        pass

# ---------- Points (JSON) ----------
def _ensure_points_exists():
    if not os.path.exists(POINTS_DB_PATH):
        with open(POINTS_DB_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f)

def _load_points() -> Dict[str, int]:
    _ensure_points_exists()
    with open(POINTS_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {str(k): int(v) for k, v in data.items()}

def _save_points(points: Dict[str, int]) -> None:
    _atomic_write(POINTS_DB_PATH, points)

async def add_points(user_id: int, amount: int) -> int:
    async with _points_lock:
        data = _load_points()
        new_val = max(0, int(data.get(str(user_id), 0)) + amount)
        data[str(user_id)] = new_val
        _save_points(data)
        return new_val

async def remove_points(user_id: int, amount: int) -> int:
    async with _points_lock:
        data = _load_points()
        new_val = max(0, int(data.get(str(user_id), 0)) - amount)
        data[str(user_id)] = new_val
        _save_points(data)
        return new_val

async def get_leaderboard(guild: discord.Guild, top: int = 10) -> List[Tuple[str, int]]:
    async with _points_lock:
        data = _load_points()
    sorted_items = sorted(((int(uid), pts) for uid, pts in data.items()),
                          key=lambda x: x[1], reverse=True)[:top]
    results: List[Tuple[str, int]] = []
    for uid, pts in sorted_items:
        member = guild.get_member(uid)
        if member:
            display = member.display_name
        else:
            try:
                user = await bot.fetch_user(uid)
                display = user.name
            except Exception:
                display = f"Utilisateur {uid}"
        results.append((display, pts))
    return results

# ---------- Shop (JSON) ----------
def _ensure_shop_exists():
    if not os.path.exists(SHOP_DB_PATH):
        with open(SHOP_DB_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "robux100": {
                    "name": "💸 100 Robux",
                    "cost": 2000,
                    "description": "Échange manuel : contacte un admin.",
                    "max_per_user": -1   # illimité
                },
                "robux1000": {
                    "name": "💸 1000 Robux",
                    "cost": 19500,
                    "description": "Échange manuel : contacte un admin.",
                    "max_per_user": -1   # illimité
                },
                "halloween": {
                  "name": "🎃 Titre Halloween",
                  "cost": 20,
                  "role_id": 1433190078737285231,
                  "description": "Attribue le rôle saisonnier d'Halloween ! Bouuhh.",
                  "max_per_user": 1
                },   
                "gift100pts": {
                    "name": "🎁 Offrir 100 points à un autre félin",
                    "cost": 150,
                    "description": "Échange manuel : contacte un admin.",
                    "max_per_user": -1    # illimité
                }
            }, f, ensure_ascii=False, indent=2)

def _load_shop() -> Dict[str, dict]:
    _ensure_shop_exists()
    with open(SHOP_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    shop = {}
    for key, item in data.items():
        shop[str(key)] = {
            "name": item.get("name", str(key)),
            "cost": int(item.get("cost", 0)),
            "role_id": int(item.get("role_id") or 0),
            "description": item.get("description", ""),
            "max_per_user": int(item.get("max_per_user", -1))  # -1 = illimité
        }
    return shop

def _save_shop(shop: Dict[str, dict]) -> None:
    _atomic_write(SHOP_DB_PATH, shop)

# ---------- Achats par utilisateur (JSON) ----------
def _ensure_purchases_exists():
    if not os.path.exists(PURCHASES_DB_PATH):
        with open(PURCHASES_DB_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f)

def _load_purchases() -> Dict[str, Dict[str, int]]:
    """Structure: { user_id(str): { item_key(str): count(int) } }"""
    _ensure_purchases_exists()
    with open(PURCHASES_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {str(uid): {str(k): int(v) for k, v in items.items()} for uid, items in data.items()}

def _save_purchases(p: Dict[str, Dict[str, int]]) -> None:
    _atomic_write(PURCHASES_DB_PATH, p)

async def get_user_purchase_count(user_id: int, key: str) -> int:
    async with _purchases_lock:
        p = _load_purchases()
        return int(p.get(str(user_id), {}).get(str(key), 0))

async def increment_purchase(user_id: int, key: str) -> int:
    async with _purchases_lock:
        p = _load_purchases()
        u = p.setdefault(str(user_id), {})
        u[str(key)] = int(u.get(str(key), 0)) + 1
        _save_purchases(p)
        return u[str(key)]

# ---------- Invite tracker (JSON + cache) ----------
def _ensure_invites_exists():
    if not os.path.exists(INVITES_DB_PATH):
        with open(INVITES_DB_PATH, "w", encoding="utf-8") as f:
            # structure: { "counts": {inviter_id: total}, "refs": {member_id: inviter_id} }
            json.dump({"counts": {}, "refs": {}}, f)

def _load_invites() -> Dict[str, Dict[str, int]]:
    _ensure_invites_exists()
    with open(INVITES_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["counts"] = {str(k): int(v) for k, v in data.get("counts", {}).items()}
    data["refs"] = {str(k): int(v) for k, v in data.get("refs", {}).items()}
    return data

def _save_invites(data: Dict[str, Dict[str, int]]) -> None:
    _atomic_write(INVITES_DB_PATH, data)


async def _add_invite_for(inviter_id: int, member_id: int) -> int:
    async with _invites_lock:
        db = _load_invites()
        counts = db.setdefault("counts", {})
        refs = db.setdefault("refs", {})
        counts[str(inviter_id)] = int(counts.get(str(inviter_id), 0)) + 1
        refs[str(member_id)] = int(inviter_id)
        _save_invites(db)
        return counts[str(inviter_id)]

async def _remove_invite_for_member(member_id: int) -> tuple[int | None, int | None]:
    """Retourne (inviter_id, nouveau_total) si on a pu décrémenter, sinon (None, None)."""
    async with _invites_lock:
        db = _load_invites()
        counts = db.setdefault("counts", {})
        refs = db.setdefault("refs", {})
        inviter_id = refs.pop(str(member_id), None)
        if inviter_id is None:
            _save_invites(db)
            return None, None
        new_total = max(0, int(counts.get(str(inviter_id), 0)) - 1)
        counts[str(inviter_id)] = new_total
        _save_invites(db)
        return inviter_id, new_total

async def _get_invite_count(inviter_id: int) -> int:
    async with _invites_lock:
        db = _load_invites()
        return int(db.get("counts", {}).get(str(inviter_id), 0))

# Cache des invites: par guilde -> code -> (uses, inviter_id)
InviteCache = Dict[int, Dict[str, tuple[int, int]]]
_invite_cache: InviteCache = {}

async def _refresh_invite_cache(guild: discord.Guild):
    """Charge guild.invites() et remplit le cache {code: (uses, inviter_id)}."""
    try:
        invites = await guild.invites()
    except discord.Forbidden:
        _invite_cache[guild.id] = {}
        return
    cache = {}
    for inv in invites:
        try:
            code = inv.code
            uses = inv.uses or 0
            inviter_id = inv.inviter.id if inv.inviter else 0
            cache[code] = (uses, inviter_id)
        except Exception:
            continue
    _invite_cache[guild.id] = cache

def _find_used_invite(before: Dict[str, tuple[int, int]], after: Dict[str, tuple[int, int]]) -> tuple[str | None, int | None]:
    """Compare 2 snapshots et renvoie (code, inviter_id) de l’invite qui a augmenté."""
    # 1) une invite dont le compteur a augmenté
    for code, (uses_before, inviter_id) in before.items():
        uses_after = after.get(code, (uses_before, inviter_id))[0]
        if uses_after > uses_before:
            return code, inviter_id
    # 2) invite disparue (atteinte max/expirée) mais présente avant => on considère utilisée
    for code, (uses_before, inviter_id) in before.items():
        if code not in after:
            return code, inviter_id
    return None, None

async def _send_invite_log(guild: discord.Guild, text: str):
    ch = None
    if INVITE_LOG_CHANNEL_ID:
        ch = guild.get_channel(INVITE_LOG_CHANNEL_ID) or await guild.fetch_channel(INVITE_LOG_CHANNEL_ID)
    if ch is None:
        ch = guild.system_channel
    if ch:
        try:
            await ch.send(text)
        except Exception:
            pass
# ---------- Helper ----------
async def _mark_command_use(guild_id: int, user_id: int, command_str: str):
    command_norm = command_str.strip().lower()
    date_key = _today_str()
    async with _quests_progress_lock:
        pdb  = _load_quests_progress()
        qcfg = _load_quests()

        # Assigner l’utilisateur si besoin pour aujourd’hui
        assigned_daily = _ensure_assignments(pdb, qcfg, "daily", date_key, guild_id, user_id, k=3)

        for qkey, q in qcfg.get("daily", {}).items():
            if qkey not in assigned_daily:
                continue
            if q.get("type") == "command_use" and str(q.get("command","")).strip().lower() == command_norm:
                slot = _ensure_user_quest_slot(pdb, "daily", date_key, guild_id, user_id, qkey)
                target = int(q.get("target", 1))
                slot["progress"] = min(target, int(slot.get("progress", 0)) + 1)

        _save_quests_progress(pdb)

def _get_assigned(progress_db: dict, bucket: str, period_key: str, guild_id: int, user_id: int) -> list[str]:
    return (progress_db
            .setdefault(bucket, {})
            .setdefault(period_key, {})
            .setdefault(str(guild_id), {})
            .setdefault(str(user_id), {})
            .setdefault("__assigned", []))

def _ensure_assignments(progress_db: dict, qcfg: dict, bucket: str, period_key: str,
                        guild_id: int, user_id: int, k: int = 3) -> list[str]:
    assigned = _get_assigned(progress_db, bucket, period_key, guild_id, user_id)
    if assigned:
        return assigned
    # pioche parmi les clés disponibles
    keys = list(qcfg.get(bucket, {}).keys())
    if not keys:
        assigned = []
    else:
        # pour éviter les doublons d'objectifs (ex: deux invites similaires) on garde tel quel,
        # c'est volontairement simple : un utilisateur peut avoir 2 "invites" différents.
        assigned = random.sample(keys, min(k, len(keys)))
    # on persist
    (progress_db[bucket][period_key][str(guild_id)][str(user_id)])["__assigned"] = assigned
    return assigned
                            
def tier_info(member: discord.Member) -> tuple[str | None, str | None, list[str]]:
    """
    Retourne (tier_key, tier_label, perks_list)
    - tier_key ∈ {"bronze","argent","or"} ou None
    - tier_label = texte + emoji
    - perks_list = liste des avantages à afficher dans /profile
    """
    rid = member_tier_role(member)
    if not rid:
        return None, None, []
    if rid == OR:
        return "or", "🥇 **Or**", ["Rôle exclusif", "Couleur du pseudo", "Badge dans /profile", "Hall of Fame (aura dorée)"]
    if rid == ARGENT:
        return "argent", "🥈 **Argent**", ["Rôle exclusif", "Couleur du pseudo", "Badge dans /profile"]
    if rid == BRONZE:
        return "bronze", "🥉 **Bronze**", ["Rôle exclusif", "Couleur du pseudo", "Badge dans /profile"]
    return None, None, []

def member_tier_role(member: discord.Member) -> int | None:
    ids = {r.id for r in member.roles}
    for rid in (OR, ARGENT, BRONZE):
        if rid and rid in ids:
            return rid
    return None

def points_multiplier_for(member: discord.Member) -> float:
    rid = member_tier_role(member)
    mul = POINTS_MULTIPLIERS.get(rid, 1.0)
    return min(mul, POINTS_BONUS_CAP)

def daily_flat_bonus_for(member: discord.Member) -> int:
    rid = member_tier_role(member)
    return int(DAILY_FLAT_BONUS.get(rid, 0))

def shop_discount_for(member: discord.Member) -> float:
    rid = member_tier_role(member)
    return float(SHOP_DISCOUNT.get(rid, 0.0))

# ---------- Quêtes : JSON + helpers ----------
def _ensure_quests_exists():
    """Crée un petit catalogue de quêtes si absent."""
    if not os.path.exists(QUESTS_DB_PATH):
        with open(QUESTS_DB_PATH, "w", encoding="utf-8") as f:
            # Daily + Weekly par défaut (selon ta demande)
            json.dump({
                "daily": {
                    "voice_30min": {
                        "name": "🔊 30 min en vocal",
                        "type": "voice_minutes",
                        "target": 30,
                        "reward": 10,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "messages_20": {
                        "name": "✉️ 20 messages",
                        "type": "messages",
                        "target": 20,
                        "reward": 5,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "invite_1": {
                        "name": "🤝 Inviter 1 membre",
                        "type": "invites",
                        "target": 1,
                        "reward": 10,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "say_meow": {
                        "name": "😺 Écrire MEOW dans le salon <#1431387258065391748>",
                        "type": "message_exact",
                        "text": "MEOW",
                        "channel_id": 1431387258065391748,
                        "target": 1,
                        "reward": 5,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "coucou_user": {
                        "name": "👋 Dire « Coucou <@1227330764321067039> »",
                        "type": "message_exact",
                        "text": "Coucou <@1227330764321067039>",
                        "target": 1,
                        "reward": 5,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "invite_2": {
                        "name": "🤝 Inviter 2 membres",
                        "type": "invites",
                        "target": 2,
                        "reward": 20,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "react_mod_1": {
                        "name": "❤️ Gagner une réaction d’un modérateur",
                        "type": "reaction_mod",
                        "target": 1,
                        "reward": 10,
                        "desc": "Obtiens au moins une réaction ❤️ d’un modérateur sur un de tes messages."
                    },
                    "claim_daily_bonus": {
                        "name": "🎁 Réclamer ton bonus quotidien",
                        "type": "command_use",
                        "target": 1,
                        "reward": 5,
                        "desc": "Utilise la commande `/daily` pour récupérer ton bonus journalier."
                    },
                    "react_3": {
                        "name": "🔥 Obtenir 3 réactions sur un message",
                        "type": "reaction_total",
                        "target": 3,
                        "reward": 10,
                        "desc": "Fais un message qui récolte au moins 3 réactions."
                    },
                    "profile_once": {
                        "name": "🧾 Ouvrir ton profil",
                        "type": "command_use",
                        "command": "/profile",
                        "target": 1,
                        "reward": 4,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    },
                    "night_owl_5": {
                        "name": "🌙 5 messages entre 23h–5h",
                        "type": "messages_time_window",
                        "tz": "Europe/Paris",
                        "start_hour": 23,
                        "end_hour": 5,
                        "target": 5,
                        "reward": 8,
                        "reset": "daily",
                        "max_claims_per_reset": 1
                    }
                },
                "weekly": {
                    "voice_500min": {
                        "name": "🔊 500 min en vocal",
                        "type": "voice_minutes",
                        "target": 500,
                        "reward": 20,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    },
                    "messages_200": {
                        "name": "✉️ 200 messages",
                        "type": "messages",
                        "target": 200,
                        "reward": 20,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    },
                    "invites_3": {
                        "name": "🤝 3 invitations",
                        "type": "invites",
                        "target": 3,
                        "reward": 25,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    },
                    "weekly_complete_10": {
                        "name": "🏁 Compléter 10 quêtes",
                        "type": "quests_completed",
                        "target": 10,
                        "reward": 20,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    },
                    "invites_5": {
                        "name": "🤝 5 invitations",
                        "type": "invites",
                        "target": 5,
                        "reward": 40,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    },
                    "invites_10": {
                        "name": "🤝 10 invitations",
                        "type": "invites",
                        "target": 10,
                        "reward": 80,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    },
                    "daily_claims_5": {
                        "name": "🏁 Prendre le daily 5 fois",
                        "type": "daily_claims_week",
                        "target": 5,
                        "reward": 20,
                        "reset": "weekly",
                        "max_claims_per_reset": 1
                    }
                }
            }, f, ensure_ascii=False, indent=2)

def _load_quests() -> dict:
    _ensure_quests_exists()
    with open(QUESTS_DB_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _ensure_quests_progress_exists():
    if not os.path.exists(QUESTS_PROGRESS_DB_PATH):
        with open(QUESTS_PROGRESS_DB_PATH, "w", encoding="utf-8") as f:
            # Nouveau format: deux buckets
            json.dump({"daily": {}, "weekly": {}}, f)

def _load_quests_progress() -> dict:
    _ensure_quests_progress_exists()
    with open(QUESTS_PROGRESS_DB_PATH, "r", encoding="utf-8") as f:
        pdb = json.load(f)
    # rétro-compat: ancien format “plat” -> ranger dans daily
    if "daily" not in pdb and "weekly" not in pdb:
        pdb = {"daily": pdb, "weekly": {}}
    if "daily" not in pdb:  pdb["daily"]  = {}
    if "weekly" not in pdb: pdb["weekly"] = {}
    return pdb

def _save_quests_progress(data: dict):
    _atomic_write(QUESTS_PROGRESS_DB_PATH, data)

def _today_str() -> str:
    # UTC
    return datetime.now(timezone.utc).date().isoformat()

def _week_str() -> str:
    d = datetime.now(timezone.utc).date()
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def _ensure_user_quest_slot(progress_db: dict, bucket: str, period_key: str,
                            guild_id: int, user_id: int, quest_key: str) -> dict:
    """
    bucket: 'daily' | 'weekly'
    period_key: ex: '2025-11-01' (daily) ou '2025-W44' (weekly)
    """
    g = (progress_db
         .setdefault(bucket, {})
         .setdefault(period_key, {})
         .setdefault(str(guild_id), {})
         .setdefault(str(user_id), {}))
    return g.setdefault(quest_key, {"progress": 0, "claimed": 0})

def _get_user_all_quests(progress_db: dict, bucket: str, period_key: str,
                         guild_id: int, user_id: int) -> dict:
    return (progress_db
            .get(bucket, {})
            .get(period_key, {})
            .get(str(guild_id), {})
            .get(str(user_id), {})
            or {})

def _atomic_write(path: str, data: dict):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or ".", prefix=".tmp_", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic
    finally:
        try: os.remove(tmp)
        except FileNotFoundError: pass
            
def _ensure_daily_exists():
    if not os.path.exists(DAILY_DB_PATH):
        with open(DAILY_DB_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f)
            
def _ensure_invite_rewards_exists():
    if not os.path.exists(INVITE_REWARDS_DB_PATH):
        with open(INVITE_REWARDS_DB_PATH, "w", encoding="utf-8") as f:
            # structure: { "rewarded": { member_id(str): inviter_id(int) } }
            json.dump({"rewarded": {}}, f)

def _load_invite_rewards() -> Dict[str, Dict[str, int]]:
    _ensure_invite_rewards_exists()
    with open(INVITE_REWARDS_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # cast en int pour sûreté
    rewarded = {str(mid): int(iid) for mid, iid in data.get("rewarded", {}).items()}
    return {"rewarded": rewarded}

def _save_invite_rewards(data: Dict[str, Dict[str, int]]) -> None:
    _atomic_write(INVITE_REWARDS_DB_PATH, data)

def _load_daily() -> Dict[str, dict]:
    """{ user_id(str): { 'last': ts(int), 'streak': int } } (compat ancien format int)"""
    _ensure_daily_exists()
    with open(DAILY_DB_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data: Dict[str, dict] = {}
    for k, v in raw.items():
        if isinstance(v, dict):
            last = int(v.get("last", 0))
            streak = int(v.get("streak", 0))
        else:
            # Ancien format : juste un timestamp -> on démarre à streak 1 si déjà réclamé
            last = int(v)
            streak = 1 if last > 0 else 0
        data[str(k)] = {"last": last, "streak": streak}
    return data

def _save_daily(data: Dict[str, dict]) -> None:
    _atomic_write(DAILY_DB_PATH, data)
    
def _format_cooldown(secs: float) -> str:
    s = int(round(secs))
    days, s = divmod(s, 86400)
    hours, s = divmod(s, 3600)
    minutes, s = divmod(s, 60)
    parts = []
    if days: parts.append(f"{days}j")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if s or not parts: parts.append(f"{s}s")
    return " ".join(parts)
    
# ---------- Views helpers (timeout = griser les composants) ----------
class OwnedView(discord.ui.View):
    """View qui sait griser ses composants au timeout et restreindre l’usage à son auteur (optionnel)."""
    def __init__(self, author_id: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.author_id = author_id
        self.message: discord.Message | None = None  # rempli après l’envoi du message qui porte la View

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Optionnel: utile si un jour tu postes la View en non-ephemeral.
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Tu ne peux pas utiliser ce panneau.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        # Griser visuellement tous les composants quand la View expire
        for c in self.children:
            c.disabled = True
        try:
            if self.message:
                await self.message.edit(view=self)
        except Exception:
            pass

# --- Streak (récompenses et tolérance) ---
DAILY_COOLDOWN = 24 * 60 * 60  # 24h
STREAK_MAX = 4
STREAK_REWARDS = {1: 2, 2: 3, 3: 4, 4: 5}
STREAK_GRACE = 2 * DAILY_COOLDOWN  # 48h
STREAK_WARNING_BEFORE = 30 * 60  # 30 minutes avant expiration

@tree.command(name="quests_preview", description="Aperçu des quêtes d'un membre (admin)")
@guilds_decorator()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(membre="Le membre à prévisualiser")
async def quests_preview_cmd(interaction: discord.Interaction, membre: Optional[discord.Member] = None):
    await interaction.response.defer(ephemeral=True)

    target: discord.Member = membre or (interaction.user if isinstance(interaction.user, discord.Member) else None)  # type: ignore
    if not target or not isinstance(target, discord.Member):
        await interaction.followup.send("Impossible d’identifier le membre cible.", ephemeral=True)
        return

    date_key = _today_str()
    week_key = _week_str()
    qcfg     = _load_quests()

    # Bonus multiplicateur/tier du MEMBRE ciblé (pour simuler son affichage réel)
    user_mul = points_multiplier_for(target)
    tier_key, tier_label, _ = tier_info(target)

    # --- helpers locaux de rendu (copie allégée de /quests) ---
    def _render_section(title: str, cfg_map: dict, user_map: dict, user_mul: float) -> str:
        lines = [f"__**{title}**__"]
        for key, q in cfg_map.items():
            name    = q.get("name", key)
            target  = int(q.get("target", 0))
            reward  = int(q.get("reward", 0))
            slot    = user_map.get(key, {"progress": 0, "claimed": 0})
            prog    = int(slot.get("progress", 0))
            claimed = int(slot.get("claimed", 0))
            maxc    = int(q.get("max_claims_per_reset", 1))

            # Barre
            done = min(prog, target)
            fill = int((done / max(1, target)) * 20) if target > 0 else (20 if claimed < maxc and prog >= target else 0)
            bar  = "█" * fill + "—" * (20 - fill)

            # Statut
            if claimed >= maxc:
                status = "✅ réclamée"
            elif prog >= target:
                status = "🎁 prête"
            else:
                status = "⏳ en cours"

            reward_txt = f"+{reward} pts"
            if user_mul > 1.0:
                est = int(round(reward * user_mul))
                reward_txt += f" *(≈ **+{est}** avec bonus)*"

            lines.append(f"**{name}** — {reward_txt}\n`{bar}` {min(prog,target)}/{target} • {status}")
        return "\n".join(lines)

    def _make_embed(d_map, w_map) -> discord.Embed:
        desc = (
            _render_section(f"Quotidien — {date_key}",  qcfg.get("daily", {}),  d_map, user_mul) + "\n\n" +
            _render_section(f"Hebdomadaire — {week_key}", qcfg.get("weekly", {}), w_map, user_mul)
        )
        note = ""
        if user_mul > 1.0 and tier_label:
            note = f"\n\n*Bonus palier actif : {tier_label} ×{user_mul:.2g} — appliqué **sur la somme totale** au moment de la réclamation.*"
        embed = discord.Embed(title="🗺️ Quêtes", description=desc + note, color=discord.Color.blurple())
        embed.set_footer(text="Daily = jour UTC • Weekly = semaine ISO (lun→dim, UTC).")
        return embed

    # Vue (affichage-only : pas de bouton "Réclamer")
    class PreviewView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
            btn_refresh = discord.ui.Button(emoji="🔄", style=discord.ButtonStyle.secondary)
            self.add_item(btn_refresh)

            async def ref_cb(i: discord.Interaction):
                async with _quests_progress_lock:
                    pdb2   = _load_quests_progress()
                    d_map2 = _get_user_all_quests(pdb2, "daily",  date_key, i.guild.id, target.id)   # type: ignore
                    w_map2 = _get_user_all_quests(pdb2, "weekly", week_key,  i.guild.id, target.id)   # type: ignore
                await i.response.edit_message(embed=_make_embed(d_map2, w_map2), view=self)

            btn_refresh.callback = ref_cb  # type: ignore

    # Charge la progression du MEMBRE ciblé et affiche
    async with _quests_progress_lock:
        pdb   = _load_quests_progress()
        d_map = _get_user_all_quests(pdb, "daily",  date_key, interaction.guild.id, target.id)  # type: ignore
        w_map = _get_user_all_quests(pdb, "weekly", week_key,  interaction.guild.id, target.id)  # type: ignore

    await interaction.followup.send(embed=_make_embed(d_map, w_map), view=PreviewView(), ephemeral=True)

@tree.command(name="quests", description="Voir les quêtes quotidiennes et hebdomadaires, et réclamer les récompenses.")
@guilds_decorator()
async def quests_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    date_key = _today_str()
    week_key = _week_str()
    qcfg     = _load_quests()

    # --- BONUS PALIER utilisateur
    user_mul = 1.0
    tier_key = tier_label = None
    if isinstance(interaction.user, discord.Member):
        user_mul = points_multiplier_for(interaction.user)
        tier_key, tier_label, _ = tier_info(interaction.user)

    async with _quests_progress_lock:
        pdb = _load_quests_progress()
        # ⚠️ on s’assure que l’utilisateur a bien un tirage actif
        assigned_daily  = _ensure_assignments(pdb, qcfg, "daily",  date_key, interaction.guild.id, interaction.user.id, k=3)
        assigned_weekly = _ensure_assignments(pdb, qcfg, "weekly", week_key,  interaction.guild.id, interaction.user.id, k=3)
        _save_quests_progress(pdb)
        
    qcfg_display = {
        "daily":  {k:v for k,v in qcfg.get("daily",  {}).items() if k in assigned_daily},
        "weekly": {k:v for k,v in qcfg.get("weekly", {}).items() if k in assigned_weekly},
    }
    # --- Rendu sections (ajout de user_mul)
    def _render_section(title: str, qcat: dict, u_map: dict, mul: float) -> str:
        if not qcat:
            return f"__{title}__\n_Aucune quête._"
        lines = [f"__{title}__"]
        for key, q in qcat.items():
            name    = q.get("name", key)
            target  = int(q.get("target", 0))
            reward  = int(q.get("reward", 0))
            slot    = u_map.get(key, {"progress": 0, "claimed": 0})
            prog    = int(slot.get("progress", 0))
            claimed = int(slot.get("claimed", 0))
            maxc    = int(q.get("max_claims_per_reset", 1))
            done    = prog >= target

            # Barre de progression
            bar_w   = 12
            filled  = max(0, min(bar_w, int(round((prog/target)*bar_w))) if target>0 else bar_w)
            bar     = "▰"*filled + "▱"*(bar_w-filled)

            # Texte statut
            status  = "✅ **Prête à réclamer**" if (done and claimed < maxc) else ("🟡 En cours" if not done else "💠 Déjà réclamée")

            # --- Affichage bonus estimé (arrondi à l'unité, comme au claim)
            reward_txt = f"**+{reward}** pts"
            if mul > 1.0:
                est = int(round(reward * user_mul))
                reward_txt += f" *(≈ **+{est}** avec bonus)*"

            lines.append(
                f"**{name}** — {reward_txt}\n"
                f"`{bar}` {min(prog,target)}/{target} • {status}"
            )
        return "\n".join(lines)

    # Compose l'embed (et note sur le bonus)
    def _make_embed(d_map, w_map) -> discord.Embed:
        desc = (
            _render_section(f"Quotidien — {date_key}",  qcfg_display.get("daily", {}),  d_map, user_mul) + "\n\n" +
            _render_section(f"Hedbdomadaire — {week_key}", qcfg_display.get("weekly", {}), w_map, user_mul)
        )
        note = ""
        if user_mul > 1.0 and tier_label:
            note = f"\n\n*Bonus palier actif : {tier_label} ×{user_mul:.2g} — appliqué **sur la somme totale** au moment de la réclamation.*"
        embed = discord.Embed(title="🗺️ Quêtes", description=desc + note, color=discord.Color.blurple())
        embed.set_footer(text="Daily = jour UTC • Weekly = semaine ISO (lun→dim, UTC).")
        return embed

    date_key = _today_str()
    week_key = _week_str()
    async with _quests_progress_lock:
        pdb   = _load_quests_progress()
        d_map = _get_user_all_quests(pdb, "daily",  date_key, interaction.guild.id, interaction.user.id)
        w_map = _get_user_all_quests(pdb, "weekly", week_key,  interaction.guild.id, interaction.user.id)
    
    # maintenant ces variables existent :
    embed = _make_embed(d_map, w_map)

    class QuestsView(OwnedView):
        def __init__(self, author_id: int):
            super().__init__(author_id=author_id, timeout=90)
            btn_claim = discord.ui.Button(label="🎁 Réclamer ce qui est prêt", style=discord.ButtonStyle.success)
            btn_ref   = discord.ui.Button(emoji="🔄", style=discord.ButtonStyle.secondary)
            self.add_item(btn_claim); self.add_item(btn_ref)

            async def claim_cb(i: discord.Interaction):
                # … dans claim_cb(...)
                gained = 0
                async with _quests_progress_lock:
                    pdb = _load_quests_progress()
                    qcfg = _load_quests()
                
                    assigned_daily  = _get_assigned(pdb, "daily",  date_key, i.guild.id, i.user.id)
                    assigned_weekly = _get_assigned(pdb, "weekly", week_key,  i.guild.id, i.user.id)
                
                    u_daily  = _get_user_all_quests(pdb, "daily",  date_key, i.guild.id, i.user.id)
                    u_weekly = _get_user_all_quests(pdb, "weekly", week_key,  i.guild.id, i.user.id)
                
                    claimed_count = 0  # nombre de quêtes réellement réclamées (pour méta)
                
                    # DAILY
                    for key, q in qcfg.get("daily", {}).items():
                        if key not in assigned_daily:
                            continue
                        target = int(q.get("target", 0))
                        reward = int(q.get("reward", 0))
                        maxc   = int(q.get("max_claims_per_reset", 1))
                        slot   = u_daily.setdefault(key, {"progress": 0, "claimed": 0})
                        if slot.get("progress", 0) >= target and slot.get("claimed", 0) < maxc:
                            slot["claimed"] = int(slot.get("claimed", 0)) + 1
                            gained += reward
                            claimed_count += 1
                
                    # WEEKLY
                    for key, q in qcfg.get("weekly", {}).items():
                        if key not in assigned_weekly:
                            continue
                        target = int(q.get("target", 0))
                        reward = int(q.get("reward", 0))
                        maxc   = int(q.get("max_claims_per_reset", 1))
                        slot   = u_weekly.setdefault(key, {"progress": 0, "claimed": 0})
                        if slot.get("progress", 0) >= target and slot.get("claimed", 0) < maxc:
                            slot["claimed"] = int(slot.get("claimed", 0)) + 1
                            gained += reward
                            claimed_count += 1
                
                    # ➕ Meta-objectif hebdo: quests_completed
                    # on ajoute 'claimed_count' dans la weekly "quests_completed" SI elle est assignée
                    for meta_key, meta_q in qcfg.get("weekly", {}).items():
                        if meta_q.get("type") == "quests_completed" and meta_key in assigned_weekly:
                            meta_slot = _ensure_user_quest_slot(pdb, "weekly", week_key, i.guild.id, i.user.id, meta_key)
                            meta_slot["progress"] = int(meta_slot.get("progress", 0)) + claimed_count
                
                    _save_quests_progress(pdb)

                if gained > 0 and isinstance(i.user, discord.Member):
                    gained = int(round(gained * points_multiplier_for(i.user)))
                    new_total = await add_points(i.user.id, gained)
                    # refresh visuel
                    async with _quests_progress_lock:
                        pdb2 = _load_quests_progress()
                        d2 = _get_user_all_quests(pdb2, "daily",  date_key, i.guild.id, i.user.id)  # type: ignore
                        w2 = _get_user_all_quests(pdb2, "weekly", week_key, i.guild.id, i.user.id)  # type: ignore
                    await i.response.edit_message(embed=_make_embed(d2, w2), view=self)
                    await i.followup.send(f"✅ **+{gained}** pts → total **{new_total}**.", ephemeral=True)
                else:
                    await i.response.edit_message(embed=embed, view=self)
                    try:
                        await i.followup.send("Rien à réclamer pour l’instant.", ephemeral=True)
                    except Exception:
                        pass

            async def ref_cb(i: discord.Interaction):
                async with _quests_progress_lock:
                    pdb2 = _load_quests_progress()
                    d2 = _get_user_all_quests(pdb2, "daily",  date_key, i.guild.id, i.user.id)  # type: ignore
                    w2 = _get_user_all_quests(pdb2, "weekly", week_key, i.guild.id, i.user.id)  # type: ignore
                await i.response.edit_message(embed=_make_embed(d2, w2), view=self)

            btn_claim.callback = claim_cb
            btn_ref.callback   = ref_cb

    view = QuestsView(author_id=interaction.user.id)
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    try:
        view.message = await interaction.original_response()
    except Exception:
        pass

@tree.command(name="daily", description="Réclame ta récompense quotidienne.")
@guilds_decorator()
async def daily_cmd(interaction: discord.Interaction):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    uid = str(interaction.user.id)

    async with _daily_lock:
        daily = _load_daily()
        state = daily.get(uid, {"last": 0, "streak": 0})
        last = int(state.get("last", 0))
        streak = int(state.get("streak", 0))
        elapsed = now_ts - last if last else None

        # encore en cooldown 24h
        if last and elapsed is not None and elapsed < DAILY_COOLDOWN:
            remain = DAILY_COOLDOWN - elapsed
            joli = _format_cooldown(remain)
            expire = now_ts + remain
            return await interaction.response.send_message(
                f"⏳ Tu as déjà pris ton daily. Réessaie dans {joli} ( <t:{expire}:R> ).",
                ephemeral=True
            )

        # Déterminer le nouveau streak :
        # - si première prise : streak=1
        # - si pris après 24h et avant 48h : streak+1 (plafonné à 4)
        # - si >48h (jour manqué) : reset à 1
        if not last:
            new_streak = 1
        else:
            if elapsed <= 2 * DAILY_COOLDOWN:
                new_streak = min(streak + 1, STREAK_MAX)
            else:
                new_streak = 1  # jour manqué -> reset

        reward = STREAK_REWARDS.get(new_streak, STREAK_REWARDS[STREAK_MAX])
        reward += daily_flat_bonus_for(interaction.user)  # +1/+2/+4 selon palier
        reward = max(0, reward)

        # Créditer & enregistrer
        new_total = await add_points(interaction.user.id, reward)
        daily[uid] = {"last": now_ts, "streak": new_streak, "warned": False}
        _save_daily(daily)

    # Texte sympa
    streak_bar = "▰" * new_streak + "▱" * (STREAK_MAX - new_streak)
    next_hint = "Reste à **5** si tu continues !" if new_streak == STREAK_MAX else f"Demain: **{STREAK_REWARDS[new_streak+1]}** pts"
    await interaction.response.send_message(
        f"🗓️ Daily pris ! **+{reward}** pts → total **{new_total}**.\n"
        f"🔥 Streak: **{new_streak}/{STREAK_MAX}** `{streak_bar}` — {next_hint}",
        ephemeral=True
    )
    # Incrémenter la (ou les) quêtes "daily_claims_week"
    async with _quests_progress_lock:
        pdb   = _load_quests_progress()
        qcfg  = _load_quests()
        week_key = _week_str()
        assigned_weekly = _ensure_assignments(pdb, qcfg, "weekly", week_key, interaction.guild.id, interaction.user.id, k=3)
    
        for qkey, q in qcfg.get("weekly", {}).items():
            if qkey not in assigned_weekly:
                continue
            if q.get("type") == "daily_claims_week":
                slot   = _ensure_user_quest_slot(pdb, "weekly", week_key, interaction.guild.id, interaction.user.id, qkey)
                target = int(q.get("target", 5))
                slot["progress"] = min(target, int(slot.get("progress", 0)) + 1)
    
        _save_quests_progress(pdb)
    # Marquer la quête d'usage de commande pour /daily
    await _mark_command_use(interaction.guild.id, interaction.user.id, "/daily")
    

@tree.command(name="purchases", description="Voir l'historique d'achats boutique.")
@guilds_decorator()
@app_commands.describe(membre="(Optionnel) Le membre dont afficher les achats")
async def purchases_cmd(
    interaction: discord.Interaction,
    membre: discord.Member | None = None
):
    target = membre or interaction.user  # type: ignore

    # Si on essaie de voir quelqu'un d'autre sans être admin → refus
    if target.id != interaction.user.id and not interaction.user.guild_permissions.administrator:  # type: ignore
        return await interaction.response.send_message(
            "⛔ Tu ne peux voir que **tes** achats. (Réservé aux admins pour les autres.)",
            ephemeral=True
        )

    async with _purchases_lock:
        p = _load_purchases()
    items = p.get(str(target.id), {})

    if not items:
        return await interaction.response.send_message(
            f"🧾 Aucun achat enregistré pour **{target.display_name}**.",
            ephemeral=True
        )

    # Noms jolis depuis le shop
    async with _shop_lock:
        shop = _load_shop()

    lines = [f"**Achats de {target.display_name} :**"]
    for key, count in items.items():
        label = shop.get(key, {}).get("name", key)
        lines.append(f"- {label} (`{key}`) × **{count}**")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

PER_PAGE = 15  # éléments par page
class InviteListView(discord.ui.View):
    def __init__(self, author_id: int, cible: discord.Member, total: int, rows: List[str]):
        super().__init__(timeout=120)  # 2 min d'interactions possibles
        self.author_id = author_id
        self.cible = cible
        self.total = total
        self.rows = rows
        self.page = 0
        self.max_page = max((len(rows) - 1) // PER_PAGE, 0)
        self._sync_buttons_state()

    # Empêche les autres d'utiliser les boutons
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Seul·e l’auteur de la commande peut utiliser ces boutons.", ephemeral=True)
            return False
        return True

    def _slice(self) -> List[str]:
        start = self.page * PER_PAGE
        end = start + PER_PAGE
        return self.rows[start:end]

    def _make_embed(self) -> discord.Embed:
        lines = self._slice()
        more = max(0, len(self.rows) - ((self.page + 1) * PER_PAGE))
        desc_parts = [
            f"**Total invitations :** **{self.total}**",
            "**Invité·es :**" if lines else "_Aucun invité enregistré pour l’instant._"
        ]
        if lines:
            desc_parts.append("\n".join(lines))
        if more > 0:
            desc_parts.append(f"_… et encore **{more}** autre(s) hors page._")

        embed = discord.Embed(
            title=f"📨 Invitations — {self.cible.display_name}",
            description="\n\n".join(desc_parts),
            color=discord.Color.gold()
        )
        if self.rows:
            # Indique la pagination (ex: Page 1/4 – éléments 1–15 sur 53)
            start_index = self.page * PER_PAGE + 1
            end_index = min((self.page + 1) * PER_PAGE, len(self.rows))
            embed.set_footer(text=f"Page {self.page + 1}/{self.max_page + 1} — éléments {start_index}–{end_index} sur {len(self.rows)} • ID: {self.cible.id}")
        else:
            embed.set_footer(text=f"ID: {self.cible.id}")
        return embed

    def _sync_buttons_state(self):
        # Active/désactive selon la page
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "first":
                    child.disabled = (self.page <= 0)
                elif child.custom_id == "prev":
                    child.disabled = (self.page <= 0)
                elif child.custom_id == "next":
                    child.disabled = (self.page >= self.max_page)
                elif child.custom_id == "last":
                    child.disabled = (self.page >= self.max_page)

    async def _redraw(self, interaction: discord.Interaction):
        self._sync_buttons_state()
        await interaction.response.edit_message(embed=self._make_embed(), view=self)

    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary, custom_id="first")
    async def go_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = 0
        await self._redraw(interaction)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.secondary, custom_id="prev")
    async def go_prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
        await self._redraw(interaction)

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.secondary, custom_id="next")
    async def go_next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.max_page:
            self.page += 1
        await self._redraw(interaction)

    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary, custom_id="last")
    async def go_last(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = self.max_page
        await self._redraw(interaction)

    @discord.ui.button(label="Fermer", style=discord.ButtonStyle.danger, custom_id="close")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)

    async def on_timeout(self) -> None:
        # Désactive tout à la fin du délai
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

@tree.command(name="invites", description="Voir le nombre d'invitations d'un membre, avec liste paginée des invités.")
@guilds_decorator()
@app_commands.describe(membre="Le membre (si vide, toi)")
async def invites_cmd(interaction: discord.Interaction, membre: discord.Member | None = None):
    cible = membre or interaction.user  # type: ignore

    # --- Récupération des données depuis invites.json ---
    async with _invites_lock:
        db = _load_invites()
        total = int(db.get("counts", {}).get(str(cible.id), 0))
        # refs: { member_id(str): inviter_id(int) }
        invitee_ids = [int(mid) for mid, iid in db.get("refs", {}).items() if int(iid) == int(cible.id)]

    # --- Préparation des lignes affichées ---
    rows: List[Tuple[str, str]] = []
    for mid in invitee_ids:
        m = interaction.guild.get_member(mid)  # type: ignore
        if m:
            # Affiche mention + ID
            rows.append((m.display_name.lower(), f"- {m.mention} (`{m.id}`)"))
        else:
            # Membre peut avoir quitté; on garde la mention par ID
            rows.append((f"zzz-{mid}", f"- <@{mid}> (`{mid}`)"))

    rows.sort(key=lambda x: x[0])
    pretty_rows = [r[1] for r in rows]

    view = InviteListView(author_id=interaction.user.id, cible=cible, total=total, rows=pretty_rows)

    # Envoi initial
    await interaction.response.send_message(embed=view._make_embed(), view=view, ephemeral=True)

@tree.command(name="ping", description="Test rapide de réponse du bot.")
@guilds_decorator()
async def ping_cmd(interaction: discord.Interaction):
    await interaction.response.send_message("Pong 🏓", ephemeral=True)

@tree.command(name="addpoints", description="Ajouter des points à un membre (admin).")
@guilds_decorator()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(membre="Le membre à créditer", points="Nombre de points à ajouter (>=1)")
async def addpoints_cmd(interaction: discord.Interaction, membre: discord.Member, points: app_commands.Range[int, 1, 1_000_000]):
    new_total = await add_points(membre.id, int(points))
    await interaction.response.send_message(f"✅ **{membre.display_name}** a maintenant **{new_total}** points (+{int(points)}).")
    await _send_admin_log(
        interaction.guild,
        interaction.user,
        "addpoints",
        membre=f"{membre} ({membre.id})",
        points=int(points),
        new_total=new_total
    )

@tree.command(name="removepoints", description="Retirer des points à un membre (admin).")
@guilds_decorator()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(membre="Le membre à débiter", points="Nombre de points à retirer (>=1)")
async def removepoints_cmd(interaction: discord.Interaction, membre: discord.Member, points: app_commands.Range[int, 1, 1_000_000]):
    new_total = await remove_points(membre.id, int(points))
    await interaction.response.send_message(f"✅ **{membre.display_name}** a maintenant **{new_total}** points (-{int(points)}).")
    await _send_admin_log(
        interaction.guild,
        interaction.user,
        "removepoints",
        membre=f"{membre} ({membre.id})",
        points=int(points),
        new_total=new_total
    )

@tree.command(name="mp", description="Envoie un message privé à un membre ou à tout le serveur. (admin)")
@guilds_decorator()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    cible="Le membre à qui envoyer le message (laisser vide pour tout le serveur)",
    message="Le contenu du message à envoyer"
)
async def mp_cmd(
    interaction: discord.Interaction,
    cible: discord.Member | None,
    message: str
):
    """Envoie un message privé à un membre ou à tout le serveur (admin)."""
    guild = interaction.guild
    sender = interaction.user

    # --- MP individuel ---
    if cible:
        try:
            await cible.send(message)
            await interaction.response.send_message(
                f"✅ Message envoyé à {cible.mention} en MP.", ephemeral=True
            )
            await _send_admin_log(guild, sender, "mp.send", cible=f"{cible} ({cible.id})", scope="unique")
        except discord.Forbidden:
            await interaction.response.send_message(
                f"⚠️ Impossible d’envoyer un message à {cible.mention} (MP fermés).",
                ephemeral=True
            )
        return

    # --- MP à tout le serveur ---
    class ConfirmView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)

        @discord.ui.button(label="✅ Confirmer l’envoi à tout le serveur", style=discord.ButtonStyle.danger)
        async def confirm(self, i: discord.Interaction, _):
            await i.response.defer() 
            sent = 0
            failed = 0
            for m in guild.members:
                if m.bot:
                    continue
                try:
                    await m.send(message)
                    sent += 1
                    await asyncio.sleep(0.2)  # éviter le rate-limit
                except discord.Forbidden:
                    failed += 1
                except Exception:
                    failed += 1
            await i.edit_original_response(
                content=f"📨 Envoi terminé ! ✅ {sent} succès / ⚠️ {failed} échecs (MP fermés ou erreurs).",
                view=None
            )
            await _send_admin_log(
                guild, sender, "mp.broadcast",
                total_members=len(guild.members),
                sent=sent,
                failed=failed
            )

        @discord.ui.button(label="❌ Annuler", style=discord.ButtonStyle.secondary)
        async def cancel(self, i: discord.Interaction, _):
            await i.response.edit_message(content="Envoi annulé.", view=None)

    await interaction.response.send_message(
        "⚠️ Tu es sur le point d’envoyer **un message privé à tout le serveur**.\n"
        "Clique sur **Confirmer** pour lancer l’envoi (cela peut prendre un moment).",
        view=ConfirmView(),
        ephemeral=True
    )

@tree.command(name="setpoints", description="Définir le solde exact d'un membre (admin).")
@guilds_decorator()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(membre="Le membre", points="Nouveau solde (>=0)")
async def setpoints_cmd(interaction: discord.Interaction, membre: discord.Member, points: app_commands.Range[int,0,1_000_000]):
    async with _points_lock:
        data = _load_points()
        data[str(membre.id)] = int(points)
        _save_points(data)
    await interaction.response.send_message(f"🧮 Solde de **{membre.display_name}** fixé à **{int(points)}** pts.", ephemeral=True)
    await _send_admin_log(interaction.guild, interaction.user, "setpoints",
                          membre=f"{membre} ({membre.id})", points=int(points))

# ---------- Classement paginé ----------

def _medal(idx: int) -> str:
    return "🥇" if idx == 0 else ("🥈" if idx == 1 else ("🥉" if idx == 2 else f"#{idx+1}"))

def _progress_bar(value: int, top: int, width: int = 10) -> str:
    if top <= 0:
        return "▱" * width
    filled = int(round((value / top) * width))
    filled = max(0, min(width, filled))
    return "▰" * filled + "▱" * (width - filled)

async def _full_leaderboard(guild: discord.Guild) -> list[dict]:
    """Retourne une liste triée: [{uid, pts, name, mention, in_guild}]"""
    # Charge toutes les banques de points
    data = _load_points()  # pas besoin de lock en lecture seule si on accepte un petit "lag"
    # Trie par points desc puis uid pour stabilité
    pairs = sorted(((int(uid), int(pts)) for uid, pts in data.items()),
                   key=lambda x: (-x[1], x[0]))
    results: list[dict] = []
    for uid, pts in pairs:
        member = guild.get_member(uid)
        if member:
            name = member.display_name
            mention = member.mention
            in_guild = True
        else:
            # essaie de récupérer un nom propre
            try:
                user = await guild._state.http.get_user(uid)  # petit trick low-level, sinon fetch_user
                name = user.get("username", f"Utilisateur {uid}")
            except Exception:
                name = f"Utilisateur {uid}"
            mention = f"<@{uid}>"
            in_guild = False
        results.append({"uid": uid, "pts": pts, "name": name, "mention": mention, "in_guild": in_guild})
    return results

def _render_lb_page(guild: discord.Guild, rows: list[dict], page: int, page_size: int,
                    viewer_id: int | None = None) -> discord.Embed:
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    slice_ = rows[start:start + page_size]
    top_score = rows[0]["pts"] if rows else 0

    lines: list[str] = []
    for local_idx, row in enumerate(slice_):
        global_idx = start + local_idx
        medal = _medal(global_idx)
        bar = _progress_bar(row["pts"], top_score, 12)
        faded = " _(hors serveur)_" if not row["in_guild"] else ""
        you = " **(toi)**" if viewer_id and row["uid"] == viewer_id else ""
        lines.append(f"{medal} — **{row['name']}**{you}{faded}\n`{bar}` {row['pts']} pts")

    if not lines:
        desc = "_Aucune donnée pour le moment._"
    else:
        desc = "\n\n".join(lines)

    title = f"🏆 Classement — Page {page+1}/{total_pages}"
    embed = discord.Embed(title=title, description=desc, color=discord.Color.gold())
    embed.set_footer(text=f"Total entrées: {total} • Taille page: {page_size}")
    return embed

class LeaderboardView(OwnedView):
    def __init__(self, author_id: int, guild: discord.Guild, rows: list[dict], page: int, page_size: int):
        super().__init__(author_id=author_id, timeout=120)
        self.guild = guild
        self.rows = rows
        self.page = page
        self.page_size = page_size
        self.total_pages = max(1, (len(self.rows) + self.page_size - 1) // self.page_size)
        self.update_children()

    def update_children(self):
        self.clear_items()
        # Boutons de nav
        btn_first = discord.ui.Button(emoji="⏮", style=discord.ButtonStyle.secondary)
        btn_prev  = discord.ui.Button(emoji="◀️", style=discord.ButtonStyle.secondary)
        btn_next  = discord.ui.Button(emoji="▶️", style=discord.ButtonStyle.secondary)
        btn_last  = discord.ui.Button(emoji="⏭", style=discord.ButtonStyle.secondary)
        btn_refresh = discord.ui.Button(emoji="🔄", style=discord.ButtonStyle.secondary)
        btn_my = discord.ui.Button(label="🔎 Mon rang", style=discord.ButtonStyle.primary)
        btn_goto = discord.ui.Button(label="Aller à la page…", style=discord.ButtonStyle.secondary)
        btn_close = discord.ui.Button(label="Fermer", style=discord.ButtonStyle.danger)

        btn_first.disabled = self.page <= 0
        btn_prev.disabled  = self.page <= 0
        btn_next.disabled  = self.page >= (self.total_pages - 1)
        btn_last.disabled  = self.page >= (self.total_pages - 1)

        async def _edit(i: discord.Interaction):
            embed = _render_lb_page(self.guild, self.rows, self.page, self.page_size, viewer_id=i.user.id)
            await i.response.edit_message(embed=embed, view=self)

        async def first_cb(i: discord.Interaction): self.page = 0; await _edit(i)
        async def prev_cb(i: discord.Interaction):  self.page = max(0, self.page-1); await _edit(i)
        async def next_cb(i: discord.Interaction):  self.page = min(self.total_pages-1, self.page+1); await _edit(i)
        async def last_cb(i: discord.Interaction):  self.page = self.total_pages-1; await _edit(i)

        async def refresh_cb(i: discord.Interaction):
            # Recharge frais (peut changer entre-temps)
            new_rows = await _full_leaderboard(self.guild)
            self.rows = new_rows
            self.total_pages = max(1, (len(self.rows)+self.page_size-1)//self.page_size)
            self.page = max(0, min(self.page, self.total_pages-1))
            await _edit(i)

        async def myrank_cb(i: discord.Interaction):
            # Trouver la position de l'utilisateur
            idx = next((n for n, r in enumerate(self.rows) if r["uid"] == i.user.id), None)
            if idx is None:
                # pas dans la liste (0 point ?)
                try:
                    await i.response.send_message("Tu n’apparais pas encore au classement (aucun point ?).", ephemeral=True)
                except Exception:
                    pass
                return
            self.page = idx // self.page_size
            await _edit(i)

        async def goto_cb(i: discord.Interaction):
            class GotoModal(discord.ui.Modal, title="Aller à la page"):
                page_field = discord.ui.TextInput(label="Numéro de page", placeholder=f"1..{self.total_pages}", min_length=1, max_length=5)
                async def on_submit(self, mi: discord.Interaction):
                    try:
                        p = int(str(self.page_field))
                        if p < 1 or p > self.parent.total_pages:
                            raise ValueError
                    except Exception:
                        return await mi.response.send_message(f"❌ Page invalide. (1..{self.parent.total_pages})", ephemeral=True)
                    self.parent.page = p-1
                    embed = _render_lb_page(self.parent.guild, self.parent.rows, self.parent.page, self.parent.page_size, viewer_id=mi.user.id)
                    await mi.response.edit_message(embed=embed, view=self.parent)
            modal = GotoModal()
            modal.parent = self  # pour accéder à la vue depuis le modal
            await i.response.send_modal(modal)

        async def close_cb(i: discord.Interaction):
            await i.response.edit_message(content="Classement fermé.", embed=None, view=None)

        btn_first.callback = first_cb
        btn_prev.callback  = prev_cb
        btn_next.callback  = next_cb
        btn_last.callback  = last_cb
        btn_refresh.callback = refresh_cb
        btn_my.callback    = myrank_cb
        btn_goto.callback  = goto_cb
        btn_close.callback = close_cb

        self.add_item(btn_first)
        self.add_item(btn_prev)
        self.add_item(btn_next)
        self.add_item(btn_last)
        self.add_item(btn_refresh)
        self.add_item(btn_my)
        self.add_item(btn_goto)
        self.add_item(btn_close)

@tree.command(name="classement", description="Afficher le classement des points.")
@guilds_decorator()
@app_commands.describe(
    page="Page à afficher (défaut 1)",
    taille="Taille de page (5 à 25, défaut 10)"
)
async def classement_cmd(
    interaction: discord.Interaction,
    page: app_commands.Range[int, 1, 10_000] = 1,
    taille: app_commands.Range[int, 5, 25] = 10
):
    await interaction.response.defer(ephemeral=False)
    rows = await _full_leaderboard(interaction.guild)  # type: ignore
    if not rows:
        return await interaction.followup.send("Aucun point enregistré pour le moment.")

    page0 = max(0, page - 1)
    embed = _render_lb_page(interaction.guild, rows, page0, taille, viewer_id=interaction.user.id)  # type: ignore
    view = LeaderboardView(author_id=interaction.user.id, guild=interaction.guild, rows=rows, page=page0, page_size=taille)  # type: ignore
    msg = await interaction.followup.send(embed=embed, view=view)
    try:
        view.message = msg
    except Exception:
        pass

@tree.command(name="profile", description="Affiche un profil (points, achats, invites).")
@guilds_decorator()
@app_commands.describe(membre="(Optionnel) Le membre dont afficher le profil")
async def profile_cmd(interaction: discord.Interaction, membre: discord.Member | None = None):
    target: discord.Member = membre or interaction.user  # type: ignore
    uid = str(target.id)

    # --- Données ---
    async with _points_lock:
        points_map = _load_points()
        pts = int(points_map.get(uid, 0))

    async with _purchases_lock:
        purchases_map = _load_purchases()
        user_purchases = purchases_map.get(uid, {})

    invites = await _get_invite_count(target.id)

    # Daily (streak + cooldown)
    last_ts = 0
    streak = 0
    try:
        async with _daily_lock:
            daily = _load_daily()
            st = daily.get(uid, {"last": 0, "streak": 0})
            last_ts = int(st.get("last", 0))
            streak = int(st.get("streak", 0))
    except Exception:
        pass

    now_ts = int(datetime.now(timezone.utc).timestamp())
    daily_eta_txt = "✅ Disponible"
    if last_ts:
        elapsed = now_ts - last_ts
        if elapsed < DAILY_COOLDOWN:
            remain = DAILY_COOLDOWN - elapsed
            daily_eta_txt = f"⏳ Dans { _format_cooldown(remain) } ( <t:{now_ts + remain}:R> )"

    # Achats (aperçu)
    async with _shop_lock:
        shop_snapshot = _load_shop()

    top_items = sorted(user_purchases.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:6]
    if top_items:
        pretty_items = []
        for key, qty in top_items:
            it = shop_snapshot.get(key, {})
            label = it.get("name", key)
            pretty_items.append(f"• **{label}** × **{qty}**")
        achats_preview = "\n".join(pretty_items)
    else:
        achats_preview = "_Aucun achat enregistré_"
    total_achats = sum(int(v) for v in user_purchases.values()) if user_purchases else 0

    # --- Palier & aura ---
    tier_key, tier_label, tier_perks = tier_info(target)

    # Couleur de l'embed : OR = doré (aura), sinon couleur du rôle le plus haut si dispo, sinon blurple
    if tier_key == "or":
        color = discord.Color.gold()
    else:
        color = target.top_role.color if getattr(target, "top_role", None) and target.top_role.color.value else discord.Color.blurple()

    # Titre (+ ✨ pour aura Or)
    title = f"👤 Profil — {target.display_name}"
    if tier_key == "or":
        title = f"👤 ✨ Profil — {target.display_name} ✨"

    # --- Embed ---
    embed = discord.Embed(title=title, color=color)
    embed.set_thumbnail(url=target.display_avatar.url)

    # Champs principaux
    embed.add_field(name="💰 Points", value=f"**{pts}**", inline=True)
    embed.add_field(name="🛒 Achats", value=f"**{total_achats}**", inline=True)
    embed.add_field(name="📨 Invitations", value=f"**{invites}**", inline=True)

    # Daily + streak (0 si grace window dépassée)
    streak_preview = streak
    if last_ts and (now_ts - last_ts) > STREAK_GRACE:
        streak_preview = 0
    embed.add_field(
        name="🗓️ Daily",
        value=f"{daily_eta_txt}\nStreak: **{streak_preview}/{STREAK_MAX}**",
        inline=True
    )

    # Dates
    if target.created_at:
        created_ts = int(target.created_at.replace(tzinfo=timezone.utc).timestamp())
        embed.add_field(name="🆔 Compte créé", value=f"<t:{created_ts}:D> (<t:{created_ts}:R>)", inline=True)
    if target.joined_at:
        joined_ts = int(target.joined_at.replace(tzinfo=timezone.utc).timestamp())
        embed.add_field(name="🚪 Arrivée serveur", value=f"<t:{joined_ts}:D> (<t:{joined_ts}:R>)", inline=True)

    # Achats (aperçu)
    embed.add_field(name="🧾 Détails achats (aperçu)", value=achats_preview, inline=False)

    # Palier + avantages + Hall of Fame
    if tier_label:
        embed.add_field(name="🎖️ Palier", value=tier_label, inline=True)
        
    if isinstance(target, discord.Member):
        disc = int(shop_discount_for(target)*100)
        if disc:
            embed.add_field(name="💸 Remise boutique", value=f"**-{disc}%**", inline=True)

    # Footer
    embed.set_footer(text=f"ID: {target.id}")
    
    # Ephemeral si on regarde son propre profil
    is_self = (target.id == interaction.user.id)
    await interaction.response.send_message(embed=embed, ephemeral=is_self)
    await _mark_command_use(interaction.guild.id, interaction.user.id, "/profile")

@tree.command(name="topinvites", description="Classement des invitations.")
@guilds_decorator()
@app_commands.describe(top="Combien d'utilisateurs afficher (défaut 10)")
async def topinvites_cmd(interaction: discord.Interaction, top: app_commands.Range[int,1,50]=10):
    async with _invites_lock:
        data = _load_invites().get("counts", {})
    if not data:
        return await interaction.response.send_message("Aucune invitation enregistrée.")
    pairs = sorted(((int(uid), c) for uid, c in data.items()), key=lambda x: x[1], reverse=True)[:top]
    lines = []
    for i,(uid,count) in enumerate(pairs,1):
        m = interaction.guild.get_member(uid) or (await bot.fetch_user(uid))  # type: ignore
        name = m.display_name if hasattr(m,"display_name") else getattr(m,"name","Utilisateur")
        lines.append(f"**#{i}** — {name} : **{count}**")
    await interaction.response.send_message(embed=discord.Embed(
        title=f"🏅 Top invites — Top {top}", description="\n".join(lines), color=discord.Color.gold()))

@tree.command(name="boutique", description="Ouvre la boutique pour dépenser tes points.")
@guilds_decorator()
async def boutique_cmd(interaction: discord.Interaction):
    PAGE_SIZE = 5

    # --- données fraîches ---
    async with _points_lock:
        points_data = _load_points()
        user_points = int(points_data.get(str(interaction.user.id), 0))
    async with _shop_lock:
        shop = _load_shop()
        
    user_discount = 0.0
    if isinstance(interaction.user, discord.Member):
        user_discount = shop_discount_for(interaction.user)
    # rien en boutique
    if not shop:
        return await interaction.response.send_message("La boutique est vide pour le moment.", ephemeral=True)

    # enrichissement items (reste/limite/achetable)
    enriched = []
    for key, it in shop.items():
        max_per   = int(it.get("max_per_user", -1))
        already   = await get_user_purchase_count(interaction.user.id, key)
        remaining = (max_per - already) if max_per >= 0 else -1
    
        base_cost = int(it.get("cost", 0))
        final_cost = max(1, int(round(base_cost * (1.0 - user_discount))))  # <<< remise appliquée
    
        affordable = user_points >= final_cost                                 # <<< test avec prix remisé
        role_id    = int(it.get("role_id", 0))
    
        badges = []
        if role_id:
            badges.append("🎖 rôle")
        if max_per >= 0:
            badges.append(f"🔢 {max_per} max")
        if remaining == 0:
            badges.append("⛔ limite atteinte")
        if user_discount > 0:
            badges.append(f"💸 -{int(user_discount*100)}%")                    # <<< badge remise
    
        enriched.append({
            "key": key,
            "name": it.get("name", key),
            "cost": final_cost,                                               # <<< on stocke le prix remisé
            "description": (it.get("description") or "").strip(),
            "role_id": role_id,
            "max_per": max_per,
            "already": already,
            "remaining": remaining,
            "affordable": affordable,
            "badges": " • ".join(badges) if badges else "—",
            "base_cost": base_cost,                                           # (optionnel) pour affichage comparatif
        })

    # tri par défaut: coût croissant
    def sort_items(items, mode: str):
        if mode == "price_desc":
            return sorted(items, key=lambda x: (-x["cost"], x["name"].lower()))
        if mode == "name":
            return sorted(items, key=lambda x: x["name"].lower())
        if mode == "remaining":
            # items illimités (= -1) en bas
            return sorted(items, key=lambda x: (x["remaining"] == -1, x["remaining"] if x["remaining"]!=-1 else 1_000_000))
        # default price_asc
        return sorted(items, key=lambda x: (x["cost"], x["name"].lower()))

    # rendu "carte" d'un item
    def render_card(i, it, balance: int):
        cost = it["cost"]
        have = min(balance, cost)
        filled = int((have / cost) * 10) if cost > 0 else 10
        bar = "▰" * filled + "▱" * (10 - filled) if cost > 0 else "──────────"
        lim_txt = "∞" if it["max_per"] < 0 else f"{max(0,it['max_per']-it['already'])}/{it['max_per']}"
        can_buy = it["affordable"] and (it["remaining"] != 0)
        status = "🟢 Achetable" if can_buy else ("🟡 Solde insuffisant" if not it["affordable"] else "🔴 Limite atteinte")
        role_txt = f" | rôle: <@&{it['role_id']}>" if it["role_id"] else ""
        desc = it["description"] or "_Aucune description_"
        old = f" ~~{it['base_cost']}~~" if ("base_cost" in it and it["base_cost"] > it["cost"]) else ""
        cost_line = f"— **{it['cost']}** pts{old}"
    
        return (
    f"""**{i}. {it['name']}** {cost_line}{role_txt}
    {desc}
    `{bar}`  •  {status}  •  limite: **{lim_txt}**
    *{it['badges']}*"""
        )


    def page_slice(items, page: int):
        start = page * PAGE_SIZE
        return items[start:start+PAGE_SIZE]

    # Vue navigateur
    class ShopBrowser(OwnedView):
        def __init__(self, author_id: int, items: list[dict], page: int = 0, sort_mode: str = "price_asc"):
            super().__init__(author_id=author_id, timeout=120)
            self.items_all = items
            self.sort_mode = sort_mode
            self.page = page
            self.update_children()

        # helpers
        async def _render_embed(self, user: discord.User | discord.Member, user_points: int):
            items_sorted = sort_items(self.items_all, self.sort_mode)
            page_items = page_slice(items_sorted, self.page)
            total_pages = max(1, (len(items_sorted)+PAGE_SIZE-1)//PAGE_SIZE)
            color = discord.Color.green() if user_points > 0 else discord.Color.dark_gray()
            title = f"🛒 Boutique — Page {self.page+1}/{total_pages}"
            remise_txt = ""
            if isinstance(user, discord.Member):
                d = shop_discount_for(user)
                if d > 0:
                    remise_txt = f" • Remise: **-{int(d*100)}%**"
            desc_top = f"**Solde : {user_points} pts**{remise_txt}\n"

            if page_items:
                lines = [render_card(i, it, user_points) for i, it in enumerate(page_items, start=1)]
                body = "\n\n".join(lines)
            else:
                body = "_Aucun item sur cette page._"
            embed = discord.Embed(title=title, description=desc_top + "\n" + body, color=color)
            embed.set_footer(text="Utilise le sélecteur pour choisir un article, puis confirme.")
            return embed
    
        def update_children(self):
            self.clear_items()
    
            # --- Select TRI ---
            sort_select = discord.ui.Select(
                placeholder="Trier…",
                min_values=1, max_values=1,
                options=[
                    discord.SelectOption(label="Prix ↑", value="price_asc", default=self.sort_mode=="price_asc"),
                    discord.SelectOption(label="Prix ↓", value="price_desc", default=self.sort_mode=="price_desc"),
                    discord.SelectOption(label="Nom", value="name", default=self.sort_mode=="name"),
                    discord.SelectOption(label="Restant", value="remaining", default=self.sort_mode=="remaining"),
                ]
            )
    
            async def sort_callback(interaction_inner: discord.Interaction):
                self.sort_mode = sort_select.values[0]
                self.page = 0
                # recharger le solde pour l'embed
                async with _points_lock:
                    d = _load_points()
                    me_pts = int(d.get(str(interaction_inner.user.id), 0))
                self.update_children()
                embed = await self._render_embed(interaction_inner.user, me_pts)
                await interaction_inner.response.edit_message(embed=embed, view=self)
    
            sort_select.callback = sort_callback
            self.add_item(sort_select)
    
            # --- Sélecteur d’achat (items page) ---
            page_items = page_slice(sort_items(self.items_all, self.sort_mode), self.page)
            options = []
            for idx, it in enumerate(page_items, start=1):
                label = f"{idx}. {it['name']}"
                suffix = "" if it["affordable"] and it["remaining"] != 0 else (" (limite)" if it["remaining"]==0 else " (cher)")
                options.append(discord.SelectOption(
                    label=label[:100],
                    description=f"{it['cost']} pts{suffix}"[:100],
                    value=it["key"]
                ))
            if not options:
                options = [discord.SelectOption(label="Aucun item sur cette page", value="__none__", default=True)]
    
            buy_select = discord.ui.Select(
                placeholder="Choisis un article à acheter…",
                min_values=1, max_values=1,
                options=options
            )
    
            async def buy_callback(interaction_inner: discord.Interaction):
                key = buy_select.values[0]
                if key == "__none__":
                    return await interaction_inner.response.send_message("Rien à acheter ici 🙂", ephemeral=True)
    
                async with _shop_lock:
                    snapshot = _load_shop()
                    item = snapshot.get(key)
                if not item:
                    return await interaction_inner.response.send_message("❌ Cet item n'existe plus.", ephemeral=True)
    
                cost = int(item.get("cost", 0))
                role_id = int(item.get("role_id", 0))
                max_per = int(item.get("max_per_user", -1))
                already = await get_user_purchase_count(interaction_inner.user.id, key)
                async with _points_lock:
                    d = _load_points()
                    me_pts = int(d.get(str(interaction_inner.user.id), 0))
                    
                disc = 0.0
                if isinstance(interaction_inner.user, discord.Member):
                    disc = shop_discount_for(interaction_inner.user)
                final_cost = max(1, int(round(cost * (1.0 - disc))))
    
                left = "∞" if max_per < 0 else f"{max(0, max_per-already)}"
                recap = [
                    f"**Article :** {item.get('name', key)}",
                    f"**Prix :** {final_cost} pts" + (f"  *(remise {int(disc*100)}% — {cost} → {final_cost})*" if disc > 0 else ""),
                ]
                if role_id:
                    recap.append(f"**Rôle :** <@&{role_id}>")
                if item.get("description"):
                    recap.append(f"**Description :** {item['description']}")
                if max_per >= 0:
                    recap.append(f"**Limite par utilisateur :** {max_per} (tu en as **{already}**, reste **{left}**)")
                recap.append(f"**Ton solde :** {me_pts} pts → **reste après achat :** {me_pts - final_cost} pts")
                
                embed = discord.Embed(title="🧾 Confirmer l’achat", description="\n".join(recap), color=discord.Color.orange())
                view = ConfirmBuy(
                    user_points=me_pts,
                    user_id=interaction_inner.user.id,
                    key=key,
                    item={"cost": cost, "role_id": role_id, "description": item.get("description",""), "name": item.get("name", key)},
                    already=already
                )
                view.final_cost = final_cost 
                await interaction_inner.response.send_message(embed=embed, view=view, ephemeral=True)
                try:
                    view.message = await interaction_inner.original_response()
                except Exception:
                    pass

            buy_select.callback = buy_callback
            self.add_item(buy_select)
    
            # --- Boutons navigation ---
            btn_prev = discord.ui.Button(label="◀️ Précédent", style=discord.ButtonStyle.secondary)
            btn_next = discord.ui.Button(label="Suivant ▶️", style=discord.ButtonStyle.secondary)
            btn_refresh = discord.ui.Button(label="🔄 Actualiser", style=discord.ButtonStyle.secondary)
            btn_close = discord.ui.Button(label="❌ Fermer", style=discord.ButtonStyle.danger)
    
            total_pages = max(1, (len(self.items_all) + PAGE_SIZE - 1)//PAGE_SIZE)
            btn_prev.disabled = self.page <= 0
            btn_next.disabled = self.page >= (total_pages - 1)
    
            async def prev_callback(interaction_inner: discord.Interaction):
                self.page = max(0, self.page - 1)
                async with _points_lock:
                    d = _load_points()
                    me_pts = int(d.get(str(interaction_inner.user.id), 0))
                self.update_children()
                embed = await self._render_embed(interaction_inner.user, me_pts)
                await interaction_inner.response.edit_message(embed=embed, view=self)
    
            async def next_callback(interaction_inner: discord.Interaction):
                total = max(1, (len(self.items_all) + PAGE_SIZE - 1)//PAGE_SIZE)
                self.page = min(total - 1, self.page + 1)
                async with _points_lock:
                    d = _load_points()
                    me_pts = int(d.get(str(interaction_inner.user.id), 0))
                self.update_children()
                embed = await self._render_embed(interaction_inner.user, me_pts)
                await interaction_inner.response.edit_message(embed=embed, view=self)
    
            async def refresh_callback(interaction_inner: discord.Interaction):
                async with _points_lock:
                    d = _load_points()
                    me_pts = int(d.get(str(interaction_inner.user.id), 0))
                # Recalculer "affordable" pour l'état visuel
                for it in self.items_all:
                    it["affordable"] = me_pts >= int(it["cost"])
                self.update_children()
                embed = await self._render_embed(interaction_inner.user, me_pts)
                await interaction_inner.response.edit_message(embed=embed, view=self)
    
            async def close_callback(interaction_inner: discord.Interaction):
                await interaction_inner.response.edit_message(content="Boutique fermée.", embed=None, view=None)
    
            btn_prev.callback = prev_callback
            btn_next.callback = next_callback
            btn_refresh.callback = refresh_callback
            btn_close.callback = close_callback
    
            self.add_item(btn_prev)
            self.add_item(btn_next)
            self.add_item(btn_refresh)
            self.add_item(btn_close)

    # Vue de confirmation (reprend ta logique existante)
    class ConfirmBuy(OwnedView):
        def __init__(self, user_points: int, user_id: int, key: str, item: dict, already: int):
            super().__init__(author_id=user_id, timeout=45)
            self.user_points = user_points
            self.user_id = user_id
            self.key = key
            self.item = item
            self.already = already
    
            # --- IMPORTANT : prix remisé pour le pré-check
            base_cost = int(item.get("cost", 0))
            disc = 0.0
            self.final_cost = base_cost

        async def on_timeout(self):
            # Appelle l’implémentation parent pour GRISER + EDIT le message
            await super().on_timeout()

        @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.success)
        async def confirm(self, i: discord.Interaction, _):
            async with _points_lock:
                d = _load_points()
                current_pts = int(d.get(str(self.user_id), 0))
            if current_pts < self.final_cost:
                return await i.response.send_message("❌ Solde insuffisant au moment de la confirmation.", ephemeral=True)
            await _handle_purchase(i, self.key)
            try:
                msg = await i.original_response()
                await msg.edit(view=None)
            except Exception:
                pass

        @discord.ui.button(label="Annuler", style=discord.ButtonStyle.danger)
        async def cancel(self, i: discord.Interaction, _):
            try:
                await i.response.edit_message(content="Achat annulé.", view=None)
            except Exception:
                try:
                    await i.response.send_message("Achat annulé.", ephemeral=True)
                except Exception:
                    pass

    # --- ouverture initiale ---
    view = ShopBrowser(author_id=interaction.user.id, items=list(enriched), page=0, sort_mode="price_asc")
    embed = await view._render_embed(interaction.user, user_points)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    # enregistrer le message pour pouvoir le griser au timeout
    try:
        view.message = await interaction.original_response()
    except Exception:
        pass
        
async def _try_add_role(member: discord.Member, role: discord.Role, reason: str) -> tuple[bool, str]:
    guild = member.guild
    me = guild.me
    if not me.guild_permissions.manage_roles:
        return False, "Le bot n’a pas la permission **Gérer les rôles**."
    if role >= me.top_role:
        return False, f"Le rôle **{role.name}** est au-dessus du rôle du bot."
    try:
        await member.add_roles(role, reason=reason)
        return True, ""
    except discord.Forbidden:
        return False, "Permission refusée par Discord."
    except Exception as e:
        return False, f"Erreur: {e!s}"

async def _handle_purchase(interaction: discord.Interaction, key: str):
    # Item
    async with _shop_lock:
        shop = _load_shop()
        item = shop.get(key)
    if not item:
        return await interaction.response.send_message("❌ Cet item n'existe plus.", ephemeral=True)

    base_cost = int(item["cost"])
    name = item["name"]
    role_id = int(item.get("role_id", 0))
    max_per = int(item.get("max_per_user", -1))
    already = await get_user_purchase_count(interaction.user.id, key)
    
    if max_per >= 0 and already >= max_per:
        return await interaction.response.send_message(
            f"❌ Tu as déjà acheté **{name}** le nombre maximum de fois autorisé ({max_per}).",
            ephemeral=True
        )
    
    # Refus si l'utilisateur a déjà le rôle (si item de rôle)
    if role_id:
        role = interaction.guild.get_role(role_id)
        if role and isinstance(interaction.user, discord.Member) and role in interaction.user.roles:
            return await interaction.response.send_message(
                f"❌ Tu as déjà le rôle **{role.name}**.",
                ephemeral=True
            )
    
    # >>> CALCUL REMISE CÔTÉ SERVEUR (SÉCURITÉ)
    disc = 0.0
    if isinstance(interaction.user, discord.Member):
        disc = shop_discount_for(interaction.user)
    cost = max(1, int(round(base_cost * (1.0 - disc))))
    
    # Débit points (avec le coût remisé)
    async with _points_lock:
        data = _load_points()
        user_points = int(data.get(str(interaction.user.id), 0))
        if user_points < cost:
            return await interaction.response.send_message(
                f"❌ Il te manque **{cost - user_points}** points pour acheter **{name}**.",
                ephemeral=True
            )
        remaining = user_points - cost
        data[str(interaction.user.id)] = remaining
        _save_points(data)

    # Récompense + logs
    role_id = int(item.get("role_id", 0))
    if role_id:
        role = interaction.guild.get_role(role_id)
        if role:
            ok, why = await _try_add_role(interaction.user, role, f"Achat boutique: {name}")
            if ok:
                await interaction.response.send_message(
                    f"✅ Tu as acheté **{name}** pour **{cost}** pts. Rôle **{role.name}** ajouté.",
                    ephemeral=True
                )
                await increment_purchase(interaction.user.id, key)
                await _send_shop_log(
                    interaction.guild, interaction.user, name, cost, remaining,
                    role_name=role.name, note="Rôle ajouté"
                )
            else:
                await interaction.response.send_message(
                    f"✅ Achat **{name}** (−{cost} pts).\n⚠️ Impossible d’ajouter **{role.name}** : {why}\nPing un admin.",
                    ephemeral=True
                )
                await increment_purchase(interaction.user.id, key)
                await _send_shop_log(
                    interaction.guild, interaction.user, name, cost, remaining,
                    role_name=role.name, note=f"Rôle non ajouté : {why}"
                )
        else:
            await interaction.response.send_message(
                f"✅ Tu as acheté **{name}** pour **{cost}** pts.\n⚠️ Le rôle avec l’ID `{role_id}` est introuvable, ping un admin.",
                ephemeral=True
            )
            await increment_purchase(interaction.user.id, key)
            await _send_shop_log(
                interaction.guild, interaction.user, name, cost, remaining,
                role_name=f"#{role_id}", note="Rôle introuvable"
            )
    else:
        desc = item.get("description", "Contacte un admin pour la remise.")
        await interaction.response.send_message(
            f"✅ Tu as acheté **{name}** pour **{cost}** pts.\nℹ️ {desc}",
            ephemeral=True
        )
        await increment_purchase(interaction.user.id, key)
        await _send_shop_log(
            interaction.guild, interaction.user, name, cost, remaining,
            role_name=None, note="Remise manuelle"
        )



# ---------- /shopadmin : menu interactif (remplace l'ancien groupe) ----------
@tree.command(name="shopadmin", description="Ouvre le panneau admin de la boutique (admin).")
@guilds_decorator()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
async def shopadmin_menu(interaction: discord.Interaction):

    class AddItemModal(discord.ui.Modal, title="Ajouter un item"):
        key = discord.ui.TextInput(label="Clé (unique, ex: vip, robux100)", min_length=1, max_length=40)
        name = discord.ui.TextInput(label="Nom affiché", min_length=1, max_length=80)
        cost = discord.ui.TextInput(label="Coût (points, entier ≥1)", placeholder="1000", min_length=1, max_length=10)
        role_id = discord.ui.TextInput(label="ID du rôle à donner (optionnel)", required=False, max_length=20)
        max_per_user = discord.ui.TextInput(label="Limite par utilisateur (-1 = illimité)", placeholder="-1", min_length=1, max_length=10)

        async def on_submit(self, modal_interaction: discord.Interaction):
            try:
                c = int(str(self.cost))
                lim = int(str(self.max_per_user))
                if c < 1:
                    raise ValueError
            except Exception:
                return await modal_interaction.response.send_message("❌ Coût ou limite invalide.", ephemeral=True)

            rid_txt = str(self.role_id).strip()
            rid_val: int = 0
            if rid_txt:
                try:
                    rid_val = int(rid_txt)
                except Exception:
                    return await modal_interaction.response.send_message("❌ ID de rôle invalide.", ephemeral=True)

            async with _shop_lock:
                shop = _load_shop()
                k = str(self.key).strip()
                if k in shop:
                    return await modal_interaction.response.send_message("❌ Cette clé existe déjà.", ephemeral=True)
                shop[k] = {
                    "name": str(self.name).strip(),
                    "cost": c,
                    "role_id": rid_val,
                    "description": "",
                    "max_per_user": lim
                }
                _save_shop(shop)

            await modal_interaction.response.send_message(
                f"✅ Item **{self.name}** ajouté (clé `{self.key}` — {c} pts, limite {lim}).",
                ephemeral=True
            )
            await _send_admin_log(
                modal_interaction.guild, modal_interaction.user, "shopadmin.add_item",
                key=str(self.key).strip(), name=str(self.name).strip(),
                cost=c, role_id=rid_val or None, max_per_user=lim
            )




    class EditItemView(View):
        def __init__(self, key: str):
            super().__init__(timeout=120)
            self.key = key

        @discord.ui.button(label="Changer le coût", style=discord.ButtonStyle.primary)
        async def set_cost(self, btn_inter: discord.Interaction, _):
            key_ctx = self.key

            class CostModal(discord.ui.Modal, title=f"Coût pour {key_ctx}"):
                cost = discord.ui.TextInput(label="Nouveau coût (points, entier ≥1)")

                async def on_submit(self, mi: discord.Interaction):
                    try:
                        c = int(str(self.cost))
                        if c < 1:
                            raise ValueError
                    except Exception:
                        return await mi.response.send_message("❌ Valeur invalide.", ephemeral=True)
                    async with _shop_lock:
                        shop = _load_shop()
                        if key_ctx not in shop:
                            return await mi.response.send_message("❌ Clé introuvable.", ephemeral=True)
                        shop[key_ctx]["cost"] = c
                        _save_shop(shop)
                    await mi.response.send_message(f"✅ Coût mis à jour: `{key_ctx}` → {c} pts.", ephemeral=True)
                    await _send_admin_log(
                        mi.guild, mi.user, "shopadmin.edit.set_cost",
                        key=key_ctx, cost=c
                    )


            await btn_inter.response.send_modal(CostModal())

        @discord.ui.button(label="Définir l’ID du rôle", style=discord.ButtonStyle.secondary)
        async def set_role_id(self, btn_inter: discord.Interaction, _):
            key_ctx = self.key

            class RoleIdModal(discord.ui.Modal, title=f"ID du rôle pour {key_ctx}"):
                role_id = discord.ui.TextInput(label="ID du rôle (laisser vide pour retirer)", required=False)

                async def on_submit(self, mi: discord.Interaction):
                    rid_txt = str(self.role_id).strip()
                    rid_val: int = 0
                    if rid_txt:
                        try:
                            rid_val = int(rid_txt)
                        except Exception:
                            return await mi.response.send_message("❌ ID invalide.", ephemeral=True)
                    async with _shop_lock:
                        shop = _load_shop()
                        if key_ctx not in shop:
                            return await mi.response.send_message("❌ Clé introuvable.", ephemeral=True)
                        shop[key_ctx]["role_id"] = rid_val
                        _save_shop(shop)
                    txt = f"role_id = `{rid_val}`" if rid_val else "aucun rôle"
                    await mi.response.send_message(f"✅ `{key_ctx}` → {txt}.", ephemeral=True)
                    await _send_admin_log(mi.guild, mi.user, "shopadmin.edit.set_role_id", key=key_ctx, role_id=(rid_val or None))

            await btn_inter.response.send_modal(RoleIdModal())


        @discord.ui.button(label="Définir la limite", style=discord.ButtonStyle.secondary)
        async def set_limit(self, btn_inter: discord.Interaction, _):
            key_ctx = self.key

            class LimitModal(discord.ui.Modal, title=f"Limite pour {key_ctx}"):
                limit = discord.ui.TextInput(label="Limite par utilisateur (-1 = illimité)")

                async def on_submit(self, mi: discord.Interaction):
                    try:
                        lim = int(str(self.limit))
                    except Exception:
                        return await mi.response.send_message("❌ Valeur invalide.", ephemeral=True)
                    async with _shop_lock:
                        shop = _load_shop()
                        if key_ctx not in shop:
                            return await mi.response.send_message("❌ Clé introuvable.", ephemeral=True)
                        shop[key_ctx]["max_per_user"] = lim
                        _save_shop(shop)
                    limtxt = "illimité" if lim < 0 else str(lim)
                    await mi.response.send_message(f"✅ Limite mise à jour: `{key_ctx}` → {limtxt}.", ephemeral=True)
                    await _send_admin_log(
                        mi.guild, mi.user, "shopadmin.edit.set_limit",
                        key=key_ctx, limit=lim
                    )


            await btn_inter.response.send_modal(LimitModal())

        @discord.ui.button(label="Modifier la description", style=discord.ButtonStyle.secondary)
        async def set_desc(self, btn_inter: discord.Interaction, _):
            key_ctx = self.key

            class DescModal(discord.ui.Modal, title=f"Description pour {key_ctx}"):
                desc = discord.ui.TextInput(label="Description (peut être vide)", style=discord.TextStyle.paragraph, required=False)

                async def on_submit(self, mi: discord.Interaction):
                    async with _shop_lock:
                        shop = _load_shop()
                        if key_ctx not in shop:
                            return await mi.response.send_message("❌ Clé introuvable.", ephemeral=True)
                        shop[key_ctx]["description"] = str(self.desc)
                        _save_shop(shop)
                    await mi.response.send_message(f"✅ Description mise à jour pour `{key_ctx}`.", ephemeral=True)
                    await _send_admin_log(
                        mi.guild, mi.user, "shopadmin.edit.set_desc",
                        key=key_ctx
                    )


            await btn_inter.response.send_modal(DescModal())

        @discord.ui.button(label="Retour", style=discord.ButtonStyle.danger)
        async def back(self, btn_inter: discord.Interaction, _):
            await open_root(btn_inter)


    class RootView(View):
        def __init__(self):
            super().__init__(timeout=120)

        @discord.ui.button(label="➕ Ajouter un item", style=discord.ButtonStyle.success)
        async def add_item(self, btn_inter: discord.Interaction, button):
            await btn_inter.response.send_modal(AddItemModal())

        @discord.ui.button(label="✏️ Éditer un item", style=discord.ButtonStyle.primary)
        async def edit_item(self, btn_inter: discord.Interaction, button):
            async with _shop_lock:
                shop = _load_shop()
            if not shop:
                return await btn_inter.response.send_message("La boutique est vide.", ephemeral=True)

            options = [
                discord.SelectOption(label=it["name"], description=f"clé: {k} — {it['cost']} pts", value=k)
                for k, it in list(shop.items())[:25]
            ]

            class PickEdit(View):
                def __init__(self):
                    super().__init__(timeout=90)
                @discord.ui.select(placeholder="Choisis un item à éditer…", min_values=1, max_values=1, options=options)
                async def choose(self, si: discord.Interaction, select: Select):
                    key = select.values[0]
                    await si.response.edit_message(content=f"**Édition de `{key}`**", view=EditItemView(key))

                @discord.ui.button(label="Retour", style=discord.ButtonStyle.danger)
                async def back(self, si, _):
                    await open_root(si)

            await btn_inter.response.edit_message(content="Sélectionne un item à éditer :", view=PickEdit())

        @discord.ui.button(label="🗑️ Supprimer un item", style=discord.ButtonStyle.secondary)
        async def remove_item(self, btn_inter: discord.Interaction, button):
            async with _shop_lock:
                shop = _load_shop()
            if not shop:
                return await btn_inter.response.send_message("La boutique est vide.", ephemeral=True)

            options = [
                discord.SelectOption(label=it["name"], description=f"clé: {k}", value=k)
                for k, it in list(shop.items())[:25]
            ]

            class ConfirmRemove(View):
                def __init__(self, key: str):
                    super().__init__(timeout=60)
                    self.key = key

                @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.danger)
                async def yes(self, ci: discord.Interaction, _):
                    async with _shop_lock:
                        shop = _load_shop()
                        if self.key not in shop:
                            return await ci.response.send_message("❌ Clé introuvable.", ephemeral=True)
                        removed = shop.pop(self.key)
                        _save_shop(shop)
                    await ci.response.edit_message(content=f"✅ Supprimé **{removed['name']}** (clé `{self.key}`).", view=None)
                    await _send_admin_log(
                        ci.guild, ci.user, "shopadmin.remove_item",
                        key=self.key, name=removed.get("name")
                    )


                @discord.ui.button(label="Annuler", style=discord.ButtonStyle.secondary)
                async def no(self, ci: discord.Interaction, _):
                    await open_root(ci)

            class PickRemove(View):
                def __init__(self):
                    super().__init__(timeout=90)
                @discord.ui.select(placeholder="Choisis un item à supprimer…", min_values=1, max_values=1, options=options)
                async def choose(self, si: discord.Interaction, select: Select):
                    key = select.values[0]
                    await si.response.edit_message(content=f"Supprimer `{key}` ?", view=ConfirmRemove(key))

                @discord.ui.button(label="Retour", style=discord.ButtonStyle.danger)
                async def back(self, si, _):
                    await open_root(si)

            await btn_inter.response.edit_message(content="Sélectionne un item à supprimer :", view=PickRemove())

        @discord.ui.button(label="📜 Lister les items", style=discord.ButtonStyle.secondary)
        async def list_items(self, btn_inter: discord.Interaction, button):
            async with _shop_lock:
                shop = _load_shop()
            if not shop:
                return await btn_inter.response.send_message("La boutique est vide.", ephemeral=True)
            lines = []
            for key, it in shop.items():
                rid = int(it.get("role_id", 0))
                role_obj = btn_inter.guild.get_role(rid) if rid else None  # type: ignore
                role_part = f" | rôle: {role_obj.name} (<@&{rid}>)" if role_obj else (f" | rôle: <@&{rid}>" if rid else "")
                limit = int(it.get("max_per_user", -1))
                limit_part = "∞" if limit < 0 else str(limit)
                desc_part = f"\n    {it['description']}" if it.get("description") else ""
                lines.append(f"- `{key}` → **{it['name']}** ({it['cost']} pts) | limite/utilisateur: {limit_part}{role_part}{desc_part}")
            await btn_inter.response.send_message("**Boutique actuelle :**\n" + "\n".join(lines), ephemeral=True)
            await _send_admin_log(btn_inter.guild, btn_inter.user, "shopadmin.list_items")



        @discord.ui.button(label="📊 Stats achats", style=discord.ButtonStyle.secondary)
        async def stats(self, btn_inter: discord.Interaction, button):
            class StatsMenu(View):
                def __init__(self):
                    super().__init__(timeout=60)

                @discord.ui.button(label="Global", style=discord.ButtonStyle.primary)
                async def global_stats(self, si, _):
                    async with _purchases_lock:
                        p = _load_purchases()
                    if not p:
                        return await si.response.send_message("ℹ️ Aucun achat enregistré.", ephemeral=True)
                    lines = ["**Achats totaux (par membre) :**"]
                    for uid, items in p.items():
                        total = sum(items.values())
                        lines.append(f"- <@{uid}> → **{total}** (détail: {', '.join(f'{k}:{v}' for k, v in items.items())})")
                    await si.response.send_message("\n".join(lines), ephemeral=True)
                    await _send_admin_log(
                        si.guild, si.user, "shopadmin.stats.global"
                    )


                @discord.ui.button(label="Par item", style=discord.ButtonStyle.secondary)
                async def by_item(self, si, _):
                    async with _shop_lock:
                        shop = _load_shop()
                    if not shop:
                        return await si.response.send_message("La boutique est vide.", ephemeral=True)
                    options = [discord.SelectOption(label=it["name"], value=k) for k, it in list(shop.items())[:25]]

                    class PickItem(View):
                        def __init__(self):
                            super().__init__(timeout=60)
                        @discord.ui.select(placeholder="Choisis un item…", min_values=1, max_values=1, options=options)
                        async def choose(self, pi_i: discord.Interaction, select: Select):
                            key = select.values[0]
                            async with _purchases_lock:
                                p = _load_purchases()
                            found = False
                            lines = []
                            for uid, items in p.items():
                                if key in items:
                                    found = True
                                    lines.append(f"- <@{uid}> → **{items[key]}**")
                            if not found:
                                return await pi_i.response.send_message("ℹ️ Aucun achat pour cette clé.", ephemeral=True)
                            lines.insert(0, f"**Achats pour `{key}` :**")
                            await pi_i.response.send_message("\n".join(lines), ephemeral=True)
                            await _send_admin_log(
                                pi_i.guild, pi_i.user, "shopadmin.stats.by_item",
                                key=key
                            )


                        @discord.ui.button(label="Retour", style=discord.ButtonStyle.danger)
                        async def back(self, pi_i, _):
                            await open_root(pi_i)

                    await si.response.edit_message(content="Sélectionne un item :", view=PickItem())

                @discord.ui.button(label="Par membre (ID ou @mention)", style=discord.ButtonStyle.secondary)
                async def by_member(self, si, _):
                    class MemberModal(discord.ui.Modal, title="Stats par membre"):
                        ident = discord.ui.TextInput(label="ID ou @mention", placeholder="@Pseudo ou 1234567890")

                        async def on_submit(self, mi: discord.Interaction):
                            text = str(self.ident).strip()
                            uid = None
                            # extraire l'ID d'une mention <@123> / <@!123>
                            if text.startswith("<@") and text.endswith(">"):
                                digits = "".join(ch for ch in text if ch.isdigit())
                                if digits:
                                    uid = int(digits)
                            if uid is None:
                                try:
                                    uid = int(text)
                                except Exception:
                                    pass
                            member = None
                            if uid:
                                member = mi.guild.get_member(uid)  # type: ignore
                                if not member:
                                    try:
                                        member = await mi.guild.fetch_member(uid)  # type: ignore
                                    except Exception:
                                        member = None
                            if not member:
                                return await mi.response.send_message("❌ Membre introuvable.", ephemeral=True)

                            async with _purchases_lock:
                                p = _load_purchases()
                            items = p.get(str(member.id), {})
                            if not items:
                                return await mi.response.send_message("ℹ️ Aucun achat pour ce membre.", ephemeral=True)
                            lines = [f"**Achats de {member.display_name} :**"]
                            for k, c in items.items():
                                lines.append(f"- `{k}` = **{c}**")
                            await mi.response.send_message("\n".join(lines), ephemeral=True)
                            await _send_admin_log(
                                mi.guild, mi.user, "shopadmin.stats.by_member",
                                member=f"{member} ({member.id})"
                            )

                    await si.response.send_modal(MemberModal())

                @discord.ui.button(label="Retour", style=discord.ButtonStyle.danger)
                async def back(self, si, _):
                    await open_root(si)

            await btn_inter.response.edit_message(content="Stats achats :", view=StatsMenu())

        @discord.ui.button(label="❌ Fermer", style=discord.ButtonStyle.danger)
        async def close(self, btn_inter: discord.Interaction, button):
            await btn_inter.response.edit_message(content="Panneau fermé.", view=None)

    await _send_admin_log(
        interaction.guild,
        interaction.user,
        "shopadmin.open"
    )
    async def open_root(resp_inter: discord.Interaction):
        await resp_inter.response.edit_message(content="**Panneau admin de la boutique**", view=RootView())

    # Première ouverture
    await interaction.response.send_message("**Panneau admin de la boutique**", view=RootView(), ephemeral=True)

# ---------- Erreurs commandes ----------
@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    # 1) Manque de permissions (prévues) → message propre + log soft, pas de traceback
    if isinstance(error, app_commands.MissingPermissions):
        msg = "⛔ Tu n'as pas la permission d'utiliser cette commande."
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except Exception:
            pass
        # Log léger, sans stacktrace
        try:
            cmd = interaction.command.qualified_name if interaction.command else "unknown"
            chan = f"#{getattr(interaction.channel, 'name', '?')}"
            logging.warning("MissingPermissions: %s a tenté /%s dans %s", interaction.user, cmd, chan)
        except Exception:
            pass
        return

    # 2) Autres erreurs "connues" utiles à rendre jolies (facultatif)
    if isinstance(error, app_commands.CommandOnCooldown):
        try:
            joli = _format_cooldown(error.retry_after)
            # Variante bonus : aussi montrer l'heure d'expiration Discord (relative)
            # from datetime import timedelta  # <-- ajoute cette import en haut
            # expire = int((datetime.now(timezone.utc) + timedelta(seconds=int(error.retry_after))).timestamp())
            # txt = f"⏳ Cette commande est en cooldown. Réessaie dans {joli} ( <t:{expire}:R> )."

            txt = f"⏳ Cette commande est en cooldown. Réessaie dans {joli}."
            if not interaction.response.is_done():
                await interaction.response.send_message(txt, ephemeral=True)
            else:
                await interaction.followup.send(txt, ephemeral=True)
        except Exception:
            pass
        return

    # 3) Erreurs inattendues → traceback + message générique
    logging.exception("Slash command error: %r", error)
    try:
        txt = "❌ Erreur lors de l'exécution de la commande."
        if not interaction.response.is_done():
            await interaction.response.send_message(txt, ephemeral=True)
        else:
            await interaction.followup.send(txt, ephemeral=True)
    except Exception:
        pass

# ---------- Sync + Ready ----------
@bot.event
async def on_reaction_add(reaction, user):
    if user.bot:
        return
    message = reaction.message
    if not message.guild:
        return

    # Vérifie que l’auteur du message existe
    author = message.author
    if author.bot:
        return

    # Charger les quêtes
    qcfg = _load_quests()
    async with _quests_progress_lock:
        pdb = _load_quests_progress()
        date_key = _today_str()
        week_key = _week_str()

        # --- Cas 1 : réaction d’un modérateur
        if any(r.permissions.administrator or r.permissions.manage_messages for r in user.roles):
            for qkey, q in qcfg.get("daily", {}).items():
                if q.get("type") == "reaction_mod":
                    slot = _ensure_user_quest_slot(pdb, "daily", date_key, message.guild.id, author.id, qkey)
                    slot["progress"] = min(q["target"], slot.get("progress", 0) + 1)

        # --- Cas 2 : total de réactions sur un message
        total_reacts = sum(r.count for r in message.reactions)
        for qkey, q in qcfg.get("daily", {}).items():
            if q.get("type") == "reaction_total":
                if total_reacts >= q["target"]:
                    slot = _ensure_user_quest_slot(pdb, "daily", date_key, message.guild.id, author.id, qkey)
                    slot["progress"] = q["target"]

        _save_quests_progress(pdb)

@bot.event
async def on_invite_create(invite: discord.Invite):
    # S’assure que les nouveaux codes sont connus AVANT un join
    g = invite.guild
    cache = _invite_cache.setdefault(g.id, {})
    inviter_id = invite.inviter.id if invite.inviter else 0
    cache[invite.code] = (invite.uses or 0, inviter_id)

@bot.event
async def on_invite_delete(invite: discord.Invite):
    # Retire le code supprimé/épuisé du cache
    g = invite.guild
    cache = _invite_cache.setdefault(g.id, {})
    cache.pop(invite.code, None)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    # Ignore les bots
    if member.bot:
        return
    guild = member.guild
    key = (guild.id, member.id)
    now = int(datetime.now(timezone.utc).timestamp())

    was_in = before.channel is not None
    now_in = after.channel is not None

    try:
        # Début de session
        if not was_in and now_in:
            _voice_sessions[key] = now

        # Fin de session
        elif was_in and not now_in:
            start = _voice_sessions.pop(key, None)
            if start:
                delta_secs = max(0, now - start)
                delta_min = delta_secs // 60
                if delta_min > 0:
                    date_key = _today_str()
                    week_key = _week_str()
                    async with _quests_progress_lock:
                        pdb  = _load_quests_progress()
                        qcfg = _load_quests()
                    
                        # <-- récupère les quêtes assignées (sets de clés)
                        assigned_daily  = _ensure_assignments(pdb, "daily",  date_key, guild.id, member.id)
                        assigned_weekly = _ensure_assignments(pdb, "weekly", week_key, guild.id, member.id)
                    
                        # DAILY
                        for qkey, q in qcfg.get("daily", {}).items():
                            if q.get("type") == "voice_minutes" and qkey in assigned_daily:
                                slot = _ensure_user_quest_slot(pdb, "daily", date_key, guild.id, member.id, qkey)
                                slot["progress"] = int(slot.get("progress", 0)) + int(delta_min)
                    
                        # WEEKLY
                        for qkey, q in qcfg.get("weekly", {}).items():
                            if q.get("type") == "voice_minutes" and qkey in assigned_weekly:
                                slot = _ensure_user_quest_slot(pdb, "weekly", week_key, guild.id, member.id, qkey)
                                slot["progress"] = int(slot.get("progress", 0)) + int(delta_min)
                    
                        _save_quests_progress(pdb)

        # Changement de salon vocal (on clôture + rouvre pour être simple)
        elif was_in and now_in and before.channel != after.channel:
            start = _voice_sessions.pop(key, None)
            if start:
                delta_secs = max(0, now - start)
                delta_min = delta_secs // 60
                if delta_min > 0:
                    date_key = _today_str()
                    week_key = _week_str()
                    async with _quests_progress_lock:
                        pdb  = _load_quests_progress()
                        qcfg = _load_quests()
                    
                        # <-- récupère les quêtes assignées (sets de clés)
                        assigned_daily  = _ensure_assignments(pdb, "daily",  date_key, guild.id, member.id)
                        assigned_weekly = _ensure_assignments(pdb, "weekly", week_key, guild.id, member.id)
                    
                        # DAILY
                        for qkey, q in qcfg.get("daily", {}).items():
                            if q.get("type") == "voice_minutes" and qkey in assigned_daily:
                                slot = _ensure_user_quest_slot(pdb, "daily", date_key, guild.id, member.id, qkey)
                                slot["progress"] = int(slot.get("progress", 0)) + int(delta_min)
                    
                        # WEEKLY
                        for qkey, q in qcfg.get("weekly", {}).items():
                            if q.get("type") == "voice_minutes" and qkey in assigned_weekly:
                                slot = _ensure_user_quest_slot(pdb, "weekly", week_key, guild.id, member.id, qkey)
                                slot["progress"] = int(slot.get("progress", 0)) + int(delta_min)
                    
                        _save_quests_progress(pdb)
            # nouvelle session dans le nouveau salon
            _voice_sessions[key] = now

    except Exception:
        logging.exception("Erreur on_voice_state_update")

@bot.event
async def setup_hook():
    if GUILD_ID:
        cmds = await tree.sync(guild=discord.Object(id=GUILD_ID))
        logging.info("Synced %d cmd(s) pour la guilde %s", len(cmds), GUILD_ID)
    else:
        cmds = await tree.sync()
        logging.info("Synced %d cmd(s) globales", len(cmds))

    asyncio.create_task(quests_midnight_rollover())
    asyncio.create_task(streak_monitor())

@bot.event
async def on_ready():
    logging.info("Connecté en tant que %s (%s)", bot.user, bot.user.id)  # type: ignore
    # Précharger le cache d’invites pour toutes les guildes
    for g in bot.guilds:
        await _refresh_invite_cache(g)
    logging.info("Prêt.")

@bot.event
async def on_guild_join(guild: discord.Guild):
    await _refresh_invite_cache(guild)

@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    # snapshot avant
    before = _invite_cache.get(guild.id, {}).copy()
    # re-fetch après le join — avec retries pour laisser le temps à l’API de propager les uses
    code = None
    inviter_id = None
    for delay in (0.5, 1.5, 3.0):  # 3 tentatives espacées
        await asyncio.sleep(delay)
        await _refresh_invite_cache(guild)
        after = _invite_cache.get(guild.id, {})
        code, inviter_id = _find_used_invite(before, after)
        if code:
            break
    
    # Si toujours rien trouvé, on teste le vanity pour affiner le message
    vanity_used = False
    if not inviter_id:
        try:
            vanity = await guild.vanity_invite()  # None si pas de vanity
            vanity_used = vanity is not None
        except discord.Forbidden:
            pass

    if inviter_id:
        total = await _add_invite_for(inviter_id, member.id)
        # Quêtes 'invites' (daily + weekly) pour l'invitant
        try:
            date_key = _today_str()
            week_key = _week_str()
            # … après avoir trouvé inviter_id …
            async with _quests_progress_lock:
                pdb  = _load_quests_progress()
                qcfg = _load_quests()
            
                date_key = _today_str()
                week_key = _week_str()
            
                assigned_daily  = _ensure_assignments(pdb, qcfg, "daily",  date_key, member.guild.id, inviter_id, k=3)
                assigned_weekly = _ensure_assignments(pdb, qcfg, "weekly", week_key,  member.guild.id, inviter_id, k=3)
            
                for qkey, q in qcfg.get("daily", {}).items():
                    if qkey in assigned_daily and q.get("type") == "invites":
                        slot = _ensure_user_quest_slot(pdb, "daily", date_key, member.guild.id, inviter_id, qkey)
                        slot["progress"] = int(slot.get("progress", 0)) + 1
            
                for qkey, q in qcfg.get("weekly", {}).items():
                    if qkey in assigned_weekly and q.get("type") == "invites":
                        slot = _ensure_user_quest_slot(pdb, "weekly", week_key, member.guild.id, inviter_id, qkey)
                        slot["progress"] = int(slot.get("progress", 0)) + 1
            
                _save_quests_progress(pdb)

        except Exception:
            logging.exception("Erreur incrément quêtes invites")

        await _send_invite_log(
            guild,
            f"🌿 {member.mention} s’avance dans le camp, guidé par <@{inviter_id}>. "
            f"Son mentor compte désormais **{total}** guerrier(s) qu’il a amené dans le Clan."
        )
        # Récompense points (une seule fois par invité unique)
        try:
            async with _invite_rewards_lock:
                rdb = _load_invite_rewards()
                rewarded = rdb.setdefault("rewarded", {})
                mid = str(member.id)

                if mid not in rewarded:
                    # Première fois que ce membre rejoint et crédite un parrain → on récompense
                    inviter = guild.get_member(inviter_id)
                    mul = points_multiplier_for(inviter) if inviter else 1.0
                    new_total_pts = await add_points(inviter_id, int(round(INVITE_REWARD_POINTS * mul)))
                    rewarded[mid] = int(inviter_id)
                    _save_invite_rewards(rdb)

                    # petit log / feedback côté staff (même salon que les joins si tu veux)
                    await _send_invite_log(
                        guild,
                        f"🎁 +{int(round(INVITE_REWARD_POINTS * mul))} pts pour <@{inviter_id}> (nouveau total: **{new_total_pts}**) — premier join crédité de {member.mention}."
                    )
                else:
                    # déjà récompensé par le passé → pas de points
                    pass
        except Exception:
            # on avale l’erreur pour ne pas bloquer l’event
            logging.exception("Invite reward error")
    else:
        # Cas indéterminé : on précise la raison si possible
        reason = "propagation lente/indétectable"
        if vanity_used:
            reason = "lien vanity probable"
    
        # 🔍 Dernière tentative : on regarde les logs d’audit pour une invite à 1 usage
        inviter_id = None
        try:
            async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.invite_create):
                inv: discord.Invite = entry.target  # type: ignore
                if getattr(inv, "max_uses", 0) == 1:
                    # créé il y a très peu de temps (2 min)
                    if (datetime.now(timezone.utc) - entry.created_at).total_seconds() < 120:
                        inviter_id = entry.user.id
                        break
        except discord.Forbidden:
            pass
    
        if inviter_id:
            inviter = guild.get_member(inviter_id)
            await _send_invite_log(
                guild,
                f"👋 {member.mention} a rejoint, invité par {inviter.mention if inviter else 'inconnu'} (détection audit log, 1 usage)."
            )
            # ici tu peux aussi ajouter ton incrément de points / quêtes si tu veux
        else:
            await _send_invite_log(
                guild,
                f"👋 {member.mention} a rejoint, **invitation non déterminée** ({reason})."
            )

@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    inviter_id, new_total = await _remove_invite_for_member(member.id)
    actor = bot.user or member  # pour le log

    if inviter_id is not None:
        try:
            inviter = guild.get_member(inviter_id) or await bot.fetch_user(inviter_id)
            inviter_label = f"{inviter} (<@{inviter_id}>)"
        except Exception:
            inviter_label = f"<@{inviter_id}> (inconnu)"

        await _send_admin_log(
            guild,
            actor,
            "member.leave",
            membre=f"{member} ({member.id})",
            inviteur=inviter_label,
            invites_total=new_total
        )
    else:
        await _send_admin_log(
            guild,
            actor,
            "member.leave",
            membre=f"{member} ({member.id})",
            inviteur="Non déterminé"
        )

@bot.event
async def on_message(message: discord.Message):
    # On ignore les messages du bot lui-même
    if message.author.bot:
        return

    # Si le message vient d’un DM (pas d’un serveur)
    if isinstance(message.channel, discord.DMChannel):
        user = message.author
        if ADMIN_LOG_CHANNEL_ID:
            channel = bot.get_channel(ADMIN_LOG_CHANNEL_ID)
            if channel is None:
                try:
                    channel = await bot.fetch_channel(ADMIN_LOG_CHANNEL_ID)
                except Exception:
                    channel = None
            if channel:
                embed = discord.Embed(
                    title="💬 Nouveau message privé reçu",
                    color=discord.Color.blurple(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(name="Auteur", value=f"{user.mention} (`{user.id}`)", inline=False)
                embed.add_field(name="Contenu", value=message.content or "*[vide]*", inline=False)
                if message.attachments:
                    urls = "\n".join(a.url for a in message.attachments)
                    embed.add_field(name="Pièces jointes", value=urls, inline=False)
                try:
                    await channel.send(embed=embed)
                except Exception:
                    pass
        # on ne compte pas les DMs pour les quêtes
        return

    # --- Quêtes: compter les messages en serveur ---
    if message.guild:
        date_key = _today_str()
        week_key = _week_str()
        # dans on_message (partie "Quêtes: compter les messages en serveur")
        async with _quests_progress_lock:
            pdb  = _load_quests_progress()
            qcfg = _load_quests()
        
            # Assigner si besoin
            assigned_daily  = _ensure_assignments(pdb, qcfg, "daily",  date_key, message.guild.id, message.author.id, k=3)
            assigned_weekly = _ensure_assignments(pdb, qcfg, "weekly", week_key,  message.guild.id, message.author.id, k=3)
        
            # DAILY
            for qkey, q in qcfg.get("daily", {}).items():
                if qkey not in assigned_daily:
                    continue
                qtype = q.get("type")
            
                if qtype == "messages":
                    slot = _ensure_user_quest_slot(pdb, "daily", date_key, message.guild.id, message.author.id, qkey)
                    slot["progress"] = int(slot.get("progress", 0)) + 1
            
                elif qtype == "message_exact":
                    wanted = str(q.get("text", "")).strip()
                    if wanted and message.content.strip() == wanted:
                        ch_ok = True
                        cid = q.get("channel_id")
                        if cid:
                            ch_ok = (int(cid) == message.channel.id)
                        if ch_ok:
                            slot = _ensure_user_quest_slot(pdb, "daily", date_key, message.guild.id, message.author.id, qkey)
                            slot["progress"] = min(1, int(slot.get("progress", 0)) + 1)
            
                elif qtype == "messages_time_window":
                    # Fenêtre horaire locale, ex: 22 -> 2 en Europe/Paris
                    tz_name   = str(q.get("tz", "UTC"))
                    start_h   = int(q.get("start_hour", 0))
                    end_h     = int(q.get("end_hour", 0))
                    target    = int(q.get("target", 1))
            
                    # created_at est en UTC (aware) -> converti dans le fuseau demandé
                    local_dt  = message.created_at.astimezone(ZoneInfo(tz_name))
                    hour      = local_dt.hour
            
                    if start_h == end_h:
                        in_window = True  # toute la journée (cas limite)
                    elif start_h < end_h:
                        # fenêtre simple, ex 10 -> 18
                        in_window = (start_h <= hour < end_h)
                    else:
                        # fenêtre chevauchant minuit, ex 22 -> 2
                        in_window = (hour >= start_h or hour < end_h)
            
                    if in_window:
                        slot = _ensure_user_quest_slot(pdb, "daily", date_key, message.guild.id, message.author.id, qkey)
                        slot["progress"] = min(target, int(slot.get("progress", 0)) + 1)

            # WEEKLY
            for qkey, q in qcfg.get("weekly", {}).items():
                if qkey not in assigned_weekly:
                    continue
                qtype = q.get("type")
                if qtype == "messages":
                    slot = _ensure_user_quest_slot(pdb, "weekly", week_key, message.guild.id, message.author.id, qkey)
                    slot["progress"] = int(slot.get("progress", 0)) + 1
                elif qtype == "message_exact":
                    wanted = str(q.get("text", "")).strip()
                    if wanted and message.content.strip() == wanted:
                        ch_ok = True
                        cid = q.get("channel_id")
                        if cid:
                            ch_ok = (int(cid) == message.channel.id)
                        if ch_ok:
                            slot = _ensure_user_quest_slot(pdb, "weekly", week_key, message.guild.id, message.author.id, qkey)
                            slot["progress"] = min(1, int(slot.get("progress", 0)) + 1)
        
            _save_quests_progress(pdb)


    # Propager aux autres commandes
    await bot.process_commands(message)

async def quests_midnight_rollover():
    """À chaque minute, si on passe un jour UTC, on coupe les sessions vocales et on range la progression au bon jour."""
    await bot.wait_until_ready()
    last_day = _today_str()
    while not bot.is_closed():
        try:
            now_day = _today_str()
            if now_day != last_day:
                # On ferme proprement toutes les sessions vocales ouvertes (créditées sur "hier").
                now_ts = int(datetime.now(timezone.utc).timestamp())
                closings = list(_voice_sessions.items())
                _voice_sessions.clear()
                if closings:
                    async with _quests_progress_lock:
                        pdb  = _load_quests_progress()
                        qcfg = _load_quests()
                
                        # last_day est déjà défini au-dessus
                        y, m, d = map(int, last_day.split("-"))
                        from datetime import date as _date
                        iso_year, iso_week, _ = _date(y, m, d).isocalendar()
                        last_week = f"{iso_year}-W{iso_week:02d}"
                
                        for (guild_id, user_id), start in closings:
                            delta_min = max(0, (now_ts - start) // 60)
                            if delta_min <= 0:
                                continue
                            # DAILY -> veille (last_day)
                            assigned_daily  = _ensure_assignments(pdb, "daily",  last_day,  guild_id, user_id)
                            for qkey, q in qcfg.get("daily", {}).items():
                                if q.get("type") == "voice_minutes" and qkey in assigned_daily:
                                    slot = _ensure_user_quest_slot(pdb, "daily", last_day, guild_id, user_id, qkey)
                                    slot["progress"] = int(slot.get("progress", 0)) + int(delta_min)
                            
                            # WEEKLY -> semaine de la veille (last_week)
                            assigned_weekly = _ensure_assignments(pdb, "weekly", last_week, guild_id, user_id)
                            for qkey, q in qcfg.get("weekly", {}).items():
                                if q.get("type") == "voice_minutes" and qkey in assigned_weekly:
                                    slot = _ensure_user_quest_slot(pdb, "weekly", last_week, guild_id, user_id, qkey)
                                    slot["progress"] = int(slot.get("progress", 0)) + int(delta_min)
                        _save_quests_progress(pdb)
                last_day = now_day
        except Exception:
            logging.exception("Erreur quests_midnight_rollover")
        await asyncio.sleep(60)

async def streak_monitor():
    """Vérifie régulièrement les streaks daily et prévient les utilisateurs."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            async with _daily_lock:
                daily = _load_daily()

            now_ts = int(datetime.now(timezone.utc).timestamp())
            updated = False

            for uid, state in list(daily.items()):
                last = int(state.get("last", 0))
                streak = int(state.get("streak", 0))
                warned = bool(state.get("warned", False))
                if not last or streak == 0:
                    continue

                elapsed = now_ts - last
                user = bot.get_user(int(uid))
                if not user:
                    try:
                        user = await bot.fetch_user(int(uid))
                    except Exception:
                        user = None
                if not user:
                    continue

                # ⚠️ Avertissement (une seule fois)
                if (STREAK_GRACE - STREAK_WARNING_BEFORE) <= elapsed < STREAK_GRACE:
                    if not warned:
                        try:
                            await user.send("⚠️ **Votre daily streak expire bientôt !** (~30 min restantes) ⏰")
                        except Exception:
                            pass
                        state["warned"] = True
                        updated = True

                # 💀 Expiration
                elif elapsed >= STREAK_GRACE:
                    daily[uid] = {"last": last, "streak": 0, "warned": False}
                    updated = True
                    try:
                        await user.send("💀 **Votre daily streak a expiré !** Tu repars à 0 😿")
                    except Exception:
                        pass

            if updated:
                async with _daily_lock:
                    _save_daily(daily)

        except Exception as e:
            logging.exception("Erreur dans streak_monitor: %s", e)

        # Tu peux mettre 300 (5 min) si tu veux encore moins de charge.
        await asyncio.sleep(60)

# ---------- Run ----------
if __name__ == "__main__":
    # Crée les fichiers si absents
    for ensure in (_ensure_points_exists, _ensure_shop_exists, _ensure_purchases_exists, _ensure_quests_exists, _ensure_quests_progress_exists):
        try:
            ensure()
        except Exception:
            pass
    bot.run(TOKEN)



