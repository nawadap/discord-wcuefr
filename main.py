# --- BOT POINTS + BOUTIQUE (SANS VOCAUX) ---

import asyncio
import json
import logging
import os
from typing import Dict, Tuple, List

import discord
from discord import Intents, app_commands
from discord.ext import commands
from discord.ui import View, Select
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

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

# --- Salons de logs ---
SHOP_LOG_CHANNEL_ID = int(os.getenv("SHOP_LOG_CHANNEL_ID", "0"))
ADMIN_LOG_CHANNEL_ID = int(os.getenv("ADMIN_LOG_CHANNEL_ID", "0"))
INVITE_LOG_CHANNEL_ID = int(os.getenv("INVITE_LOG_CHANNEL_ID", "0"))

# --- Paramètres ---
INVITE_REWARD_POINTS = int(os.getenv("INVITE_REWARD_POINTS", "30"))

# --- Verrous (internes, pas dans .env) ---
_points_lock = asyncio.Lock()
_shop_lock = asyncio.Lock()
_purchases_lock = asyncio.Lock()
_invites_lock = asyncio.Lock()
_daily_lock = asyncio.Lock()
_invite_rewards_lock = asyncio.Lock()

# ---------- Intents & client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True

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
    with open(POINTS_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(points, f, ensure_ascii=False, indent=2)

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
                    "cost": 1000,
                    "description": "Échange manuel : contacte un admin.",
                    "max_per_user": -1   # illimité
                },
                "robux1000": {
                    "name": "💸 1000 Robux",
                    "cost": 9500,
                    "description": "Échange manuel : contacte un admin.",
                    "max_per_user": -1   # illimité
                },
                "halloween": {
                  "name": "🎃 Titre Halloween",
                  "cost": 100,
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
    with open(SHOP_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(shop, f, ensure_ascii=False, indent=2)

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
    with open(PURCHASES_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2)

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
    with open(INVITES_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
        if code not in after and uses_before > 0:
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
    with open(INVITE_REWARDS_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _load_daily() -> Dict[str, int]:
    """{ user_id(str): last_claim_ts(int, secondes UTC) }"""
    _ensure_daily_exists()
    with open(DAILY_DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # cast en int par sûreté
    return {str(k): int(v) for k, v in data.items()}

def _save_daily(data: Dict[str, int]) -> None:
    with open(DAILY_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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

# ---------- Slash commands ----------

DAILY_REWARD = 10          # à ta guise
DAILY_COOLDOWN = 24*60*60  # 24h en secondes

@tree.command(name="daily", description="Réclame ta récompense quotidienne.")
@guilds_decorator()
async def daily_cmd(interaction: discord.Interaction):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    uid = str(interaction.user.id)

    async with _daily_lock:
        daily = _load_daily()
        last = int(daily.get(uid, 0))
        elapsed = now_ts - last

        if last > 0 and elapsed < DAILY_COOLDOWN:
            remain = DAILY_COOLDOWN - elapsed
            joli = _format_cooldown(remain)
            expire = now_ts + remain
            # Affiche aussi l’heure relative Discord <t:...:R>
            return await interaction.response.send_message(
                f"⏳ Tu as déjà pris ton daily. Réessaie dans {joli} ( <t:{expire}:R> ).",
                ephemeral=True
            )

        # ok : on crédite, on enregistre le nouveau timestamp
        new_total = await add_points(interaction.user.id, DAILY_REWARD)
        daily[uid] = now_ts
        _save_daily(daily)

    await interaction.response.send_message(
        f"🗓️ Daily pris ! +**{DAILY_REWARD}** points → total **{new_total}**.",
        ephemeral=True
    )

@tree.command(name="purchases", description="Voir ton historique d'achats boutique.")
@guilds_decorator()
async def purchases_cmd(interaction: discord.Interaction):
    async with _purchases_lock:
        p = _load_purchases()
    items = p.get(str(interaction.user.id), {})
    if not items:
        return await interaction.response.send_message("🧾 Aucun achat enregistré pour toi.", ephemeral=True)

    # On récupère les noms jolis depuis le shop
    async with _shop_lock:
        shop = _load_shop()
    lines = [f"**Achats de {interaction.user.display_name} :**"]
    for key, count in items.items():
        label = shop.get(key, {}).get("name", key)
        lines.append(f"- {label} (`{key}`) × **{count}**")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@tree.command(name="invites", description="Voir le nombre d'invitations d'un membre.")
@guilds_decorator()
@app_commands.describe(membre="Le membre (si vide, toi)")
async def invites_cmd(interaction: discord.Interaction, membre: discord.Member | None = None):
    cible = membre or interaction.user  # type: ignore
    total = await _get_invite_count(cible.id)
    await interaction.response.send_message(
        f"📨 **{cible.display_name}** a **{total}** invitation(s).",
        ephemeral=True
    )

@tree.command(name="ping", description="Test rapide de réponse du bot.")
@guilds_decorator()
async def ping_cmd(interaction: discord.Interaction):
    await interaction.response.send_message("Pong 🏓", ephemeral=True)

@tree.command(name="addpoints", description="Ajouter des points à un membre (admin seulement).")
@guilds_decorator()
@app_commands.describe(membre="Le membre à créditer", points="Nombre de points à ajouter (>=1)")
@app_commands.checks.has_permissions(administrator=True)
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

@tree.command(name="removepoints", description="Retirer des points à un membre (admin seulement).")
@guilds_decorator()
@app_commands.describe(membre="Le membre à débiter", points="Nombre de points à retirer (>=1)")
@app_commands.checks.has_permissions(administrator=True)
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
    
@tree.command(name="setpoints", description="Définir le solde exact d'un membre (admin).")
@guilds_decorator()
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

@tree.command(name="classement", description="Afficher le classement des points.")
@guilds_decorator()
@app_commands.describe(top="Combien d'utilisateurs afficher (par défaut 10)")
async def classement_cmd(interaction: discord.Interaction, top: app_commands.Range[int, 1, 50] = 10):
    await interaction.response.defer(ephemeral=False)
    lb = await get_leaderboard(interaction.guild, top=top)  # type: ignore
    if not lb:
        return await interaction.followup.send("Aucun point enregistré pour le moment.")
    lines = [f"**#{i}** — {name} : **{pts}**" for i, (name, pts) in enumerate(lb, start=1)]
    embed = discord.Embed(title=f"🏆 Classement — Top {top}", description="\n".join(lines), color=discord.Color.gold())
    await interaction.followup.send(embed=embed)

@tree.command(name="profile", description="Affiche un profil (points, achats, invites).")
@guilds_decorator()
@app_commands.describe(membre="(Optionnel) Le membre dont afficher le profil")
async def profile_cmd(interaction: discord.Interaction, membre: discord.Member | None = None):
    target: discord.Member = membre or interaction.user  # type: ignore
    uid = str(target.id)

    # --- Chargements (avec locks) ---
    async with _points_lock:
        points_map = _load_points()
        pts = int(points_map.get(uid, 0))

    async with _purchases_lock:
        purchases_map = _load_purchases()
        user_purchases = purchases_map.get(uid, {})

    invites = await _get_invite_count(target.id)

    # Daily status (si tu utilises déjà DAILY_DB)
    last_ts = 0
    try:
        async with _daily_lock:
            daily = _load_daily()
            last_ts = int(daily.get(uid, 0))
    except Exception:
        pass

    now_ts = int(datetime.now(timezone.utc).timestamp())
    daily_ready = True
    daily_eta_txt = "✅ Disponible"
    if last_ts:
        elapsed = now_ts - last_ts
        if elapsed < DAILY_COOLDOWN:
            remain = DAILY_COOLDOWN - elapsed
            daily_ready = False
            daily_eta_txt = f"⏳ Dans { _format_cooldown(remain) } ( <t:{now_ts + remain}:R> )"

    # --- Détails achats (jolis labels depuis shop) ---
    async with _shop_lock:
        shop_snapshot = _load_shop()

    # Tri des achats par quantité desc., puis clé
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

    # --- Apparence embed ---
    # Couleur = couleur du rôle le plus haut si défini, sinon blurple
    color = target.top_role.color if getattr(target, "top_role", None) and target.top_role.color.value else discord.Color.blurple()

    embed = discord.Embed(
        title=f"👤 Profil — {target.display_name}",
        color=color
    )

    # Thumbnail avatar
    embed.set_thumbnail(url=target.display_avatar.url)

    # Champs principaux
    embed.add_field(name="💰 Points", value=f"**{pts}**", inline=True)
    embed.add_field(name="🛒 Achats", value=f"**{total_achats}**", inline=True)
    embed.add_field(name="📨 Invitations", value=f"**{invites}**", inline=True)

    # Daily
    embed.add_field(name="🗓️ Daily", value=daily_eta_txt, inline=True)

    # Dates (création compte & join serveur)
    if target.created_at:
        created_ts = int(target.created_at.replace(tzinfo=timezone.utc).timestamp())
        embed.add_field(name="🆔 Compte créé", value=f"<t:{created_ts}:D> (<t:{created_ts}:R>)", inline=True)
    if target.joined_at:
        joined_ts = int(target.joined_at.replace(tzinfo=timezone.utc).timestamp())
        embed.add_field(name="🚪 Arrivée serveur", value=f"<t:{joined_ts}:D> (<t:{joined_ts}:R>)", inline=True)

    # Achats (aperçu)
    embed.add_field(name="🧾 Détails achats (aperçu)", value=achats_preview, inline=False)

    # Footer
    embed.set_footer(text=f"ID: {target.id}")

    # Si on regarde son propre profil → message privé (ephemeral). Pour un autre → public.
    is_self = (target.id == interaction.user.id)
    await interaction.response.send_message(embed=embed, ephemeral=is_self)

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
    # Solde
    async with _points_lock:
        points_data = _load_points()
        user_points = int(points_data.get(str(interaction.user.id), 0))
    # Shop
    async with _shop_lock:
        shop = _load_shop()
    if not shop:
        return await interaction.response.send_message("La boutique est vide pour le moment.", ephemeral=True)

    # Options (masque les items dont la limite est atteinte)
    options = []
    user_id = interaction.user.id
    for key, item in list(shop.items())[:25]:
        max_per = int(item.get("max_per_user", -1))
        already = await get_user_purchase_count(user_id, key)   # <-- await + bon nom
        if max_per >= 0 and already >= max_per:
            continue
        remaining_txt = "∞" if max_per < 0 else f"{max_per - already}"
        label = f"{item['name']}"
        desc = f"Prix: {item['cost']} pts — Restant: {remaining_txt}"
        options.append(discord.SelectOption(label=label, description=desc, value=key))

    if not options:
        return await interaction.response.send_message("Tu as déjà atteint la limite de tous les items disponibles. 😊", ephemeral=True)

    class ShopView(View):
        def __init__(self):
            super().__init__(timeout=60)
        async def on_timeout(self):
            for c in self.children:
                c.disabled = True
        @discord.ui.select(placeholder="Choisis un objet à acheter…", min_values=1, max_values=1, options=options)
        async def select_item(self, select_interaction: discord.Interaction, select: Select):
            key = select.values[0]
        
            # Relecture fraîche des données (prix, limites, solde)
            async with _shop_lock:
                shop_snapshot = _load_shop()
                item = shop_snapshot.get(key)
            if not item:
                return await select_interaction.response.send_message("❌ Cet item n'existe plus.", ephemeral=True)
        
            cost = int(item.get("cost", 0))
            role_id = int(item.get("role_id", 0))
            max_per = int(item.get("max_per_user", -1))
            already = await get_user_purchase_count(select_interaction.user.id, key)
        
            # Solde utilisateur
            async with _points_lock:
                points_data = _load_points()
                user_points = int(points_data.get(str(select_interaction.user.id), 0))
        
            # Texte récapitulatif
            desc_lines = []
            desc_lines.append(f"**Article :** {item.get('name', key)}")
            desc_lines.append(f"**Prix :** {cost} pts")
            if role_id:
                desc_lines.append(f"**Rôle :** <@&{role_id}>")
            if item.get("description"):
                desc_lines.append(f"**Description :** {item['description']}")
            if max_per >= 0:
                remaining = max(0, max_per - already)
                desc_lines.append(f"**Limite par utilisateur :** {max_per} (tu en as **{already}**, reste **{remaining}**)")
        
            desc_lines.append(f"**Ton solde :** {user_points} pts → **reste après achat :** {user_points - cost} pts")
            recap = "\n".join(desc_lines)
        
            # Affiche le récap + boutons
            embed = discord.Embed(
                title="🧾 Confirmer l’achat",
                description=recap,
                color=discord.Color.orange()
            )
        
            view = ConfirmBuy(user_points=user_points, user_id=select_interaction.user.id,
                              key=key, item=item, already=already)
        
            # On envoie une *nouvelle* réponse (ephemeral) et on laisse le message d'origine tel quel
            await select_interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    embed = discord.Embed(
        title="🛒 Boutique des Points",
        description=f"Tu as **{user_points}** points.\nSélectionne un item ci-dessous.",
        color=discord.Color.green()
    )
    await interaction.response.send_message(embed=embed, view=ShopView(), ephemeral=True)

    class ConfirmBuy(View):
        def __init__(self, user_points: int, user_id: int, key: str, item: dict, already: int):
            super().__init__(timeout=45)
            self.user_points = user_points
            self.user_id = user_id
            self.key = key
            self.item = item
            self.already = already
            self.cost = int(item.get("cost", 0))
    
        async def on_timeout(self):
            for c in self.children:
                c.disabled = True
    
        @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.success)
        async def confirm(self, i: discord.Interaction, _):
            # Double check rapide côté UX (le _handle_purchase refera les vérifs serveur)
            if self.user_points < self.cost:
                try:
                    await i.response.send_message("❌ Solde insuffisant au moment de la confirmation.", ephemeral=True)
                except Exception:
                    pass
                return
            await _handle_purchase(i, self.key)  # toutes les vérifs/transactions sont déjà dedans
            # Désactive la vue après action
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
                # fallback si edit impossible
                try:
                    await i.response.send_message("Achat annulé.", ephemeral=True)
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

    cost = int(item["cost"])
    name = item["name"]
    role_id = int(item.get("role_id", 0))
    max_per = int(item.get("max_per_user", -1))
    already = await get_user_purchase_count(interaction.user.id, key)  # <-- await + bon nom

    if max_per >= 0 and already >= max_per:
        return await interaction.response.send_message(
            f"❌ Tu as déjà acheté **{name}** le nombre maximum de fois autorisé ({max_per}).",
            ephemeral=True
        )
        
    if role_id:
        role = interaction.guild.get_role(role_id)
        if role and isinstance(interaction.user, discord.Member) and role in interaction.user.roles:
            return await interaction.response.send_message(
                f"❌ Tu as déjà le rôle **{role.name}**.",
                ephemeral=True
            )
    # Débit points
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
@tree.command(name="shopadmin", description="Ouvre le panneau admin de la boutique (admins uniquement).")
@guilds_decorator()
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
async def setup_hook():
    if GUILD_ID:
        cmds = await tree.sync(guild=discord.Object(id=GUILD_ID))
        logging.info("Synced %d cmd(s) pour la guilde %s", len(cmds), GUILD_ID)
    else:
        cmds = await tree.sync()
        logging.info("Synced %d cmd(s) globales", len(cmds))

@bot.event
async def on_ready():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("discord.app_commands").setLevel(logging.WARNING)
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
    # re-fetch après le join
    await _refresh_invite_cache(guild)
    after = _invite_cache.get(guild.id, {})
    code, inviter_id = _find_used_invite(before, after)
    if inviter_id:
        total = await _add_invite_for(inviter_id, member.id)
        await _send_invite_log(guild, f"👋 {member.mention} via code `{code}` par <@{inviter_id}> — total **{total}** invitation(s).")
        # Récompense points (une seule fois par invité unique)
        try:
            async with _invite_rewards_lock:
                rdb = _load_invite_rewards()
                rewarded = rdb.setdefault("rewarded", {})
                mid = str(member.id)

                if mid not in rewarded:
                    # Première fois que ce membre rejoint et crédite un parrain → on récompense
                    new_total_pts = await add_points(inviter_id, INVITE_REWARD_POINTS)
                    rewarded[mid] = int(inviter_id)
                    _save_invite_rewards(rdb)

                    # petit log / feedback côté staff (même salon que les joins si tu veux)
                    await _send_invite_log(
                        guild,
                        f"🎁 +{INVITE_REWARD_POINTS} pts pour <@{inviter_id}> (nouveau total: **{new_total_pts}**) — premier join crédité de {member.mention}."
                    )
                else:
                    # déjà récompensé par le passé → pas de points
                    pass
        except Exception:
            # on avale l’erreur pour ne pas bloquer l’event
            logging.exception("Invite reward error")
    else:
        # Cas vanity URL / impossible à déterminer
        await _send_invite_log(guild, f"👋 {member.mention} a rejoint, **invitation non déterminée** (vanity/permissions manquantes).")

@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    inviter_id, new_total = await _remove_invite_for_member(member.id)
    if inviter_id is not None:
        inviter_mention = f"<@{inviter_id}>"
        text = f"👋 {member.mention} a quitté le serveur, invité·e par {inviter_mention} et a maintenant **{new_total}** invitation(s)."
        await _send_invite_log(guild, text)

# ---------- Run ----------
if __name__ == "__main__":
    # Crée les fichiers si absents
    for ensure in ( _ensure_points_exists, _ensure_shop_exists, _ensure_purchases_exists ):
        try:
            ensure()
        except Exception:
            pass
    bot.run(TOKEN)




