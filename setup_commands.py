# setup_commands.py
import discord
from discord import app_commands
from discord.ext import commands
from guild_config import load_cfg, save_cfg

def is_admin():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.guild_permissions.administrator
    return app_commands.check(predicate)

class Setup(commands.Cog):
    def __init__(self, bot: commands.Bot, data_dir: str):
        self.bot = bot
        self.data_dir = data_dir

    @app_commands.guild_only()
    @is_admin()
    @app_commands.command(name="setup_channels", description="Lier les salons de logs")
    @app_commands.describe(shop_log="Salon logs achats boutique",
                           admin_log="Salon logs actions admin",
                           invite_log="Salon logs invitations")
    async def setup_channels(self, interaction: discord.Interaction,
                             shop_log: discord.TextChannel,
                             admin_log: discord.TextChannel,
                             invite_log: discord.TextChannel):
        cfg = load_cfg(self.data_dir, interaction.guild.id)
        cfg["channels"]["shop_log"] = shop_log.id
        cfg["channels"]["admin_log"] = admin_log.id
        cfg["channels"]["invite_log"] = invite_log.id
        save_cfg(self.data_dir, interaction.guild.id, cfg)
        await interaction.response.send_message("✅ Salons de logs configurés.", ephemeral=True)

    @app_commands.guild_only()
    @is_admin()
    @app_commands.command(name="setup_roles", description="Lier les rôles Bronze/Argent/Or")
    async def setup_roles(self, interaction: discord.Interaction,
                          bronze: discord.Role, argent: discord.Role, or_: discord.Role):
        cfg = load_cfg(self.data_dir, interaction.guild.id)
        cfg["roles"]["bronze"] = bronze.id
        cfg["roles"]["argent"] = argent.id
        cfg["roles"]["or"] = or_.id
        save_cfg(self.data_dir, interaction.guild.id, cfg)
        await interaction.response.send_message("✅ Rôles configurés.", ephemeral=True)

    @app_commands.guild_only()
    @is_admin()
    @app_commands.command(name="setup_invites", description="Configurer les points par invitation validée")
    async def setup_invites(self, interaction: discord.Interaction, points: app_commands.Range[int, 0, 100000]):
        cfg = load_cfg(self.data_dir, interaction.guild.id)
        cfg["params"]["invite_reward_points"] = points
        save_cfg(self.data_dir, interaction.guild.id, cfg)
        await interaction.response.send_message(f"✅ Invitations: {points} points/récompense.", ephemeral=True)

async def setup(bot: commands.Bot, data_dir: str):
    await bot.add_cog(Setup(bot, data_dir))
