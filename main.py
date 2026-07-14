import discord
from discord import app_commands
from discord.ext import commands, tasks
import asyncio
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import closing

# --- CONFIGURATION ---
TARGET_BOT_ID = 1509876788875231242

# Path to the SQLite database file. On Railway, attach a Volume (e.g. mounted
# at /app/data) and set DB_PATH=/app/data/bot_data.db in your environment
# variables so the data survives redeploys, not just restarts.
DB_PATH = os.environ.get("DB_PATH", "bot_data.db")

CHECK_INTERVAL_MINUTES = 1
# How many history rows to keep before pruning old ones (per guild).
HISTORY_RETENTION_DAYS = 30

# Recommendations sent to the bot owner's DM whenever the monitored bot goes offline.
OFFLINE_RECOMMENDATIONS = (
    "• Check your hosting dashboard (Railway/VPS/etc.) for crash logs\n"
    "• Verify the process wasn't OOM-killed or hit a CPU/memory limit\n"
    "• Check the Discord API status page: https://discordstatus.com\n"
    "• Confirm the bot's token hasn't been reset or revoked\n"
    "• Check whether the host redeployed/restarted the service unexpectedly\n"
    "• Restart the process manually if it isn't set to auto-restart"
)


# =========================================================================
# DATABASE LAYER
# =========================================================================
class Database:
    """
    Thin synchronous SQLite wrapper. Every public method opens and closes
    its own connection, which is fine for a low-traffic bot like this one.
    Callers from async code should wrap calls in asyncio.to_thread(...) so
    the event loop / heartbeat never gets blocked by disk I/O.
    """

    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self):
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id     INTEGER PRIMARY KEY,
                    channel_id   INTEGER NOT NULL,
                    message_id   INTEGER NOT NULL,
                    ping_role_id INTEGER,
                    ping_user_id INTEGER
                )
                """
            )
            # Migration for databases created before ping columns existed.
            existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(guild_config)")}
            if "ping_role_id" not in existing_cols:
                conn.execute("ALTER TABLE guild_config ADD COLUMN ping_role_id INTEGER")
            if "ping_user_id" not in existing_cols:
                conn.execute("ALTER TABLE guild_config ADD COLUMN ping_user_id INTEGER")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS status_history (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id  INTEGER NOT NULL,
                    status    TEXT NOT NULL,   -- 'online' | 'offline' | 'maintenance'
                    timestamp TEXT NOT NULL    -- ISO 8601 UTC
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_status_guild_time "
                "ON status_history (guild_id, timestamp)"
            )
            conn.commit()

    # --- guild config -----------------------------------------------------
    def upsert_guild_config(
        self,
        guild_id: int,
        channel_id: int,
        message_id: int,
        ping_role_id: int | None = None,
        ping_user_id: int | None = None,
    ):
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO guild_config (guild_id, channel_id, message_id, ping_role_id, ping_user_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    channel_id   = excluded.channel_id,
                    message_id   = excluded.message_id,
                    ping_role_id = excluded.ping_role_id,
                    ping_user_id = excluded.ping_user_id
                """,
                (guild_id, channel_id, message_id, ping_role_id, ping_user_id),
            )
            conn.commit()

    def remove_guild_config(self, guild_id: int):
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM guild_config WHERE guild_id = ?", (guild_id,))
            conn.execute("DELETE FROM status_history WHERE guild_id = ?", (guild_id,))
            conn.commit()

    def get_guild_config(self, guild_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT channel_id, message_id, ping_role_id, ping_user_id "
                "FROM guild_config WHERE guild_id = ?",
                (guild_id,),
            ).fetchone()
            if row:
                return {
                    "channel_id": row[0],
                    "message_id": row[1],
                    "ping_role_id": row[2],
                    "ping_user_id": row[3],
                }
            return None

    def get_all_guild_configs(self):
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT guild_id, channel_id, message_id, ping_role_id, ping_user_id FROM guild_config"
            ).fetchall()
            return {
                guild_id: {
                    "channel_id": channel_id,
                    "message_id": message_id,
                    "ping_role_id": ping_role_id,
                    "ping_user_id": ping_user_id,
                }
                for guild_id, channel_id, message_id, ping_role_id, ping_user_id in rows
            }

    def get_last_status(self, guild_id: int):
        """Returns the most recently logged status for a guild, or None if there's no history yet."""
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT status FROM status_history WHERE guild_id = ? "
                "ORDER BY id DESC LIMIT 1",
                (guild_id,),
            ).fetchone()
            return row[0] if row else None

    # --- status history -----------------------------------------------------
    def log_status(self, guild_id: int, status: str):
        with closing(self._connect()) as conn:
            conn.execute(
                "INSERT INTO status_history (guild_id, status, timestamp) VALUES (?, ?, ?)",
                (guild_id, status, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()

    def prune_old_history(self, days: int = HISTORY_RETENTION_DAYS):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM status_history WHERE timestamp < ?", (cutoff,))
            conn.commit()

    def get_uptime_stats(self, guild_id: int, hours: int):
        """
        Returns (uptime_percentage, sample_count) for the given window.
        'maintenance' rows are excluded from the calculation entirely,
        since they don't reflect the real online/offline state.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT status FROM status_history
                WHERE guild_id = ? AND timestamp >= ? AND status != 'maintenance'
                """,
                (guild_id, cutoff),
            ).fetchall()

        total = len(rows)
        if total == 0:
            return None, 0

        online_count = sum(1 for (status,) in rows if status == "online")
        percentage = round((online_count / total) * 100, 2)
        return percentage, total

    # --- global bot settings -----------------------------------------------------
    def set_setting(self, key: str, value: str | None):
        with closing(self._connect()) as conn:
            if value is None:
                conn.execute("DELETE FROM bot_settings WHERE key = ?", (key,))
            else:
                conn.execute(
                    """
                    INSERT INTO bot_settings (key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, value),
                )
            conn.commit()

    def get_setting(self, key: str):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT value FROM bot_settings WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else None


db = Database(DB_PATH)


# =========================================================================
# BOT
# =========================================================================
class StatusBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.presences = True  # Required to see bot status
        super().__init__(command_prefix="!", intents=intents)

        # Loaded from the database at startup so it survives restarts.
        self.maintenance_message = None

        # Cached owner id, used to DM whoever owns the bot application when
        # the monitored bot's global status flips. Populated in setup_hook.
        self.owner_id = None

    async def setup_hook(self):
        # Restore persisted maintenance message, if any.
        self.maintenance_message = await asyncio.to_thread(
            db.get_setting, "maintenance_message"
        )

        # Resolve the bot owner (whoever owns the application in the Dev
        # Portal) so we know who to DM on offline/online transitions.
        try:
            app_info = await self.application_info()
            self.owner_id = app_info.owner.id
        except Exception as e:
            print(f"⚠️ Could not resolve application owner: {e}")

        # Starts the background loop (check_bot_status is a module-level task, not a method)
        check_bot_status.start()
        prune_old_history_task.start()

        # Syncs slash commands globally
        await self.tree.sync()


bot = StatusBot()


@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user.name}")


def build_status_embed(guild: discord.Guild, uptime_24h=None) -> discord.Embed:
    """Builds the embed shown in the tracked status channel."""
    embed = discord.Embed(timestamp=discord.utils.utcnow())
    embed.set_footer(text="Last Updated")

    if bot.maintenance_message is not None:
        embed.title = "⚠️ System Update Notice"
        embed.description = bot.maintenance_message
        embed.color = discord.Color.red()
    else:
        target_member = guild.get_member(TARGET_BOT_ID)

        if target_member and target_member.status != discord.Status.offline:
            embed.title = "🟢 Bot is Online"
            embed.description = "The bot is currently up and running smoothly."
            embed.color = discord.Color.green()
        else:
            embed.title = "🔴 Bot is Offline"
            embed.description = "The bot is currently down or experiencing issues."
            embed.color = discord.Color.dark_red()

    if uptime_24h is not None:
        embed.add_field(name="Uptime (24h)", value=f"{uptime_24h}%", inline=True)

    return embed


def current_status_label(guild: discord.Guild) -> str:
    """Returns 'online' / 'offline' / 'maintenance' for history logging."""
    if bot.maintenance_message is not None:
        return "maintenance"
    target_member = guild.get_member(TARGET_BOT_ID)
    if target_member and target_member.status != discord.Status.offline:
        return "online"
    return "offline"


def get_target_status_anywhere() -> str | None:
    """
    Looks for TARGET_BOT_ID across every guild this bot shares with it and
    returns a single 'online' / 'offline' label, independent of any one
    guild's config. Returns None if the target bot isn't visible in any
    shared guild yet (e.g. right after startup, before caches populate).
    """
    for guild in bot.guilds:
        member = guild.get_member(TARGET_BOT_ID)
        if member:
            return "online" if member.status != discord.Status.offline else "offline"
    return None


def build_mention_string(config: dict) -> str | None:
    """Builds a '<@&role> <@user>' mention string from a guild config, or None if unset."""
    mentions = []
    if config.get("ping_role_id"):
        mentions.append(f"<@&{config['ping_role_id']}>")
    if config.get("ping_user_id"):
        mentions.append(f"<@{config['ping_user_id']}>")
    return " ".join(mentions) if mentions else None


TRANSITION_TEXT = {
    "online": "🟢 **Status changed to Online** — the monitored bot is back up.",
    "offline": "🔴 **Status changed to Offline** — the monitored bot appears to be down!",
    "maintenance": "⚠️ **Maintenance mode enabled** for the monitored bot.",
}


async def notify_status_change(channel: discord.abc.Messageable, config: dict, new_status: str):
    """Sends a separate ping message (if a role/user is configured) when status changes."""
    mention = build_mention_string(config)
    text = TRANSITION_TEXT.get(new_status, f"Status changed to **{new_status}**.")
    if mention:
        text = f"{mention} {text}"
    try:
        await channel.send(
            text,
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
        )
    except discord.Forbidden:
        pass


async def notify_owner_of_global_status_change(new_status: str):
    """
    DMs the bot application owner whenever the *monitored* bot's real
    presence flips between online/offline (independent of maintenance mode
    and independent of any single guild's setup). Offline DMs include a
    short checklist of things to look at.
    """
    owner_id = bot.owner_id
    if not owner_id:
        return

    try:
        owner = bot.get_user(owner_id) or await bot.fetch_user(owner_id)
    except discord.NotFound:
        return

    if new_status == "offline":
        embed = discord.Embed(
            title="🔴 Monitored bot just went OFFLINE",
            description="The bot you're tracking dropped offline. Here's what to check:",
            color=discord.Color.red(),
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Recommended steps", value=OFFLINE_RECOMMENDATIONS, inline=False)
        content = "⚠️ **Ping:** your monitored bot appears to be down."
    else:
        embed = discord.Embed(
            title="🟢 Monitored bot is back ONLINE",
            description="The bot you're tracking has recovered and is showing an online presence again.",
            color=discord.Color.green(),
            timestamp=discord.utils.utcnow(),
        )
        content = "✅ Your monitored bot is back online."

    try:
        await owner.send(content=content, embed=embed)
    except discord.Forbidden:
        # Owner has DMs disabled / blocked the bot — nothing else we can do.
        print("⚠️ Could not DM the bot owner (DMs closed or blocked).")


# --- 1. SLASH COMMAND: SETUP CHANNEL ---
@bot.tree.command(name="setup", description="Set up the channel where the status embed will be updated.")
@app_commands.describe(
    channel="The channel where the status will be posted",
    ping_role="Optional: role to tag whenever the status changes",
    ping_user="Optional: user to tag whenever the status changes",
)
@app_commands.checks.has_permissions(administrator=True)
async def setup(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    ping_role: discord.Role = None,
    ping_user: discord.Member = None,
):
    await interaction.response.defer(ephemeral=True)

    # Send an initial embed that will be updated by the loop
    embed = discord.Embed(title="🔄 Checking Status...", color=discord.Color.orange())
    initial_msg = await channel.send(embed=embed)

    await asyncio.to_thread(
        db.upsert_guild_config,
        interaction.guild.id,
        channel.id,
        initial_msg.id,
        ping_role.id if ping_role else None,
        ping_user.id if ping_user else None,
    )

    confirmation = f"✅ Status embed successfully set up in {channel.mention}!"
    if ping_role or ping_user:
        tagged = " and ".join(
            filter(None, [ping_role.mention if ping_role else None, ping_user.mention if ping_user else None])
        )
        confirmation += f"\n{tagged} will be tagged whenever the status changes."

    await interaction.followup.send(confirmation, ephemeral=True)


@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message(
            "❌ You need Administrator permissions to use this command.", ephemeral=True
        )


# --- 2. SLASH COMMAND: REMOVE SETUP ---
@bot.tree.command(name="remove", description="Stop status tracking in this server and delete the embed message.")
@app_commands.checks.has_permissions(administrator=True)
async def remove(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    config = await asyncio.to_thread(db.get_guild_config, interaction.guild.id)
    if not config:
        await interaction.followup.send(
            "ℹ️ This server doesn't have a status tracker set up.", ephemeral=True
        )
        return

    channel = interaction.guild.get_channel(config["channel_id"])
    if channel:
        try:
            msg = await channel.fetch_message(config["message_id"])
            await msg.delete()
        except discord.NotFound:
            pass
        except discord.Forbidden:
            pass

    await asyncio.to_thread(db.remove_guild_config, interaction.guild.id)

    await interaction.followup.send(
        "🗑️ Status tracking removed for this server.", ephemeral=True
    )


@remove.error
async def remove_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message(
            "❌ You need Administrator permissions to use this command.", ephemeral=True
        )


# --- 3. SLASH COMMAND: MANUAL STATUS CHECK ---
@bot.tree.command(name="status", description="Run an immediate status check, without waiting for the automatic loop.")
@app_commands.checks.has_permissions(administrator=True)
async def status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    label = current_status_label(interaction.guild)
    uptime_24h, samples = await asyncio.to_thread(db.get_uptime_stats, interaction.guild.id, 24)

    embed = build_status_embed(interaction.guild, uptime_24h=uptime_24h)
    await interaction.followup.send(embed=embed, ephemeral=True)

    # Also push the update to the tracked channel/message right away, if configured.
    config = await asyncio.to_thread(db.get_guild_config, interaction.guild.id)
    if config:
        channel = interaction.guild.get_channel(config["channel_id"])
        if channel:
            try:
                msg = await channel.fetch_message(config["message_id"])
                await msg.edit(embed=embed)
            except discord.NotFound:
                pass

    await asyncio.to_thread(db.log_status, interaction.guild.id, label)


@status.error
async def status_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message(
            "❌ You need Administrator permissions to use this command.", ephemeral=True
        )


# --- 4. SLASH COMMAND: UPTIME STATS ---
@bot.tree.command(name="uptime", description="Show uptime statistics for the tracked bot in this server.")
async def uptime(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    config = await asyncio.to_thread(db.get_guild_config, interaction.guild.id)
    if not config:
        await interaction.followup.send(
            "ℹ️ This server doesn't have a status tracker set up. Use `/setup` first.",
            ephemeral=True,
        )
        return

    pct_24h, samples_24h = await asyncio.to_thread(db.get_uptime_stats, interaction.guild.id, 24)
    pct_7d, samples_7d = await asyncio.to_thread(db.get_uptime_stats, interaction.guild.id, 24 * 7)

    embed = discord.Embed(title="📊 Uptime Statistics", color=discord.Color.blurple())

    if pct_24h is None:
        embed.add_field(name="Last 24 hours", value="No data yet", inline=True)
    else:
        embed.add_field(name="Last 24 hours", value=f"{pct_24h}% ({samples_24h} samples)", inline=True)

    if pct_7d is None:
        embed.add_field(name="Last 7 days", value="No data yet", inline=True)
    else:
        embed.add_field(name="Last 7 days", value=f"{pct_7d}% ({samples_7d} samples)", inline=True)

    embed.set_footer(text=f"Samples are taken roughly every {CHECK_INTERVAL_MINUTES} minute(s)")
    await interaction.followup.send(embed=embed, ephemeral=True)


# --- 5. PREFIX COMMAND: MODE UPDATE (BOT OWNER ONLY) ---
@bot.command(name="modeupdate")
@commands.is_owner()  # Strictly restricts this command to the Bot Creator/Owner
async def mode_update(ctx, *, message: str = None):
    """
    Allows the Bot Owner to override the automatic check with a custom global message.
    Usage: !modeupdate <your message>
    To reset: !modeupdate
    """
    if message:
        bot.maintenance_message = message
        await asyncio.to_thread(db.set_setting, "maintenance_message", message)
        await ctx.send(f"⚠️ Global update mode enabled. Message sent to all servers:\n> {message}")
    else:
        bot.maintenance_message = None
        await asyncio.to_thread(db.set_setting, "maintenance_message", None)
        await ctx.send("🔄 Global update mode disabled. Switched back to automatic monitoring.")

    # Force trigger the status loop immediately so servers don't have to wait 1 minute
    await check_bot_status()


@mode_update.error
async def mode_update_error(ctx, error):
    if isinstance(error, commands.NotOwner):
        await ctx.send("❌ Only the Bot Creator/Owner can use this command.", delete_after=5)


# =========================================================================
# BACKGROUND TASKS
# =========================================================================
@tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
async def check_bot_status():
    # --- Global (guild-independent) online/offline tracking -------------
    # This drives the owner DM below and is separate from the per-guild
    # embeds/pings, so it still fires even for guilds that haven't run
    # /setup, and won't fire spuriously while maintenance mode is on.
    if bot.maintenance_message is None:
        global_status = get_target_status_anywhere()
        if global_status is not None:
            previous_global_status = await asyncio.to_thread(db.get_setting, "last_global_status")
            if previous_global_status is not None and previous_global_status != global_status:
                await notify_owner_of_global_status_change(global_status)
            if previous_global_status != global_status:
                await asyncio.to_thread(db.set_setting, "last_global_status", global_status)

    configs = await asyncio.to_thread(db.get_all_guild_configs)

    for guild_id, data in list(configs.items()):
        guild = bot.get_guild(guild_id)
        if not guild:
            continue

        channel = guild.get_channel(data["channel_id"])
        if not channel:
            continue

        try:
            message = await channel.fetch_message(data["message_id"])
        except discord.NotFound:
            # The embed message was deleted out from under us — clean up
            # the stale config instead of retrying forever.
            await asyncio.to_thread(db.remove_guild_config, guild_id)
            continue

        label = current_status_label(guild)
        uptime_24h, _ = await asyncio.to_thread(db.get_uptime_stats, guild_id, 24)
        embed = build_status_embed(guild, uptime_24h=uptime_24h)

        try:
            await message.edit(embed=embed)
        except Exception as e:
            print(f"Error updating message in guild {guild_id}: {e}")
            continue

        last_status = await asyncio.to_thread(db.get_last_status, guild_id)
        await asyncio.to_thread(db.log_status, guild_id, label)

        # Only ping on an actual transition, and never on the very first
        # sample (last_status is None), to avoid a spurious ping at setup.
        if last_status is not None and last_status != label:
            await notify_status_change(channel, data, label)


@check_bot_status.before_loop
async def before_check_bot_status():
    await bot.wait_until_ready()


@tasks.loop(hours=24)
async def prune_old_history_task():
    await asyncio.to_thread(db.prune_old_history, HISTORY_RETENTION_DAYS)


@prune_old_history_task.before_loop
async def before_prune_old_history_task():
    await bot.wait_until_ready()


# =========================================================================
# ENTRYPOINT
# =========================================================================
TOKEN = os.environ.get("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN environment variable is not set. Add it in Railway's Variables tab.")

bot.run(TOKEN)
