import discord
from discord import app_commands
from discord.ext import commands, tasks
import asyncio
import os

# --- CONFIGURATION ---
TARGET_BOT_ID = 1509876788875231242 

class StatusBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.presences = True  # Required to see bot status
        super().__init__(command_prefix="!", intents=intents)
        
        # Global variable for the manual maintenance/update message
        self.maintenance_message = None
        
        # Database in memory (Recommended: replace with Supabase/Firebase for production)
        # Structure: { guild_id: { "channel_id": 111, "message_id": 222 } }
        self.guilds_data = {}

    async def setup_hook(self):
        # Starts the background loop
        self.check_bot_status.start()
        # Syncs slash commands globally
        await self.tree.sync()

bot = StatusBot()

@bot.event
async def on_ready():
    print(f'✅ Logged in as {bot.user.name}')

# --- 1. SLASH COMMAND: SETUP CHANNEL ---
@bot.tree.command(name="setup", description="Set up the channel where the status embed will be updated.")
@app_commands.describe(channel="The channel where the status will be posted")
@app_commands.checks.has_permissions(administrator=True)
async def setup(interaction: discord.Interaction, channel: discord.TextChannel):
    await interaction.response.defer(ephemeral=True)
    
    # Send an initial embed that will be updated by the loop
    embed = discord.Embed(title="🔄 Checking Status...", color=discord.Color.orange())
    initial_msg = await channel.send(embed=embed)
    
    # Save to our memory database
    bot.guilds_data[interaction.guild.id] = {
        "channel_id": channel.id,
        "message_id": initial_msg.id
    }
    
    await interaction.followup.send(f"✅ Status embed successfully set up in {channel.mention}!", ephemeral=True)

# Error handler for /setup if the user is not an Admin
@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message("❌ You need Administrator permissions to use this command.", ephemeral=True)

# --- 2. PREFIX COMMAND: MODE UPDATE (BOT OWNER ONLY) ---
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
        await ctx.send(f"⚠️ Global update mode enabled. Message sent to all servers:\n> {message}")
    else:
        bot.maintenance_message = None
        await ctx.send("🔄 Global update mode disabled. Switched back to automatic monitoring.")
    
    # Force trigger the status loop immediately so servers don't have to wait 1 minute
    await bot.check_bot_status()

# Error handler for !modeupdate if a non-owner tries to use it
@mode_update.error
async def mode_update_error(ctx, error):
    if isinstance(error, commands.NotOwner):
        await ctx.send("❌ Only the Bot Creator/Owner can use this command.", delete_after=5)

# --- 3. BACKGROUND TASK (RUNS EVERY 1 MINUTE) ---
@tasks.loop(minutes=1)
async def check_bot_status():
    for guild_id, data in list(bot.guilds_data.items()):
        guild = bot.get_guild(guild_id)
        if not guild:
            continue
            
        channel = guild.get_channel(data["channel_id"])
        if not channel:
            continue
            
        try:
            message = await channel.fetch_message(data["message_id"])
        except discord.NotFound:
            # If the embed message was deleted, skip it
            continue

        embed = discord.Embed(timestamp=discord.utils.utcnow())
        embed.set_footer(text="Last Updated")
        
        # Scenario A: Bot Owner forced a global manual message
        if bot.maintenance_message is not None:
            embed.title = "⚠️ System Update Notice"
            embed.description = bot.maintenance_message
            embed.color = discord.Color.red()
            
        # Scenario B: Automatic checking
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
        
        # Edit the existing message with the new embed
        try:
            await message.edit(embed=embed)
        except Exception as e:
            print(f"Error updating message in guild {guild_id}: {e}")

# Run the bot (Token is read from the DISCORD_TOKEN environment variable)
TOKEN = os.environ.get('DISCORD_TOKEN')
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN environment variable is not set. Add it in Railway's Variables tab.")

bot.run(TOKEN)
