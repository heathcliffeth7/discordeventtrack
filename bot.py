import discord
from discord.ext import commands, tasks
from discord.ext.commands import CheckFailure
import json
import os
import xlsxwriter
import datetime

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

AUTHORIZED_ROLES = [111111111111111111, 222222222222222222]
TARGET_ROLES = [333333333333333333, 444444444444444444]

EXCEL_FILE_PATH = "server_statistics.xlsx"

def admin_only():
    async def predicate(ctx):
        if any(role.id in AUTHORIZED_ROLES for role in ctx.author.roles):
            return True
        try:
            await ctx.message.delete()
        except:
            pass
        raise CheckFailure("You do not have permission.")
    return commands.check(predicate)

# Değişkenin adını stats_data yaptık
if os.path.exists("stats.json"):
    with open("stats.json", "r") as f:
        stats_data = json.load(f)
else:
    stats_data = {}

def save_stats():
    with open("stats.json", "w") as f:
        json.dump(stats_data, f, indent=2)

def clean_temp_excels():
    for f in os.listdir():
        if f.endswith(".xlsx") and f != EXCEL_FILE_PATH:
            try:
                os.remove(f)
            except:
                pass

def generate_excel(guild):
    clean_temp_excels()
    if os.path.exists(EXCEL_FILE_PATH):
        os.remove(EXCEL_FILE_PATH)

    event_list = []
    for data in stats_data.values():
        for e in data.get("events", []):
            if e not in event_list:
                event_list.append(e)
        for w in data.get("winners", []):
            if w not in event_list:
                event_list.append(w)

    event_list.sort()

    workbook = xlsxwriter.Workbook(EXCEL_FILE_PATH)
    sheet = workbook.add_worksheet("Statistics")

    headers = [
        "User Name",
        "User ID",
        "Roles",
        "Joined Events Count",
        "Won Events Count"
    ] + event_list

    for col, head in enumerate(headers):
        sheet.write(0, col, head)

    row = 1
    for member in guild.members:
        if not any(role.id in TARGET_ROLES for role in member.roles):
            continue

        user_id = str(member.id)
        data = stats_data.get(user_id, {"events": [], "winners": []})
        role_names = [role.name for role in member.roles if role.name != "@everyone"]
        joined_count = len(data["events"])
        won_count = len(data["winners"])

        sheet.write(row, 0, member.display_name)
        sheet.write(row, 1, user_id)
        sheet.write(row, 2, ", ".join(role_names))
        sheet.write(row, 3, joined_count)
        sheet.write(row, 4, won_count)

        for col_index, event_name in enumerate(event_list, start=5):
            if event_name in data["winners"]:
                status = "🏆"
            elif event_name in data["events"]:
                status = "✅"
            else:
                status = ""
            sheet.write(row, col_index, status)

        row += 1

    workbook.close()

@bot.command(name="helpcommand")
@admin_only()
async def help_command(ctx):
    """
    Yardım komutu, tüm komutları ve özellikle düzeltme komutlarındaki 'mod' değerini ayrıntılı açıklar.
    """
    embed = discord.Embed(
        title="📘 Command List",
        description="All available commands:",
        color=discord.Color.teal()
    )
    embed.add_field(
        name="!stats [@user]",
        value="Shows a user's stats.",
        inline=False
    )
    embed.add_field(
        name="!allstats",
        value="Generates and sends the Excel file.",
        inline=False
    )
    embed.add_field(
        name="!addevent <event_name> <id...>",
        value="Adds users to an event (events).",
        inline=False
    )
    embed.add_field(
        name="!eventwinners <event_name> <id...>",
        value="Marks specified users as winners.",
        inline=False
    )
    embed.add_field(
        name="!notjoined <event_name> <id...>",
        value="Removes users completely from an event.",
        inline=False
    )
    embed.add_field(
        name="!delevent <event_name>",
        value="Deletes an event from all users.",
        inline=False
    )
    embed.add_field(
        name="!copyevent <event_name> <channel_id>",
        value="Copies all members from a channel into the event.",
        inline=False
    )

    # Düzeltme komutlarını daha detaylı açıklayalım
    embed.add_field(
        name="!fixwinners <mod> <event_name> <id...>",
        value=(
            "**Fixes 'winner' status**\n"
            "• **mod='joined'** → Remove from winners, add to events.\n"
            "• **mod='notjoined'** → Remove from winners, remove from events too.\n"
            "Use this if someone was incorrectly marked as a winner or needs to revert."
        ),
        inline=False
    )
    embed.add_field(
        name="!fixjoined <mod> <event_name> <id...>",
        value=(
            "**Fixes 'joined' status**\n"
            "• **mod='winner'** → Remove from events, add to winners.\n"
            "• **mod='notjoined'** → Remove from both events and winners.\n"
            "Use this if they were incorrectly joined or need to become a winner."
        ),
        inline=False
    )
    embed.add_field(
        name="!fixnotjoined <mod> <event_name> <id...>",
        value=(
            "**Fixes 'not joined' status**\n"
            "• **mod='joined'** → Add them to events (if they were missing).\n"
            "• **mod='winner'** → Remove them from events, add to winners.\n"
            "Use this if someone was marked as not joined but actually participated or won."
        ),
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command()
@admin_only()
async def allstats(ctx):
    generate_excel(ctx.guild)
    save_stats()
    if os.path.exists(EXCEL_FILE_PATH):
        await ctx.send(file=discord.File(EXCEL_FILE_PATH))

@bot.command()
@admin_only()
async def stats(ctx, member: discord.Member = None):
    member = member or ctx.author
    user_id = str(member.id)
    if user_id not in stats_data:
        await ctx.send("No data found for this user.")
        return

    data = stats_data[user_id]
    events_joined = data.get("events", [])
    events_won = data.get("winners", [])

    embed = discord.Embed(
        title=f"Stats for {member.display_name}",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="Joined Events",
        value=len(events_joined),
        inline=False
    )
    embed.add_field(
        name="Won Events",
        value=len(events_won),
        inline=False
    )
    embed.add_field(
        name="Events",
        value=", ".join(events_joined) or "None",
        inline=False
    )
    embed.add_field(
        name="Winners",
        value=", ".join(events_won) or "None",
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command()
@admin_only()
async def addevent(ctx, event_name: str, *user_ids):
    for uid in user_ids:
        if uid not in stats_data:
            stats_data[uid] = {"events": [], "winners": []}
        if event_name not in stats_data[uid]["events"]:
            stats_data[uid]["events"].append(event_name)
    save_stats()
    await ctx.send(f"Users added to the '{event_name}' event.")

@bot.command()
@admin_only()
async def eventwinners(ctx, event_name: str, *user_ids):
    for uid in user_ids:
        if uid not in stats_data:
            stats_data[uid] = {"events": [], "winners": []}
        if event_name in stats_data[uid]["events"]:
            stats_data[uid]["events"].remove(event_name)
        if event_name not in stats_data[uid]["winners"]:
            stats_data[uid]["winners"].append(event_name)
    save_stats()
    await ctx.send(f"Winners assigned for '{event_name}'.")

@bot.command()
@admin_only()
async def notjoined(ctx, event_name: str, *user_ids):
    for uid in user_ids:
        if uid in stats_data:
            if event_name in stats_data[uid]["events"]:
                stats_data[uid]["events"].remove(event_name)
            if event_name in stats_data[uid]["winners"]:
                stats_data[uid]["winners"].remove(event_name)
    save_stats()
    await ctx.send(f"Users removed from '{event_name}'.")

@bot.command()
@admin_only()
async def delevent(ctx, event_name: str):
    for uid in stats_data:
        if event_name in stats_data[uid]["events"]:
            stats_data[uid]["events"].remove(event_name)
        if event_name in stats_data[uid]["winners"]:
            stats_data[uid]["winners"].remove(event_name)
    save_stats()
    await ctx.send(f"The event '{event_name}' has been removed from all users.")

@bot.command()
@admin_only()
async def copyevent(ctx, event_name: str, channel_id: int):
    channel = ctx.guild.get_channel(channel_id)
    if not channel:
        await ctx.send("Channel not found.")
        return
    count = 0
    for m in channel.members:
        uid = str(m.id)
        if uid not in stats_data:
            stats_data[uid] = {"events": [], "winners": []}
        if event_name not in stats_data[uid]["events"]:
            stats_data[uid]["events"].append(event_name)
            count += 1
    save_stats()
    await ctx.send(f"{count} members have been automatically added to '{event_name}'.")

@bot.command()
@admin_only()
async def fixwinners(ctx, mode: str, event_name: str, *user_ids):
    for uid in user_ids:
        if uid in stats_data:
            if event_name in stats_data[uid]["winners"]:
                stats_data[uid]["winners"].remove(event_name)
            if mode == "joined" and event_name not in stats_data[uid]["events"]:
                stats_data[uid]["events"].append(event_name)
            elif mode == "notjoined" and event_name in stats_data[uid]["events"]:
                stats_data[uid]["events"].remove(event_name)
    save_stats()
    await ctx.send(f"Winners fixed for '{event_name}' event.")

@bot.command()
@admin_only()
async def fixjoined(ctx, mode: str, event_name: str, *user_ids):
    for uid in user_ids:
        if uid in stats_data:
            if event_name in stats_data[uid]["events"]:
                stats_data[uid]["events"].remove(event_name)
            if mode == "winner" and event_name not in stats_data[uid]["winners"]:
                stats_data[uid]["winners"].append(event_name)
            elif mode == "notjoined" and event_name in stats_data[uid]["winners"]:
                stats_data[uid]["winners"].remove(event_name)
    save_stats()
    await ctx.send(f"Joined status fixed for '{event_name}' event.")

@bot.command()
@admin_only()
async def fixnotjoined(ctx, mode: str, event_name: str, *user_ids):
    for uid in user_ids:
        if uid not in stats_data:
            stats_data[uid] = {"events": [], "winners": []}
        if mode == "joined":
            if event_name not in stats_data[uid]["events"]:
                stats_data[uid]["events"].append(event_name)
        elif mode == "winner":
            if event_name in stats_data[uid]["events"]:
                stats_data[uid]["events"].remove(event_name)
            if event_name not in stats_data[uid]["winners"]:
                stats_data[uid]["winners"].append(event_name)
    save_stats()
    await ctx.send(f"'Not joined' status fixed for '{event_name}' event.")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, CheckFailure):
        try:
            await ctx.message.delete()
        except:
            pass
        return
    else:
        await ctx.send(f"An error occurred: {error}")

@bot.event
async def on_ready():
    print(f"Bot is active: {bot.user}")
    if not update_members_loop.is_running():
        update_members_loop.start()

from discord.ext import tasks

@tasks.loop(minutes=30)
async def update_members_loop():
    for guild in bot.guilds:
        for member in guild.members:
            if not any(role.id in TARGET_ROLES for role in member.roles):
                continue
            user_id = str(member.id)
            if user_id not in stats_data:
                stats_data[uid] = {"events": [], "winners": []}  # ← Hata var
    save_stats()
    for guild in bot.guilds:
        generate_excel(guild)

bot.run("YOUR_BOT_TOKEN")
