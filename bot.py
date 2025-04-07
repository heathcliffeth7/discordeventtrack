import discord
from discord.ext import commands, tasks
from discord.ext.commands import CheckFailure, MissingRequiredArgument, BadArgument, CommandError, MemberNotFound
import json
import os
import xlsxwriter
import datetime
import re
import asyncio # Added for bulk operations
from datetime import time, date, timedelta, timezone # Time related imports
import traceback # Added for error logging

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
intents.members = True          # REQUIRED for accessing member list (ENABLE IN DEV PORTAL)
intents.voice_states = True
bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True) # Commands are case-insensitive

# --- CONFIGURATION ---
# !! REPLACE THESE WITH YOUR ACTUAL ROLE IDS !!
AUTHORIZED_ROLES = [111111111111111111, 222222222222222222] # Roles that can use admin commands
TARGET_ROLES = [333333333333333333, 444444444444444444]     # Roles to be included in statistics
EXCEL_FILE_PATH = "server_statistics.xlsx"
STATS_FILE_PATH = "stats.json"

# --- DATA LOADING & GLOBAL VARS ---
if os.path.exists(STATS_FILE_PATH):
    try:
        # Specify UTF-8 encoding for broader compatibility
        with open(STATS_FILE_PATH, "r", encoding='utf-8') as f: stats_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e: print(f"ERROR: Failed to load {STATS_FILE_PATH}: {e}. Starting with empty stats."); stats_data = {}
else: stats_data = {}

config_data = stats_data.setdefault("config", {})
twitter_log_channel_id = config_data.get("twitter_log_channel_id", None) # None if not set
# Load posted links into a set for efficient checking, ensure list exists first
posted_links_list = stats_data.setdefault("posted_twitter_links", [])
posted_links_set = set(posted_links_list) # Use set for quick lookups
# Default user data structure template
DEFAULT_USER_TEMPLATE = lambda: {
    "events": [],
    "winners": [],
    "twitter_links": [],
    "total_message_count": 0
}

# --- HELPER FUNCTIONS ---
def admin_only():
    """Decorator check for authorized roles or bot owner."""
    async def predicate(ctx):
        if ctx.guild is None: return False # Ignore DMs
        if await bot.is_owner(ctx.author): return True
        if any(role.id in AUTHORIZED_ROLES for role in ctx.author.roles): return True
        try: await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException): pass
        raise CheckFailure("You do not have permission to use this command.") # English Error
    return commands.check(predicate)

def save_stats():
    """Saves the current stats_data dictionary to the JSON file."""
    global twitter_log_channel_id
    try:
        stats_data.setdefault("config", {})["twitter_log_channel_id"] = twitter_log_channel_id
        stats_data["posted_twitter_links"] = sorted(list(posted_links_set))

        with open(STATS_FILE_PATH, "w", encoding='utf-8') as f:
            json.dump(stats_data, f, indent=2)
    except IOError as e: print(f"ERROR: Could not save stats to {STATS_FILE_PATH}: {e}")
    except Exception as e: print(f"UNEXPECTED ERROR (save_stats): {e}"); traceback.print_exc()

def clean_temp_excels():
    """Removes temporary/old .xlsx files (optional function)."""
    removed_count = 0
    for f in os.listdir('.'):
        if f.endswith(".xlsx") and f != EXCEL_FILE_PATH:
            try:
                file_path = os.path.join('.', f)
                os.remove(file_path)
                removed_count += 1
            except OSError as e: print(f"Could not remove temporary file {f}: {e}")
    # if removed_count > 0: print(f"Cleaned {removed_count} temporary Excel file(s).") # Optional log

def parse_filter(filter_str):
    """Parses a filter string like 'msgcount>500' into ('field', 'operator', value)."""
    if not filter_str or not filter_str.lower().startswith("msgcount"): return None
    match = re.search(r"(?i)msgcount\s*(!=|>=|<=|>|<|=)\s*(\d+)", filter_str.strip())
    if match:
        operator, value_str = match.groups()[0], match.groups()[1]
        try: value = int(value_str); field = "total_message_count"; return (field, operator, value)
        except ValueError: print(f"Filter parse error: Invalid number '{value_str}'"); return None
    else: print(f"Filter parse error: Invalid format '{filter_str}'"); return None

# --- generate_excel (With Filtering and Sorting) ---
def generate_excel(guild, sort_key=None, filter_details=None):
    """Generates the Excel statistics file with optional filtering and sorting."""
    if os.path.exists(EXCEL_FILE_PATH):
        try: os.remove(EXCEL_FILE_PATH)
        except OSError as e: print(f"Could not delete existing Excel: {e}")

    event_list_set = set(); user_data_for_excel = []
    processed_user_ids = set()

    print(f"Gathering/filtering data for Excel ({guild.name}). Filter: {filter_details}, Sort: {sort_key}")
    for user_id_str, data in stats_data.items():
        if not user_id_str.isdigit() or not isinstance(data, dict) or user_id_str in processed_user_ids: continue
        user_id = int(user_id_str); member = guild.get_member(user_id)
        if not member or not any(role.id in TARGET_ROLES for role in member.roles): continue
        passes_filter = True
        if filter_details:
            field, operator, filter_value = filter_details; user_value = data.get(field, 0)
            op_map = { ">": lambda a, b: a > b, "<": lambda a, b: a < b, ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b, "=": lambda a, b: a == b, "!=": lambda a, b: a != b }
            if operator in op_map: passes_filter = op_map[operator](user_value, filter_value)
            else: print(f"Warning: Unknown filter op: '{operator}'. Skipping filter."); passes_filter = True
        if not passes_filter: continue
        processed_user_ids.add(user_id_str)
        event_list_set.update(data.get("events", [])); event_list_set.update(data.get("winners", []))
        role_names = [r.name for r in member.roles if r.name != "@everyone"]
        user_data_for_excel.append({
            "member": member, "user_id": user_id_str, "roles": ", ".join(role_names),
            "joined_count": len(data.get("events", [])), "won_count": len(data.get("winners", [])),
            "total_message_count": data.get("total_message_count", 0), "tweet_count": len(data.get("twitter_links", [])),
            "twitter_links_list": data.get("twitter_links", []), "raw_data": data })

    sorted_event_list = sorted(list(event_list_set))
    print(f"{len(user_data_for_excel)} members to process. Sorting...")

    # Sort Data
    if sort_key == "messages": user_data_for_excel.sort(key=lambda x: (-x["total_message_count"], x["member"].display_name.lower()))
    elif sort_key == "tweets": user_data_for_excel.sort(key=lambda x: (-x["tweet_count"], x["member"].display_name.lower()))
    else: user_data_for_excel.sort(key=lambda x: x["member"].display_name.lower()) # Default sort by name

    print("Writing Excel file...")
    # Write Excel
    try:
        workbook = xlsxwriter.Workbook(EXCEL_FILE_PATH)
        sheet = workbook.add_worksheet("Statistics")
        # Define Headers (English)
        headers = ["User Name", "User ID", "Roles", "Joined Events Count", "Won Events Count", "Total Messages Sent", "Tweet Count"] + sorted_event_list + ["Posted Twitter Links"]
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'align': 'center', 'valign': 'vcenter'})
        header_map = {head: idx for idx, head in enumerate(headers)}
        for col, head in enumerate(headers): sheet.write(0, col, head, header_format)

        # Define Cell Formats
        row_format=workbook.add_format({'valign': 'top'}); wrap_format=workbook.add_format({'text_wrap': True, 'valign': 'top'}); num_format=workbook.add_format({'valign': 'top', 'num_format': '#,##0'})
        url_format = workbook.add_format({'font_color': 'blue', 'underline': 1, 'valign': 'top', 'text_wrap': True}) # URL format

        # Write Data Rows
        for row_idx, user_info in enumerate(user_data_for_excel):
            row = row_idx + 1; data = user_info["raw_data"]
            sheet.write(row, header_map.get("User Name"), user_info["member"].display_name, row_format)
            sheet.write(row, header_map.get("User ID"), user_info["user_id"], row_format)
            sheet.write(row, header_map.get("Roles"), user_info["roles"], wrap_format)
            sheet.write(row, header_map.get("Joined Events Count"), user_info["joined_count"], num_format)
            sheet.write(row, header_map.get("Won Events Count"), user_info["won_count"], num_format)
            sheet.write(row, header_map.get("Total Messages Sent"), user_info["total_message_count"], num_format)
            sheet.write(row, header_map.get("Tweet Count"), user_info["tweet_count"], num_format) # Write Tweet Count
            for event_name in sorted_event_list:
                 event_col = header_map.get(event_name, -1)
                 if event_col != -1: status = "🏆" if event_name in data.get("winners", []) else ("✅" if event_name in data.get("events", []) else ""); sheet.write(row, event_col, status, row_format)
            tw_col = header_map.get("Posted Twitter Links", -1)
            if tw_col != -1: sheet.write_string(row, tw_col, "\n".join(user_info["twitter_links_list"]), url_format) # Write links as formatted string

        # Set Column Widths
        sheet.set_column(header_map.get("User Name"), header_map.get("User Name"), 25); sheet.set_column(header_map.get("User ID"), header_map.get("User ID"), 20)
        sheet.set_column(header_map.get("Roles"), header_map.get("Roles"), 35); sheet.set_column(header_map.get("Joined Events Count"), header_map.get("Won Events Count"), 18)
        sheet.set_column(header_map.get("Total Messages Sent"), header_map.get("Total Messages Sent"), 20); sheet.set_column(header_map.get("Tweet Count"), header_map.get("Tweet Count"), 15) # Tweet count width
        first_event_col = header_map.get(sorted_event_list[0], -1) if sorted_event_list else -1
        if first_event_col != -1: last_event_col = header_map.get(sorted_event_list[-1], first_event_col); sheet.set_column(first_event_col, last_event_col, 15)
        tw_col_idx = header_map.get("Posted Twitter Links",-1)
        if tw_col_idx != -1: sheet.set_column(tw_col_idx, tw_col_idx, 50)

        workbook.close(); print(f"Excel '{EXCEL_FILE_PATH}' generated ({guild.name}). {len(user_data_for_excel)} members processed.")
        return True
    except Exception as e: print(f"ERROR: Writing Excel: {e}"); traceback.print_exc(); return False

# --- COMMANDS ---

@bot.command(name="trackhelp", aliases=["thelp"])
@admin_only()
async def trackhelp(ctx):
    """Displays the list of administrator commands."""
    embed = discord.Embed( title="📘 Admin Command List", description="Available administrator commands:", color=discord.Color.teal())
    embed.add_field(name="!stats [@user or ID]", value="Shows statistics for the specified user (or yourself).", inline=False)
    embed.add_field( name="!allstats [filter] [sort_key]", value="Generates the Excel report.\n• `filter`: `msgcount>500`, `msgcount<=100` etc.\n• `sort_key`: `messages` or `tweets`.\n• Ex: `!allstats msgcount>1000 messages`", inline=False)
    embed.add_field(name="!listexcels", value="Lists available Excel (.xlsx) files.", inline=False)
    embed.add_field(name="!deleteexcel <filename.xlsx>", value="Deletes the specified Excel (.xlsx) file.", inline=False)
    embed.add_field(name="!twitterlog <channel_id>", value="Sets the channel for Twitter/X link monitoring (Scans history).", inline=False)
    embed.add_field(name="!removetwitterlog", value="Disables Twitter/X link monitoring.", inline=False)
    embed.add_field(name="--- Bulk Actions ---", value="\u200b", inline=False)
    embed.add_field(name="!bulkban <id1> [id2...] [reason=...]", value="Bans multiple users by ID.", inline=False)
    embed.add_field(name="!bulkgiverole <role_id> <id1> [id2...]", value="Gives the specified role to multiple users by ID.", inline=False)
    embed.add_field(name="--- Event Management ---", value="\u200b", inline=False)
    embed.add_field(name="!addevent <event_name> <id...>", value="Adds users to an event ('joined').", inline=False)
    embed.add_field(name="!eventwinners <event_name> <id...>", value="Marks users as winners.", inline=False)
    embed.add_field(name="!notjoined <event_name> <id...>", value="Removes users from an event.", inline=False)
    embed.add_field(name="!delevent <event_name>", value="Deletes an event from all records.", inline=False)
    embed.add_field(name="!copyevent <event_name> <channel_id>", value="Copies voice/stage members as 'joined'.", inline=False)
    embed.add_field(name="--- Correction Commands ---", value="\u200b", inline=False)
    embed.add_field(name="!fixwinners <mod> <event_name> <id...>", value="**Fix 'Winner' status** (`mod='joined'`/`'notjoined'`)", inline=False)
    embed.add_field(name="!fixjoined <mod> <event_name> <id...>", value="**Fix 'Joined' status** (`mod='winner'`/`'notjoined'`)", inline=False)
    embed.add_field(name="!fixnotjoined <mod> <event_name> <id...>", value="**Fix 'Not Joined' status** (`mod='joined'`/`'winner'`)", inline=False)
    embed.set_footer(text=f"Bot Prefix: {bot.command_prefix}")
    await ctx.send(embed=embed)

@bot.command(name="twitterlog")
@admin_only()
async def set_twitter_log_channel_new(ctx, channel_id: int):
    """Sets the channel by ID for monitoring Twitter/X links and scans history."""
    global twitter_log_channel_id; global posted_links_set
    channel = ctx.guild.get_channel(channel_id)
    if not channel or not isinstance(channel, discord.TextChannel): await ctx.send(f"❌ Error: Valid text channel with ID `{channel_id}` not found."); return
    original_channel_id = twitter_log_channel_id; twitter_log_channel_id = channel.id
    await ctx.send(f"✅ Twitter log channel set to {channel.mention}.\n⏳ Now scanning channel history (max 10k messages)... This may take a moment.")
    url_pattern = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[A-Za-z0-9_]+/status/[0-9]+")
    history_limit = 10000; added = 0; processed = 0; changed = False
    try:
        async for message in channel.history(limit=history_limit):
            processed += 1;
            if message.author.bot or not message.content: continue
            content = message.content.strip(); match = url_pattern.search(content)
            if match:
                extracted_url = match.group(0)
                if content == extracted_url:
                    norm_url = extracted_url
                    if norm_url.startswith("http://"): norm_url = norm_url.replace("http://", "https://", 1)
                    if "://twitter.com/" in norm_url: norm_url = norm_url.replace("://twitter.com/", "://x.com/", 1)
                    if "://www.x.com/" in norm_url: norm_url = norm_url.replace("://www.x.com/", "://x.com/", 1)
                    if norm_url not in posted_links_set:
                        posted_links_set.add(norm_url); uid = str(message.author.id)
                        udata = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE())
                        udata.setdefault("twitter_links", []).append(norm_url); added += 1; changed = True
    except discord.Forbidden: await ctx.send("❌ Error: Missing permission to read channel history."); twitter_log_channel_id = original_channel_id; return
    except Exception as e: await ctx.send(f"❌ Error during history scan: {e}"); print(f"Error (twitterlog history scan): {e}")
    if changed or original_channel_id != twitter_log_channel_id: save_stats()
    await ctx.send(f"✅ History scan complete. Added {added} new unique links. Channel is active!")

@set_twitter_log_channel_new.error
async def twitterlog_error(ctx, error):
    if isinstance(error, commands.BadArgument): await ctx.send("❌ Error: Please provide a valid Channel ID (numbers only).")
    elif isinstance(error, MissingRequiredArgument): await ctx.send("❌ Error: Channel ID is required. Usage: `!twitterlog <channel_id>`")
    elif isinstance(error, CheckFailure): pass
    else: await ctx.send(f"Unexpected error: {error}"); print(f"Error (twitterlog): {error}")

@bot.command(name="removetwitterlog")
@admin_only()
async def remove_twitter_log_channel(ctx):
    """Disables the Twitter/X link monitoring feature."""
    global twitter_log_channel_id
    if twitter_log_channel_id is None: await ctx.send("ℹ️ Twitter log feature is already disabled."); return
    twitter_log_channel_id = None; save_stats(); await ctx.send("✅ Twitter log feature disabled.")

@bot.command(name="allstats")
@admin_only()
async def allstats(ctx, *args):
    """Generates the Excel statistics file (optional filter/sort)."""
    sort_param = None; filter_details = None; filter_str_arg = None
    if args:
        pFilt = args[0]; opMatch = re.search(r"(?:>=|<=|!=|>|<|=)", pFilt)
        if opMatch and "msgcount" in pFilt.lower(): filter_str_arg = pFilt
        pSortIdx = 1 if filter_str_arg else 0
        if len(args) > pSortIdx and args[pSortIdx].lower() in ["messages","tweets"]: sort_param = args[pSortIdx].lower()
    if filter_str_arg: filter_details = parse_filter(filter_str_arg)
    if filter_str_arg and filter_details is None: await ctx.send(f"⚠️ Invalid filter format: `{filter_str_arg}`. Filter not applied."); filter_str_arg = None
    async with ctx.typing():
        msg = f"⏳ Generating Excel report...{' Filter: `' + filter_str_arg + '`' if filter_details else ''}{' Sort: `' + sort_param + '`' if sort_param else ''}"
        await ctx.send(msg)
        if generate_excel(ctx.guild, sort_key=sort_param, filter_details=filter_details):
            if os.path.exists(EXCEL_FILE_PATH):
                try:
                    fs=os.path.getsize(EXCEL_FILE_PATH); lim=25*1024*1024 # 25MB Limit
                    if fs > lim: await ctx.send(f"⚠️ Excel file (`{os.path.basename(EXCEL_FILE_PATH)}`, {fs/1024/1024:.2f}MB) exceeds Discord's upload limit.")
                    else: await ctx.send(file=discord.File(EXCEL_FILE_PATH))
                except Exception as e: await ctx.send(f"❌ Error sending file: {e}.")
            else: await ctx.send("❌ Error: Excel file not found after generation.")
        else: await ctx.send("❌ Failed to generate Excel report. Check bot logs.")

@bot.command(name="stats")
@admin_only()
async def stats(ctx, user_input: str = None):
    """Shows statistics for the specified user (@mention, name, or ID)."""
    target_member = None; target_user_obj = None; user_id = None
    if user_input is None: target_member = ctx.author; user_id = str(target_member.id); target_user_obj = target_member
    else:
        try: target_member = await commands.MemberConverter().convert(ctx, user_input); user_id = str(target_member.id); target_user_obj = target_member
        except MemberNotFound:
            if user_input.isdigit():
                user_id = user_input
                try: target_user_obj = await bot.fetch_user(int(user_id)); target_member = ctx.guild.get_member(int(user_id)) # May be None
                except discord.NotFound: await ctx.send(f"❌ User with ID `{user_id}` not found on Discord."); return
                except ValueError: await ctx.send(f"❌ Invalid ID format: `{user_input}`."); return
            else: await ctx.send(f"❌ User not found: `{user_input}`. Use a mention, name, or ID."); return
    data = stats_data.get(user_id)
    if not data or not isinstance(data, dict): display_name = target_user_obj.name if target_user_obj else user_id; await ctx.send(f"No statistics data found for {display_name}."); return
    events_j=data.get("events",[]); events_w=data.get("winners",[]); tw_links=data.get("twitter_links",[]); total_msg=data.get("total_message_count", 0)
    display_name = target_member.display_name if target_member else target_user_obj.name; avatar_url = target_member.display_avatar.url if target_member else target_user_obj.display_avatar.url
    embed = discord.Embed(title=f"Stats for {display_name}", color=discord.Color.blue()); embed.set_thumbnail(url=avatar_url)
    embed.add_field(name="Joined", value=len(events_j), inline=True); embed.add_field(name="Won", value=len(events_w), inline=True); embed.add_field(name="Total Msgs", value=f"{total_msg:,}", inline=True)
    embed.add_field(name="Tweet Links", value=len(tw_links), inline=True); embed.add_field(name="\u200b", value="\u200b", inline=True); embed.add_field(name="\u200b", value="\u200b", inline=True) # Spacing
    if events_j: embed.add_field(name="Joined Events", value=", ".join(sorted(events_j)) or "None", inline=False)
    if events_w: embed.add_field(name="Won Events", value=", ".join(sorted(events_w)) or "None", inline=False)
    if tw_links:
         links_d = "\n".join(tw_links); max_l=1000
         if len(links_d) > max_l: links_d = "\n".join(tw_links[:10]) + f"\n... and {len(tw_links)-10} more."
         embed.add_field(name="Posted Twitter Links", value=links_d or "None", inline=False)
    if target_member: roles = [r.mention for r in target_member.roles if r.name != "@everyone"];
    if roles: embed.add_field(name="Roles", value=" ".join(roles), inline=False)
    await ctx.send(embed=embed)

# --- Event Management Commands (Corrected and Translated) ---
@bot.command(name="addevent")
@admin_only()
async def addevent(ctx, event_name: str, *user_ids: str):
    """Adds users (by ID) to an event (marks as 'joined')."""
    if not user_ids: await ctx.send("❌ Error: Provide at least one user ID."); return
    added = 0; already = 0; changed = False; processed_ids = 0
    for uid in user_ids:
        if not uid.isdigit(): await ctx.send(f"⚠️ Warning: '{uid}' is not a valid ID. Skipping."); continue
        processed_ids += 1
        udata = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE())
        if event_name not in udata.get("events", []):
            if event_name in udata.get("winners", []): udata["winners"].remove(event_name); changed = True
            udata.setdefault("events", []).append(event_name); added += 1; changed = True
        else: already += 1
    msg = ""
    if added > 0:
        msg = f"✅ Added {added} users to the '{event_name}' event."
        if already > 0: msg += f" ({already} users were already joined)."
        save_stats()
    elif already > 0: msg = f"ℹ️ All specified valid users ({already}) were already joined in '{event_name}'."
    elif processed_ids > 0: msg = f"ℹ️ No action taken for '{event_name}'. Users might already be joined or invalid IDs provided."
    else: msg = "❌ No valid user IDs were processed."
    await ctx.send(msg)

@bot.command(name="eventwinners")
@admin_only()
async def eventwinners(ctx, event_name: str, *user_ids: str):
    """Marks specified users (by ID) as winners for an event."""
    if not user_ids: await ctx.send("Error: Provide at least one user ID."); return
    upd = 0; already = 0; changed = False; processed_ids = 0
    for uid in user_ids:
        if not uid.isdigit(): await ctx.send(f"Warning: '{uid}' invalid ID."); continue
        processed_ids += 1
        udata = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE())
        if event_name in udata.get("events",[]): udata["events"].remove(event_name); changed=True
        if event_name not in udata.get("winners",[]): udata.setdefault("winners",[]).append(event_name); upd+=1; changed=True
        else: already += 1
    msg = ""
    if upd > 0: msg = f"🏆 Marked {upd} users as winners for '{event_name}'."; # changed is implicitly True if upd > 0
    if already > 0: msg += f" ({already} were already winners)." if msg else f"ℹ️ {already} specified users were already winners for '{event_name}'."
    if not msg and processed_ids > 0 : msg = f"ℹ️ No users updated for '{event_name}'. They might already be winners or invalid IDs provided."
    elif not msg and processed_ids == 0: msg = "❌ No valid user IDs processed."
    if changed: save_stats()
    if msg: await ctx.send(msg)

@bot.command(name="notjoined")
@admin_only()
async def notjoined(ctx, event_name: str, *user_ids: str):
    """Removes users (by ID) completely from an event."""
    if not user_ids: await ctx.send("Error: Provide at least one user ID."); return
    rem = 0; nf = 0; changed = False; processed_ids = 0
    for uid in user_ids:
        if not uid.isdigit(): await ctx.send(f"Warning: '{uid}' invalid ID."); continue
        processed_ids += 1
        udata = stats_data.get(uid)
        if udata:
            found = False
            if event_name in udata.get("events", []): udata["events"].remove(event_name); found=True; changed=True
            if event_name in udata.get("winners", []): udata["winners"].remove(event_name); found=True; changed=True
            if found: rem+=1
            else: nf+=1
        else: nf+=1
    msg = ""
    if rem > 0: msg = f"🗑️ Removed {rem} users from the '{event_name}' event."; save_stats()
    if nf > 0: msg += f" ({nf} users were not found in the event)." if msg else f"ℹ️ {nf} specified users were not found in '{event_name}' records."
    if not msg and processed_ids > 0: msg = f"ℹ️ No users found to remove for '{event_name}' among the specified IDs."
    elif not msg and processed_ids == 0: msg = "❌ No valid user IDs processed."
    await ctx.send(msg)

@bot.command(name="delevent")
@admin_only()
async def delevent(ctx, event_name: str):
    """Deletes an event entirely from all user records."""
    deleted_entries = 0; affected_users = 0; changed = False
    for uid in list(stats_data.keys()):
         if not uid.isdigit() or not isinstance(stats_data[uid], dict): continue
         user_data = stats_data[uid]; removed_from_this_user = False
         if event_name in user_data.get("events", []): user_data["events"].remove(event_name); deleted_entries += 1; removed_from_this_user = True; changed = True
         if event_name in user_data.get("winners", []): user_data["winners"].remove(event_name);
         if not removed_from_this_user: deleted_entries += 1; removed_from_this_user = True; changed = True
         if removed_from_this_user: affected_users += 1
    if changed: save_stats(); await ctx.send(f"🗑️ Event '{event_name}' deleted from {affected_users} users ({deleted_entries} entries).")
    else: await ctx.send(f"Event '{event_name}' not found in any user records.")

# CORRECTED copyevent
@bot.command(name="copyevent")
@admin_only()
async def copyevent(ctx, event_name: str, channel: discord.VoiceChannel | discord.StageChannel):
    """Copies members from a voice/stage channel as 'joined' for an event."""
    count = 0
    already = 0
    changed = False
    processed_members = 0 # Keep track of non-bot members processed
    members_in_channel = channel.members

    if not members_in_channel:
        await ctx.send(f"No members found in the channel '{channel.name}'.")
        return

    for member in members_in_channel:
        if member.bot:
            continue # Skip bots

        processed_members += 1
        uid = str(member.id)
        # Define 'ud' safely using setdefault
        ud = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE()) # Use consistent variable name

        # Check if user can be added
        if event_name not in ud.get("events", []) and event_name not in ud.get("winners", []):
            ud.setdefault("events", []).append(event_name)
            count += 1
            changed = True
        else:
            already += 1

    # Construct feedback message reliably
    msg = ""
    if count > 0:
        msg = f"➕ Added {count} members from '{channel.name}' to '{event_name}' event."
        if already > 0:
            msg += f" ({already} members were already present or winners)."
        save_stats() # Save only if new members were added
    elif already > 0:
        msg = f"ℹ️ All non-bot members ({already}) from '{channel.name}' were already present or winners in '{event_name}'."
    elif processed_members > 0: # Processed non-bots but none added/already present
        msg = f"ℹ️ No eligible members found or added from '{channel.name}' for '{event_name}'."
    else: # Only bots in channel?
        msg = f"ℹ️ No non-bot members found in '{channel.name}' to process for '{event_name}'."

    await ctx.send(msg)

@copyevent.error
async def copyevent_error(ctx, error):
     """Error handler for !copyevent."""
     if isinstance(error, commands.ChannelNotFound): await ctx.send(f"❌ Error: Voice/Stage channel '{error.argument}' not found.")
     elif isinstance(error, commands.BadArgument): await ctx.send(f"❌ Error: Invalid Voice/Stage channel provided: '{error.argument}'. Must be a Voice or Stage channel.")
     elif isinstance(error, MissingRequiredArgument): await ctx.send(f"❌ Error: Event name and channel ID/name required.")
     elif isinstance(error, CheckFailure): pass # Let global handler manage permission errors
     else: await ctx.send(f"Error: {error}"); print(f"Error (copyevent): {error}")

# --- Correction Commands (Corrected and Translated) ---
@bot.command(name="fixwinners")
@admin_only()
async def fixwinners(ctx, mode: str.lower, event_name: str, *user_ids: str):
    """Fixes users incorrectly marked as winners."""
    if mode not in ["joined","notjoined"]: await ctx.send("Error: Mode must be 'joined' or 'notjoined'."); return
    if not user_ids: await ctx.send("Error: Provide at least one user ID."); return
    fix = 0; ch = False; processed_ids = 0 # Initialize counter
    for uid in user_ids:
        if not uid.isdigit(): await ctx.send(f"Warning: '{uid}' invalid ID. Skipping."); continue
        processed_ids += 1 # Increment counter
        ud=stats_data.get(uid)
        user_exists = ud is not None
        if user_exists and event_name in ud.get("winners",[]): ud["winners"].remove(event_name); ch=True; fix+=1
        ud = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE()) # Ensure exists for adding 'joined'
        if mode=="joined":
            if event_name not in ud.get("events",[]): ud.setdefault("events",[]).append(event_name); ch=True
    msg = ""
    if fix > 0: msg = f"🔧 Fixed {fix} users' winner status for '{event_name}' using mode '{mode}'."; save_stats()
    elif processed_ids > 0: msg = f"ℹ️ No users found marked as winners for '{event_name}' among the valid processed IDs ({processed_ids}), or no changes needed for mode '{mode}'."
    else: msg = "❌ No valid user IDs processed."
    await ctx.send(msg)

# CORRECTED fixjoined
@bot.command(name="fixjoined")
@admin_only()
async def fixjoined(ctx, mode: str.lower, event_name: str, *user_ids: str):
    """Fixes users incorrectly marked as joined."""
    if mode not in ["winner","notjoined"]: await ctx.send("Error: Mode must be 'winner' or 'notjoined'."); return
    if not user_ids: await ctx.send("Error: Provide at least one user ID."); return
    fix = 0; ch = False; processed_ids = 0 # Initialize counter
    for uid in user_ids:
        if not uid.isdigit(): await ctx.send(f"Warning: '{uid}' invalid ID. Skipping."); continue
        processed_ids += 1 # Increment counter
        ud=stats_data.get(uid)
        if ud and event_name in ud.get("events",[]):
            ud["events"].remove(event_name); ch=True
            ud = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE()) # Ensure user entry exists
            if mode=="winner":
                if event_name not in ud.get("winners",[]): ud.setdefault("winners",[]).append(event_name)
            elif mode=="notjoined":
                 if event_name in ud.get("winners",[]): ud["winners"].remove(event_name)
            fix+=1
    msg = ""
    if fix > 0: msg = f"🔧 Fixed {fix} users' joined status for '{event_name}' using mode '{mode}'."; save_stats()
    elif processed_ids > 0: msg = f"ℹ️ No users found marked as joined for '{event_name}' among the valid processed IDs ({processed_ids}), or no changes needed for mode '{mode}'."
    else: msg = "❌ No valid user IDs processed."
    await ctx.send(msg)

# CORRECTED fixnotjoined
@bot.command(name="fixnotjoined")
@admin_only()
async def fixnotjoined(ctx, mode: str.lower, event_name: str, *user_ids: str):
    """Fixes users incorrectly marked as not joined (or never added)."""
    if mode not in ["joined","winner"]: await ctx.send("Error: Mode must be 'joined' or 'winner'."); return
    if not user_ids: await ctx.send("Error: Provide at least one user ID."); return
    fix = 0; ch = False; processed_ids = 0 # Initialize counter
    for uid in user_ids:
        if not uid.isdigit(): await ctx.send(f"Warning: '{uid}' invalid ID. Skipping."); continue
        processed_ids += 1 # Increment counter
        ud=stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE()); act=False
        if mode=="joined":
            if event_name in ud.get("winners",[]): ud["winners"].remove(event_name); act=True
            if event_name not in ud.get("events",[]): ud.setdefault("events",[]).append(event_name); act=True
        elif mode=="winner":
            if event_name in ud.get("events",[]): ud["events"].remove(event_name); act=True
            if event_name not in ud.get("winners",[]): ud.setdefault("winners",[]).append(event_name); act=True
        if act: fix+=1; ch=True
    msg = ""
    if fix > 0: msg = f"🔧 Fixed/Set {fix} users' status for '{event_name}' using mode '{mode}'."; save_stats()
    elif processed_ids > 0: msg = f"ℹ️ No changes made for '{event_name}' with mode '{mode}' for the processed IDs ({processed_ids}) (users might already have the status)."
    else: msg = "❌ No valid user IDs processed."
    await ctx.send(msg)

# Excel File Management Commands (Translated)
@bot.command(name="listexcels")
@admin_only()
async def list_excels(ctx):
    """Lists available Excel (.xlsx) files in the bot's directory."""
    try:
        fs = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith(".xlsx")]
        if not fs: await ctx.send("No Excel (.xlsx) files found."); return
        emb = discord.Embed( title="📄 Available Excel Files", description=f"Found files:\n```\n" + "\n".join(fs) + "\n```", color=discord.Color.blue())
        await ctx.send(embed=emb)
    except Exception as e: await ctx.send(f"Error listing files: {e}"); print(f"Error (listexcels): {e}")

@bot.command(name="deleteexcel")
@admin_only()
async def delete_excel(ctx, filename: str):
    """Deletes the specified Excel (.xlsx) file."""
    if not filename.endswith(".xlsx"): await ctx.send("Error: Filename must end with `.xlsx`."); return
    if "/" in filename or "\\" in filename or ".." in filename: await ctx.send("Error: Invalid filename."); return
    if filename.lower() in [STATS_FILE_PATH.lower(), os.path.basename(__file__).lower()]: await ctx.send(f"Error: Deleting `{filename}` is not permitted."); return
    fp=os.path.join('.', filename)
    try:
        if os.path.exists(fp) and os.path.isfile(fp): os.remove(fp); await ctx.send(f"✅ Deleted: `{filename}`"); print(f"Admin {ctx.author} deleted: {filename}")
        else: await ctx.send(f"❌ Error: File `{filename}` not found.")
    except OSError as e: await ctx.send(f"❌ OS Error deleting file: {e}"); print(f"OS Error (delete_excel - {filename}): {e}")
    except Exception as e: await ctx.send(f"❌ Error deleting file: {e}"); print(f"Error (delete_excel - {filename}): {e}")

@delete_excel.error
async def delete_excel_error(ctx, error):
    """Error handler for !deleteexcel."""
    if isinstance(error, MissingRequiredArgument): await ctx.send("❌ Error: Specify filename. Usage: `!deleteexcel <filename.xlsx>`")
    elif isinstance(error, CheckFailure): pass
    else: await ctx.send(f"Error in delete command: {error}"); print(f"Unhandled Error (delete_excel): {error}")

# Bulk Action Commands (Corrected and Translated)
@bot.command(name="bulkban")
@commands.has_permissions(ban_members=True)
@admin_only()
async def bulk_ban(ctx, *args):
    """Bans multiple users specified by their IDs."""
    if not ctx.guild.me.guild_permissions.ban_members: await ctx.send("❌ Bot Error: Missing 'Ban Members' permission."); return
    if not args: await ctx.send("❌ Usage: `!bulkban <id1> [id2...] [reason=Optional reason]`"); return
    ids=[]; r_parts=[]; p_reason=False; part = "" # Initialize part safely
    for arg in args:
        if arg.lower().startswith("reason="): p_reason=True; part=arg[len("reason="):];
        # Check part has content before appending
        if part: r_parts.append(part); part = "" # Reset part after use
        elif p_reason: r_parts.append(arg)
        else:
            if arg.isdigit():
                try: ids.append(int(arg))
                except ValueError: await ctx.send(f"⚠️ Invalid or too large ID: `{arg}`. Skipping.")
            else: await ctx.send(f"⚠️ Invalid ID format: `{arg}`. Skipping.")
    if not ids: await ctx.send("❌ No valid user IDs provided to ban."); return
    reason=" ".join(r_parts).strip() if r_parts else f"Bulk ban by {ctx.author}"
    banned=0; failed=[]
    await ctx.send(f"⏳ Starting bulk ban for {len(ids)} IDs. Reason: {reason}")
    for user_id in ids:
        print(f"[BulkBan] Attempting ban: ID {user_id}")
        try: user=discord.Object(id=user_id); await ctx.guild.ban(user, reason=reason, delete_message_days=0); banned+=1; print(f"  -> Success: {user_id}")
        except discord.NotFound: failed.append((user_id, "Not Found")); print(f"  -> Fail: NotFound")
        except discord.Forbidden: failed.append((user_id, "Permission/Hierarchy")); print(f"  -> Fail: Forbidden")
        except discord.HTTPException as e: failed.append((user_id, f"HTTP {e.status}")); print(f"  -> Fail: HTTP {e.status} - {e.text}")
        except Exception as e: failed.append((user_id, f"Error: {type(e).__name__}")); print(f"!!! -> Fail: Unexpected Error ID {user_id}: {e}"); traceback.print_exc()
        await asyncio.sleep(0.5)
    summary = f"✅ Successfully banned {banned} users.";
    if failed:
        summary += f"\n❌ Failed to ban {len(failed)} users:"
        f_det = [f"  - ID `{uid}`: {err}" for uid, err in failed[:15]]
        summary += "\n" + "\n".join(f_det)
        if len(failed) > 15: summary += "\n... (list truncated)"
    await ctx.send(summary)

@bot.command(name="bulkgiverole")
@commands.has_permissions(manage_roles=True)
@admin_only()
async def bulk_give_role(ctx, *args):
    """Gives a specified role (by ID) to multiple users (by ID)."""
    if not ctx.guild.me.guild_permissions.manage_roles: await ctx.send("❌ Bot Error: Missing 'Manage Roles' permission."); return
    if len(args) < 2: await ctx.send("❌ Usage: `!bulkgiverole <role_id> <id1> [id2...]`"); return
    try: rid=int(args[0])
    except ValueError: await ctx.send(f"❌ Invalid Role ID format: `{args[0]}`."); return
    uids=[];
    for arg in args[1:]:
        if arg.isdigit():
            try: uids.append(int(arg))
            except ValueError: await ctx.send(f"⚠️ Invalid or too large User ID: `{arg}`. Skipping.")
        else: await ctx.send(f"⚠️ Invalid User ID format: `{arg}`. Skipping.")
    if not uids: await ctx.send("❌ No valid user IDs provided."); return
    role=ctx.guild.get_role(rid)
    if not role: await ctx.send(f"❌ Role with ID `{rid}` not found."); return
    if ctx.guild.me.top_role <= role: await ctx.send(f"❌ Bot cannot manage role '{role.name}' (Hierarchy issue)."); return
    if role.is_integration() or role.is_bot_managed() or role.is_premium_subscriber() or role.is_default(): await ctx.send(f"❌ Cannot assign special roles like '{role.name}'."); return
    succ=0; fail=[]; reason=f"Bulk role add: '{role.name}' by {ctx.author}"
    await ctx.send(f"⏳ Attempting to give role '{role.name}' to {len(uids)} users...")
    for uid in uids:
        member = ctx.guild.get_member(uid)
        if not member: fail.append((uid, "Member not found")); continue
        if role in member.roles: continue
        try: await member.add_roles(role, reason=reason); succ+=1
        except discord.Forbidden: fail.append((uid, "Permission/Hierarchy"))
        except discord.HTTPException as e: fail.append((uid, f"HTTP Error {e.status}"))
        except Exception as e: fail.append((uid, f"Error: {type(e).__name__}")); print(f"!!! Bulkgiverole Error ID {uid}: {e}"); traceback.print_exc()
        await asyncio.sleep(0.3)
    summary = f"✅ Successfully gave role '{role.name}' to {succ} users.";
    if fail:
        summary += f"\n❌ Failed to give role to {len(fail)} users:"
        f_det = [f"  - ID `{uid}`: {err}" for uid, err in fail[:15]]
        summary += "\n" + "\n".join(f_det)
        if len(fail) > 15: summary += "\n... (list truncated)"
    await ctx.send(summary)

# --- EVENTS ---

@bot.event
async def on_message(message):
    """Handles messages, increments total message counter, checks twitter log, processes commands."""
    if message.author.bot or message.guild is None: return
    user_id = str(message.author.id)
    user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
    user_data["total_message_count"] = user_data.get("total_message_count", 0) + 1
    global twitter_log_channel_id; global posted_links_set; valid_channel = False
    try:
        if twitter_log_channel_id and message.channel.id == twitter_log_channel_id: valid_channel = True
    except AttributeError: pass
    if valid_channel:
        url_pattern = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[A-Za-z0-9_]+/status/[0-9]+")
        content = message.content.strip(); match = url_pattern.search(content)
        delete_msg = True; reason = "Unknown issue."
        if match:
            extracted_url = match.group(0); normalized_url = extracted_url
            if normalized_url.startswith("http://"): normalized_url = normalized_url.replace("http://", "https://", 1)
            if "://twitter.com/" in normalized_url: normalized_url = normalized_url.replace("://twitter.com/", "://x.com/", 1)
            if "://www.x.com/" in normalized_url: normalized_url = normalized_url.replace("://www.x.com/", "://x.com/", 1)
            if content == match.group(0): # Check exact match
                if normalized_url not in posted_links_set:
                    delete_msg = False; posted_links_set.add(normalized_url)
                    stats_data["posted_twitter_links"] = sorted(list(posted_links_set))
                    user_data.setdefault("twitter_links", []).append(normalized_url)
                    save_stats() # Save immediately on link add
                    try: await message.add_reaction("✅")
                    except: pass
                else: reason = "duplicate link"
            else: reason = "extra text"
        else: reason = "invalid link format"
        if delete_msg:
            try: await message.delete();
            except Exception as e: print(f"Message delete error: {e}")
    await bot.process_commands(message)

@bot.event
async def on_command_error(ctx, error):
    """Global error handler."""
    if isinstance(error, commands.CommandInvokeError):
        original = error.original; print(f"!!! Command Invoke Error in '{ctx.command}': {type(original).__name__} - {original}"); traceback.print_exception(type(original), original, original.__traceback__); await ctx.send(f"Error executing command: `{type(original).__name__}`. Check logs."); return
    if isinstance(error, (CheckFailure, commands.CommandNotFound)): return
    if isinstance(error, commands.UserInputError): await ctx.send(f"❌ Input error: {error}. Use `!trackhelp` for usage.", delete_after=30)
    elif isinstance(error, commands.CommandOnCooldown): await ctx.send(f"⏳ Command on cooldown. Try again in {error.retry_after:.1f} seconds.", delete_after=10)
    elif isinstance(error, commands.MaxConcurrencyReached): await ctx.send("🚧 Command busy. Please wait.", delete_after=10)
    elif isinstance(error, commands.MissingPermissions): await ctx.send(f"❌ You lack required permissions: `{', '.join(error.missing_permissions)}`")
    elif isinstance(error, commands.BotMissingPermissions): await ctx.send(f"❌ Bot lacks required permissions: `{', '.join(error.missing_permissions)}`")
    elif isinstance(error, CommandError): print(f"!! Command Error '{ctx.command}': {type(error).__name__} - {error}"); await ctx.send(f"Command error: {type(error).__name__}")
    else: print(f"!!! UNHANDLED ERROR '{ctx.command}': {type(error).__name__} - {error}"); traceback.print_exc(); await ctx.send("An unexpected error occurred.")

@bot.event
async def on_guild_join(guild):
    """When the bot joins a new guild."""
    print(f"Joined new guild: {guild.name} (ID: {guild.id})")
    print(f"  -> Scanning members for TARGET_ROLES...")
    changes_made = False; members_checked = 0; added_count = 0
    if not guild.chunked:
        try: await guild.chunk(cache=True); print(f"  -> {guild.name}: Guild chunked.")
        except Exception as e: print(f"  -> WARN: Could not chunk {guild.name}: {e}.")
    for member in guild.members:
        members_checked += 1
        if member.bot: continue
        if any(role.id in TARGET_ROLES for role in member.roles):
            user_id = str(member.id)
            if user_id not in stats_data or not isinstance(stats_data.get(user_id), dict):
                 stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE()); added_count += 1; changes_made = True
            elif "total_message_count" not in stats_data[user_id]: stats_data[user_id]["total_message_count"] = 0;
    print(f"  -> {guild.name}: Checked {members_checked} members, added {added_count} new users to stats.")
    if changes_made: save_stats(); print(f"  -> {guild.name}: Stats saved for new members.")

@bot.event
async def on_ready():
    """Event triggered when the bot is ready."""
    print(f'--- Bot Ready [{datetime.datetime.now(timezone.utc)}] ---')
    print(f'Logged in as: {bot.user.name} ({bot.user.id}) | Discord.py: {discord.__version__}')
    print(f'Twitter Log Channel ID: {twitter_log_channel_id if twitter_log_channel_id else "Not Set"} | {len(posted_links_set)} links loaded.')
    print('Checking members in current guilds...')
    initial_changes_made = False
    for guild in bot.guilds:
        print(f"  -> Checking: {guild.name} ({guild.id})")
        if not guild.chunked:
             try: await guild.chunk(cache=True)
             except Exception as e: print(f"  -> WARN: Could not chunk {guild.name}: {e}"); continue
        guild_added = 0
        for member in guild.members:
            if member.bot: continue
            if any(role.id in TARGET_ROLES for role in member.roles):
                user_id = str(member.id)
                if user_id not in stats_data or not isinstance(stats_data.get(user_id), dict):
                     stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE()); guild_added += 1; initial_changes_made = True
                # Ensure total_message_count exists even for older data entries
                elif "total_message_count" not in stats_data[user_id]: stats_data[user_id]["total_message_count"] = 0;
    if initial_changes_made: save_stats(); print("Stats saved after initial member check.")
    print(f'Starting background tasks...')
    if not update_members_loop.is_running(): update_members_loop.start()
    if not daily_save_task.is_running(): daily_save_task.start()
    print('------ Bot Active ------')

# --- BACKGROUND TASKS ---
@tasks.loop(hours=1)
async def update_members_loop():
    """Periodically checks roles and ensures users are in stats_data."""
    changes_made = False
    for guild in bot.guilds:
        if not guild.chunked:
             try: await guild.chunk(cache=True)
             except Exception as e: print(f"WARN: Hourly task chunking {guild.name} failed: {e}"); continue
        newly_added = 0; members_with_target = set()
        for role_id in TARGET_ROLES:
            role = guild.get_role(role_id)
            if role: members_with_target.update(role.members)
        for member in members_with_target:
            if member.bot: continue; uid = str(member.id)
            if uid not in stats_data or not isinstance(stats_data.get(uid), dict): stats_data[uid] = DEFAULT_USER_TEMPLATE(); newly_added += 1; changes_made = True
            elif "total_message_count" not in stats_data[uid]: stats_data[uid]["total_message_count"]=0;
    # No save needed here.

@tasks.loop(time=time(hour=0, minute=10, tzinfo=timezone.utc)) # Daily at 00:10 UTC
async def daily_save_task():
    """Saves stats_data (including accumulated total message counts) once daily."""
    print(f"[{datetime.datetime.now(timezone.utc)}] Running daily save task...")
    try: save_stats(); print(f"[{datetime.datetime.now(timezone.utc)}] Stats saved by daily task.")
    except Exception as e: print(f"ERROR: Daily save task failed: {e}"); traceback.print_exc()

# --- RUN THE BOT ---
bot_token = "YOUR_BOT_TOKEN" # !! PASTE YOUR BOT TOKEN HERE !!

if __name__ == "__main__":
    if bot_token == "YOUR_BOT_TOKEN" or not bot_token:
        print("\n" * 3 + "!" * 60 + "\n!!! ERROR: BOT TOKEN MISSING! Please edit the script. !!!\n" + "!" * 60 + "\n" * 3)
    else:
        try:
            print("Bot connecting to Discord...")
            async def main():
                 # Optional: Enable more detailed logging from discord.py
                 # discord.utils.setup_logging(level=logging.INFO, root=False)
                 async with bot: await bot.start(bot_token)
            asyncio.run(main())
        except discord.LoginFailure: print("ERROR: Invalid bot token.")
        except discord.PrivilegedIntentsRequired: print("ERROR: Required Intents (e.g., Server Members) not enabled in Developer Portal.")
        except Exception as e:
            print(f"BOT RUNTIME ERROR: {type(e).__name__} - {e}")
            traceback.print_exc()
