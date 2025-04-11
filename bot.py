import discord
from discord.ext import commands, tasks
from discord.ext.commands import CheckFailure, MissingRequiredArgument, BadArgument, CommandError, MemberNotFound, RoleNotFound
import json
import os
import xlsxwriter
import datetime
import re
import asyncio  # Bulk operations
from datetime import time, date, timedelta, timezone  # Time-related utilities
import traceback  # Error logging
import string  # Filename sanitization

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
intents.members = True           # Enable in Developer Portal
intents.voice_states = True
class SilentBot(commands.Bot):
    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure) or isinstance(error, commands.MissingPermissions):
            try:
                await ctx.message.delete()
            except (discord.Forbidden, discord.HTTPException):
                pass
            return
        # Diğer hataları normal şekilde işle
        await super().on_command_error(ctx, error)

bot = SilentBot(command_prefix="!", intents=intents, case_insensitive=True)

# --- CONFIGURATION ---
# !! UPDATE THESE WITH YOUR ACTUAL ROLE IDS IF NEEDED !!
AUTHORIZED_ROLES = [111111111111111111, 222222222222222222]  # Track komutlarını kullanabilecek yetkili roller
TARGET_ROLES = [333333333333333333, 444444444444444444]      # İstatistik için "hedef" roller (sadece bu role sahip üyeler takip edilir)
EXCEL_FILE_PATH = "server_statistics.xlsx"
STATS_FILE_PATH = "stats.json"

# --- DATA LOADING & GLOBAL VARS ---
if os.path.exists(STATS_FILE_PATH):
    try:
        with open(STATS_FILE_PATH, "r", encoding='utf-8') as f:
            stats_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"ERROR: Failed to load {STATS_FILE_PATH}: {e}. Starting with empty stats.")
        stats_data = {}
else:
    stats_data = {}

config_data = stats_data.setdefault("config", {})
twitter_log_channel_id = config_data.get("twitter_log_channel_id", None)
posted_links_list = stats_data.setdefault("posted_twitter_links", [])
posted_links_set = set(posted_links_list)

# Rol ayarlarını config'e alıyoruz:
if "track_authorized_roles" not in config_data:
    config_data["track_authorized_roles"] = AUTHORIZED_ROLES
else:
    AUTHORIZED_ROLES = config_data["track_authorized_roles"]

if "track_target_roles" not in config_data:
    config_data["track_target_roles"] = TARGET_ROLES
else:
    TARGET_ROLES = config_data["track_target_roles"]

if "stats_authorized_roles" not in config_data:
    config_data["stats_authorized_roles"] = []
STATS_AUTHORIZED_ROLES = config_data["stats_authorized_roles"]

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
        if ctx.guild is None:
            return False
        if await bot.is_owner(ctx.author):
            return True
        if any(role.id in AUTHORIZED_ROLES for role in ctx.author.roles):
            return True
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass
        return False
    return commands.check(predicate)

async def stats_authorized_check(ctx):
    """
    Check if the user is authorized to use the !stats command.
    Kullanıcı; bot sahibi veya track yetkili rolüne sahipse yetkilidir.
    Eğer STATS_AUTHORIZED_ROLES listesi dolu ise, bu listedeki rollerden birine sahip kullanıcılar da yetkilidir.
    """
    if await bot.is_owner(ctx.author):
        return True
    if any(role.id in AUTHORIZED_ROLES for role in ctx.author.roles):
        return True
    # Eğer stats rol listesi dolu ise ve kullanıcı bu rollerden birine sahipse yetkilidir
    if STATS_AUTHORIZED_ROLES:
        for role in ctx.author.roles:
            if role.id in STATS_AUTHORIZED_ROLES:
                return True
    return False

def save_stats():
    """Saves the current stats_data dictionary to a JSON file."""
    global twitter_log_channel_id
    try:
        config_data["twitter_log_channel_id"] = twitter_log_channel_id
        config_data["track_authorized_roles"] = AUTHORIZED_ROLES
        config_data["track_target_roles"] = TARGET_ROLES
        config_data["stats_authorized_roles"] = STATS_AUTHORIZED_ROLES
        stats_data["posted_twitter_links"] = sorted(list(posted_links_set))
        with open(STATS_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(stats_data, f, indent=2)
    except IOError as e:
        print(f"ERROR: Could not save stats: {e}")
    except Exception as e:
        print(f"UNEXPECTED ERROR (save_stats): {e}")
        traceback.print_exc()

def standardize_event_name(name):
    """Trim and convert event name to lowercase."""
    return name.strip().lower()

def sanitize_filename(name):
    """Removes/replaces invalid filename characters."""
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in name if c in valid_chars)
    filename = filename.replace(' ', '_')
    if not filename:
        filename = "event_list"
    return filename[:100]

def chunk_text_by_size(text, size=1024):
    """Splits a text into chunks of up to 'size' characters."""
    lines = text.splitlines()
    chunks = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 <= size:
            current += line + "\n"
        else:
            chunks.append(current)
            current = line + "\n"
    if current:
        chunks.append(current)
    return chunks

# --- FILTER PARSING ---
def parse_numeric_filter(filter_str):
    """
    Parses filters for keywords: msgcount, twtcount, joined, won.
    Examples:
       "msgcount>100" -> ("total_message_count", ">", 100)
       "twtcount>=10" -> ("tweet_count", ">=", 10)
       "joined>0"     -> ("joined", ">", 0)
       "won>=1"       -> ("won", ">=", 1)
    """
    pattern = re.compile(r"(?i)^(msgcount|twtcount|joined|won)\s*(!=|>=|<=|>|<|=)\s*(\d+)$")
    m = pattern.match(filter_str.strip())
    if not m:
        return None
    keyword, operator, value_str = m.groups()
    try:
        value = int(value_str)
        if keyword.lower() == "msgcount":
            field = "total_message_count"
        elif keyword.lower() == "twtcount":
            field = "tweet_count"
        else:
            field = keyword.lower()  # "joined" or "won"
        return (field, operator, value)
    except ValueError:
        return None

# --- generate_excel with Extended Filtering ---
def generate_excel(guild, filters=None, sort_key=None):
    """
    Generates an Excel report with filters.
    filters is a dict containing:
       "numeric_filters": list of (field, op, value)
       "role_filter": Discord Role (user must have)
       "nothaverole": Discord Role (user must NOT have)
    sort_key: "messages" or "tweets"
    """
    if os.path.exists(EXCEL_FILE_PATH):
        try:
            os.remove(EXCEL_FILE_PATH)
        except OSError as e:
            print(f"Could not delete existing Excel: {e}")

    numeric_filters = filters.get("numeric_filters", []) if filters else []
    role_filter_object = filters.get("role_filter", None) if filters else None
    not_have_role_object = filters.get("nothaverole", None) if filters else None

    event_list_set = set()
    user_data_for_excel = []
    processed_user_ids = set()

    filter_log_parts = []
    for f in numeric_filters:
        field, op, val = f
        if field == "total_message_count":
            filter_log_parts.append(f"MsgCount({op}{val})")
        elif field == "tweet_count":
            filter_log_parts.append(f"TwtCount({op}{val})")
        elif field == "joined":
            filter_log_parts.append(f"Joined({op}{val})")
        elif field == "won":
            filter_log_parts.append(f"Won({op}{val})")
    if role_filter_object:
        filter_log_parts.append(f"Role({role_filter_object.name})")
    if not_have_role_object:
        filter_log_parts.append(f"NOTRole({not_have_role_object.name})")
    filter_log_str = ", ".join(filter_log_parts) if filter_log_parts else "None"
    print(f"Gathering data for Excel ({guild.name}). Filters: [{filter_log_str}], Sort: {sort_key}")

    for user_id_str, data in stats_data.items():
        if not user_id_str.isdigit() or not isinstance(data, dict) or user_id_str in processed_user_ids:
            continue
        try:
            user_id = int(user_id_str)
        except ValueError:
            continue
        member = guild.get_member(user_id)
        if not member:
            continue
        if not any(role.id in TARGET_ROLES for role in member.roles):
            continue

        pass_numeric = True
        for (field, operator, filter_value) in numeric_filters:
            if field == "total_message_count":
                user_val = data.get("total_message_count", 0)
            elif field == "tweet_count":
                user_val = len(data.get("twitter_links", []))
            elif field == "joined":
                user_val = len(data.get("events", []))
            elif field == "won":
                user_val = len(data.get("winners", []))
            else:
                user_val = 0
            op_map = {
                ">":  lambda a, b: a > b,
                "<":  lambda a, b: a < b,
                ">=": lambda a, b: a >= b,
                "<=": lambda a, b: a <= b,
                "=":  lambda a, b: a == b,
                "!=": lambda a, b: a != b
            }
            if operator in op_map:
                if not op_map[operator](user_val, filter_value):
                    pass_numeric = False
                    break
            else:
                pass_numeric = False
                break
        if not pass_numeric:
            continue

        if role_filter_object and (role_filter_object not in member.roles):
            continue
        if not_have_role_object and (not_have_role_object in member.roles):
            continue

        processed_user_ids.add(user_id_str)
        event_list_set.update(standardize_event_name(e) for e in data.get("events", []))
        event_list_set.update(standardize_event_name(w) for w in data.get("winners", []))
        role_names = [r.name for r in member.roles if r.name != "@everyone"]
        user_data_for_excel.append({
            "member": member,
            "user_id": user_id_str,
            "roles": ", ".join(role_names),
            "joined_count": len(data.get("events", [])),
            "won_count": len(data.get("winners", [])),
            "total_message_count": data.get("total_message_count", 0),
            "tweet_count": len(data.get("twitter_links", [])),
            "twitter_links_list": data.get("twitter_links", []),
            "raw_data": data
        })

    sorted_event_list = sorted(list(event_list_set))
    print(f"{len(user_data_for_excel)} members passed filters. Sorting...")

    if sort_key == "messages":
        user_data_for_excel.sort(key=lambda x: (-x["total_message_count"], x["member"].display_name.lower()))
    elif sort_key == "tweets":
        user_data_for_excel.sort(key=lambda x: (-x["tweet_count"], x["member"].display_name.lower()))
    else:
        user_data_for_excel.sort(key=lambda x: x["member"].display_name.lower())

    print("Writing Excel file...")
    try:
        workbook = xlsxwriter.Workbook(EXCEL_FILE_PATH)
        sheet = workbook.add_worksheet("Statistics")
        headers = ["User Name", "User ID", "Roles", "Joined Events Count", "Won Events Count", "Total Messages Sent", "Tweet Count"] + sorted_event_list + ["Posted Twitter Links"]
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'align': 'center', 'valign': 'vcenter'})
        header_map = {head: idx for idx, head in enumerate(headers)}
        for col, head in enumerate(headers):
            sheet.write(0, col, head, header_format)

        row_format = workbook.add_format({'valign': 'top'})
        wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
        num_format = workbook.add_format({'valign': 'top', 'num_format': '#,##0'})
        url_format = workbook.add_format({'font_color': 'blue', 'underline': 1, 'valign': 'top', 'text_wrap': True})

        for row_idx, user_info in enumerate(user_data_for_excel):
            row = row_idx + 1
            data = user_info["raw_data"]
            sheet.write(row, header_map.get("User Name"), user_info["member"].display_name, row_format)
            sheet.write(row, header_map.get("User ID"), user_info["user_id"], row_format)
            sheet.write(row, header_map.get("Roles"), user_info["roles"], wrap_format)
            sheet.write(row, header_map.get("Joined Events Count"), user_info["joined_count"], num_format)
            sheet.write(row, header_map.get("Won Events Count"), user_info["won_count"], num_format)
            sheet.write(row, header_map.get("Total Messages Sent"), user_info["total_message_count"], num_format)
            sheet.write(row, header_map.get("Tweet Count"), user_info["tweet_count"], num_format)

            user_events_std = {standardize_event_name(e) for e in data.get("events", [])}
            user_winners_std = {standardize_event_name(w) for w in data.get("winners", [])}
            for event_name_std in sorted_event_list:
                event_col = header_map.get(event_name_std, -1)
                if event_col != -1:
                    status = ""
                    if event_name_std in user_winners_std:
                        status = "🏆"
                    elif event_name_std in user_events_std:
                        status = "✅"
                    sheet.write(row, event_col, status, row_format)

            tw_col = header_map.get("Posted Twitter Links", -1)
            if tw_col != -1:
                joined_links = "\n".join(user_info["twitter_links_list"])
                sheet.write_string(row, tw_col, joined_links, url_format)

        sheet.set_column(header_map.get("User Name"), header_map.get("User Name"), 25)
        sheet.set_column(header_map.get("User ID"), header_map.get("User ID"), 20)
        sheet.set_column(header_map.get("Roles"), header_map.get("Roles"), 35)
        sheet.set_column(header_map.get("Joined Events Count"), header_map.get("Joined Events Count"), 18)
        sheet.set_column(header_map.get("Won Events Count"), header_map.get("Won Events Count"), 18)
        sheet.set_column(header_map.get("Total Messages Sent"), header_map.get("Total Messages Sent"), 20)
        sheet.set_column(header_map.get("Tweet Count"), header_map.get("Tweet Count"), 15)
        if sorted_event_list:
            first_event_col = header_map.get(sorted_event_list[0], -1)
            if first_event_col != -1:
                last_event_col = header_map.get(sorted_event_list[-1], first_event_col)
                sheet.set_column(first_event_col, last_event_col, 15)
        tw_col_idx = header_map.get("Posted Twitter Links", -1)
        if tw_col_idx != -1:
            sheet.set_column(tw_col_idx, tw_col_idx, 50)

        workbook.close()
        print(f"Excel '{EXCEL_FILE_PATH}' generated ({guild.name}). {len(user_data_for_excel)} members processed.")
        return True
    except Exception as e:
        print(f"ERROR: Writing Excel: {e}")
        traceback.print_exc()
        return False

# --- ROLE MANAGEMENT COMMANDS ---

@bot.command(name="settrackauthorizedrole")
@admin_only()
async def set_track_authorized_role(ctx, role: discord.Role):
    """Adds a role to the track authorized roles (allowing use of track commands)."""
    global AUTHORIZED_ROLES
    if role.id not in AUTHORIZED_ROLES:
        AUTHORIZED_ROLES.append(role.id)
        save_stats()
        await ctx.send(f"✅ {role.mention} has been added to track authorized roles.")
    else:
        await ctx.send(f"ℹ️ {role.mention} is already set as a track authorized role.")

@bot.command(name="settracktargetrole")
@admin_only()
async def set_track_target_role(ctx, *roles: discord.Role):
    """Adds one or more roles to the track target roles (only users with these roles are tracked)."""
    global TARGET_ROLES
    added_roles = []
    for role in roles:
        if role.id not in TARGET_ROLES:
            TARGET_ROLES.append(role.id)
            added_roles.append(role.mention)
    if added_roles:
        save_stats()
        await ctx.send(f"✅ Added to track target roles: {', '.join(added_roles)}.")
    else:
        await ctx.send("ℹ️ No new roles were added for tracking.")

@bot.command(name="removeauthorizedrole")
@admin_only()
async def remove_authorized_role(ctx, *roles: discord.Role):
    """Removes one or more roles from the track authorized roles."""
    global AUTHORIZED_ROLES
    removed = []
    for role in roles:
        if role.id in AUTHORIZED_ROLES:
            AUTHORIZED_ROLES.remove(role.id)
            removed.append(role.mention)
    if removed:
        save_stats()
        await ctx.send(f"✅ Removed from track authorized roles: {', '.join(removed)}.")
    else:
        await ctx.send("ℹ️ No matching authorized roles found to remove.")

@bot.command(name="removetargetrole")
@admin_only()
async def remove_target_role(ctx, role: discord.Role):
    """Removes a role from the track target roles."""
    global TARGET_ROLES
    if role.id in TARGET_ROLES:
        TARGET_ROLES.remove(role.id)
        save_stats()
        await ctx.send(f"✅ {role.mention} has been removed from track target roles.")
    else:
        await ctx.send(f"ℹ️ {role.mention} is not set as a track target role.")

@bot.command(name="setstatsroleauthorized")
@admin_only()
async def set_stats_role_authorized(ctx, *roles: discord.Role):
    """Adds one or more roles to those allowed to use the !stats command."""
    global STATS_AUTHORIZED_ROLES
    added = []
    for role in roles:
        if role.id not in STATS_AUTHORIZED_ROLES:
            STATS_AUTHORIZED_ROLES.append(role.id)
            added.append(role.mention)
    if added:
        save_stats()
        await ctx.send(f"✅ Added to stats authorized roles: {', '.join(added)}.")
    else:
        await ctx.send("ℹ️ No new roles were added for stats authorization.")

@bot.command(name="removestatsroleauthorized")
@admin_only()
async def remove_stats_role_authorized(ctx, role: discord.Role):
    """Removes a role from those allowed to use the !stats command."""
    global STATS_AUTHORIZED_ROLES
    if role.id in STATS_AUTHORIZED_ROLES:
        STATS_AUTHORIZED_ROLES.remove(role.id)
        save_stats()
        await ctx.send(f"✅ Removed {role.mention} from stats authorized roles.")
    else:
        await ctx.send(f"ℹ️ {role.mention} is not set as a stats authorized role.")

# --- COMMANDS ---
@bot.command(name="trackhelp", aliases=["thelp"])
@admin_only()
async def trackhelp(ctx):
    """Displays the list of administrator commands, split across multiple embeds if needed."""
    # Define all help fields
    help_fields = [
        {"name": "!stats [@user or ID]", "value": "Shows statistics for the specified user (or yourself).\nTwitter links that exceed embed limits are split.", "inline": False},
        {"name": "!allstats [filters...] [sort_key]", 
         "value": (
            "Generates an Excel report.\n\n"
            "**Numeric Filters:**\n"
            "  - msgcount, twtcount, joined, won (e.g., msgcount>100)\n"
            "**Role Filters:**\n"
            "  - To require a role: provide role mention or ID (e.g., @SomeRole or 123456789012345678)\n"
            "  - To require NOT having a role: 'nothaverole' followed by role\n\n"
            "Example: `!allstats msgcount>100 twtcount>=10 joined>=1 won>=0 nothaverole @SomeRole messages`\n"
            "Sort keys: 'messages' or 'tweets'"
         ), 
         "inline": False},
        {"name": "!filteruserid [filters...] [id]", 
         "value": (
            "Filters users based on numeric (msgcount, twtcount, joined, won) and role filters.\n"
            "By default, returns user IDs and display names.\n"
            "If you add 'id' as an argument, only user IDs are returned.\n\n"
            "Example: `!filteruserid msgcount>100 twtcount>=10 nothaverole @SomeRole joined>=1 won>=0`\n"
            "or `!filteruserid id`"
         ), 
         "inline": False},
        {"name": "!listexcels", "value": "Lists available Excel (.xlsx) files.", "inline": False},
        {"name": "!deleteexcel <filename.xlsx>", "value": "Deletes the specified Excel file.", "inline": False},
        {"name": "!twitterlog <channel_id>", "value": "Sets the channel for Twitter/X link monitoring.", "inline": False},
        {"name": "!removetwitterlog", "value": "Disables Twitter/X link monitoring.", "inline": False},
        {"name": "--- Role Management ---", "value": "\u200b", "inline": False},
        {"name": "!settrackauthorizedrole <role>", "value": "Allows the role to use track commands.", "inline": False},
        {"name": "!settracktargetrole <role(s)>", "value": "Sets target roles; only users with these roles are tracked.", "inline": False},
        {"name": "!removeauthorizedrole <role(s)>", "value": "Removes roles from track authorized list.", "inline": False},
        {"name": "!removetargetrole <role>", "value": "Removes a role from track target roles.", "inline": False},
        {"name": "!setstatsroleauthorized <role(s)>", "value": "Allows the role to use the !stats command.", "inline": False},
        {"name": "!removestatsroleauthorized <role>", "value": "Removes the role's access to the !stats command.", "inline": False},
        {"name": "--- Bulk Actions ---", "value": "\u200b", "inline": False},
        {"name": "!bulkban <id1> [id2...] [reason=...]", "value": "Bans multiple users by ID.", "inline": False},
        {"name": "!bulkgiverole <role_id> <id1> [id2...]", "value": "Gives the specified role to multiple users by ID.", "inline": False},
        {"name": "--- Event Management ---", "value": "\u200b", "inline": False},
        {"name": "!addevent <event_name> <id...>", "value": "Adds users to an event ('joined').", "inline": False},
        {"name": "!eventwinners <event_name> <id...>", "value": "Marks specified users as winners for an event.", "inline": False},
        {"name": "!notjoined <event_name> <id...>", "value": "Removes users from an event.", "inline": False},
        {"name": "!delevent <event_name>", "value": "Deletes an event from all records.", "inline": False},
        {"name": "!copyevent <event_name> <channel_id>", "value": "Copies members from a voice/stage channel as 'joined'.", "inline": False},
        {"name": "!winnerlist <event_name>", "value": "Lists winners of an event (sends a file).", "inline": False},
        {"name": "!joinedlist <event_name>", "value": "Lists users who only joined (but didn't win) (sends a file).", "inline": False},
        {"name": "!fixwinners <mod> <event_name> <id...>", "value": "Fixes winner status issues ('joined' or 'notjoined').", "inline": False},
        {"name": "!fixjoined <mod> <event_name> <id...>", "value": "Fixes joined status issues ('winner' or 'notjoined').", "inline": False},
        {"name": "!fixnotjoined <mod> <event_name> <id...>", "value": "Fixes not-joined status issues ('joined' or 'winner').", "inline": False}
    ]
    
    # Discord has a limit of 25 fields per embed
    MAX_FIELDS_PER_EMBED = 25
    
    # Calculate how many embeds we need
    num_embeds = (len(help_fields) + MAX_FIELDS_PER_EMBED - 1) // MAX_FIELDS_PER_EMBED
    
    for i in range(num_embeds):
        # Create a new embed for each batch of fields
        start_idx = i * MAX_FIELDS_PER_EMBED
        end_idx = min((i + 1) * MAX_FIELDS_PER_EMBED, len(help_fields))
        
        # Set appropriate title based on which embed this is
        if num_embeds == 1:
            title = "📘 Admin Command List"
            description = "Available administrator commands:"
        else:
            title = f"📘 Admin Command List (Part {i+1}/{num_embeds})"
            description = f"Available administrator commands (continued):"
        
        embed = discord.Embed(title=title, description=description, color=discord.Color.teal())
        
        # Add fields for this embed
        for field in help_fields[start_idx:end_idx]:
            embed.add_field(name=field["name"], value=field["value"], inline=field["inline"])
        
        # Add footer to the last embed
        if i == num_embeds - 1:
            embed.set_footer(text=f"Bot Prefix: {bot.command_prefix}")
        
        # Send the embed
        await ctx.send(embed=embed)

@bot.command(name="twitterlog")
@admin_only()
async def set_twitter_log_channel_new(ctx, channel_id: int):
    """Sets the channel by ID for Twitter/X link monitoring and scans its history."""
    global twitter_log_channel_id, posted_links_set
    channel = ctx.guild.get_channel(channel_id)
    if not channel or not isinstance(channel, discord.TextChannel):
        await ctx.send(f"❌ Error: Valid text channel with ID `{channel_id}` not found.")
        return
    original_channel_id = twitter_log_channel_id
    twitter_log_channel_id = channel.id
    await ctx.send(f"✅ Twitter log channel set to {channel.mention}.\n⏳ Scanning channel history (max 10k messages)...")
    url_pattern = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[A-Za-z0-9_]+/status/[0-9]+")
    history_limit = 10000
    added = 0
    processed = 0
    changed = False
    try:
        async for message in channel.history(limit=history_limit):
            processed += 1
            if message.author.bot or not message.content:
                continue
            content = message.content.strip()
            match = url_pattern.search(content)
            if match:
                extracted_url = match.group(0)
                if content == extracted_url:
                    norm_url = extracted_url
                    if norm_url.startswith("http://"):
                        norm_url = norm_url.replace("http://", "https://", 1)
                    if "://twitter.com/" in norm_url:
                        norm_url = norm_url.replace("://twitter.com/", "://x.com/", 1)
                    if "://www.x.com/" in norm_url:
                        norm_url = norm_url.replace("://www.x.com/", "://x.com/", 1)
                    if norm_url not in posted_links_set:
                        posted_links_set.add(norm_url)
                        uid = str(message.author.id)
                        udata = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE())
                        udata.setdefault("twitter_links", []).append(norm_url)
                        added += 1
                        changed = True
    except discord.Forbidden:
        await ctx.send("❌ Error: Missing permission to read channel history.")
        twitter_log_channel_id = original_channel_id
        return
    except Exception as e:
        await ctx.send(f"❌ Error during history scan: {e}")
        print(f"Error (twitterlog history scan): {e}")
    if changed or original_channel_id != twitter_log_channel_id:
        save_stats()
    await ctx.send(f"✅ History scan complete. Added {added} new unique links. Channel is active!")

@set_twitter_log_channel_new.error
async def twitterlog_error(ctx, error):
    if isinstance(error, commands.BadArgument):
        await ctx.send("❌ Error: Please provide a valid Channel ID (numbers only).")
    elif isinstance(error, MissingRequiredArgument):
        await ctx.send("❌ Error: Channel ID is required. Usage: `!twitterlog <channel_id>`")
    elif isinstance(error, CheckFailure):
        pass
    else:
        await ctx.send(f"Unexpected error: {error}")
        print(f"Error (twitterlog): {error}")

@bot.command(name="removetwitterlog")
@admin_only()
async def remove_twitter_log_channel(ctx):
    """Disables Twitter/X link monitoring."""
    global twitter_log_channel_id
    if twitter_log_channel_id is None:
        await ctx.send("ℹ️ Twitter log feature is already disabled.")
        return
    twitter_log_channel_id = None
    save_stats()
    await ctx.send("✅ Twitter log feature disabled.")

@bot.command(name="allstats")
@admin_only()
async def allstats(ctx, *args):
    """
    Generates an Excel report based on filters.
    Supported filters (order not critical):
       - Numeric: msgcount, twtcount, joined, won (e.g., msgcount>100)
       - Role: provide role mention or ID for users that must have that role.
       - nothaverole: use 'nothaverole' followed by a role.
       - Sort keys: 'messages' or 'tweets'
    Example:
       !allstats msgcount>100 twtcount>=10 joined>=1 won>=0 nothaverole @SomeRole messages
    """
    numeric_filters = []
    role_filter_object = None
    nothaverole_filter_object = None
    sort_param = None

    # Process arguments
    i = 0
    while i < len(args):
        arg = args[i]
        # Check for numeric filters
        numeric_filter = parse_numeric_filter(arg)
        if numeric_filter:
            numeric_filters.append(numeric_filter)
        # Check for sort parameter
        elif arg.lower() in ["messages", "tweets"]:
            sort_param = arg.lower()
        # Check for nothaverole filter
        elif arg.lower() == "nothaverole" and i + 1 < len(args):
            i += 1
            next_arg = args[i]
            # Try to get role from mention or ID
            role = None
            if next_arg.startswith("<@&") and next_arg.endswith(">"):
                role_id_str = next_arg[3:-1]
                try:
                    role_id = int(role_id_str)
                    role = ctx.guild.get_role(role_id)
                except ValueError:
                    pass
            else:
                try:
                    role_id = int(next_arg)
                    role = ctx.guild.get_role(role_id)
                except ValueError:
                    pass
            if role:
                nothaverole_filter_object = role
            else:
                await ctx.send(f"❌ Error: Could not find role from '{next_arg}'")
                return
        # Check for role filter (by mention or ID)
        else:
            role = None
            if arg.startswith("<@&") and arg.endswith(">"):
                role_id_str = arg[3:-1]
                try:
                    role_id = int(role_id_str)
                    role = ctx.guild.get_role(role_id)
                except ValueError:
                    pass
            else:
                try:
                    role_id = int(arg)
                    role = ctx.guild.get_role(role_id)
                except ValueError:
                    pass
            if role:
                if role_filter_object is None:
                    role_filter_object = role
                else:
                    await ctx.send("❌ Error: Only one role filter can be specified.")
                    return
            else:
                await ctx.send(f"❌ Error: Unrecognized filter: '{arg}'")
                return
        i += 1

    # Create filters dictionary
    filters = {
        "numeric_filters": numeric_filters,
        "role_filter": role_filter_object,
        "nothaverole": nothaverole_filter_object
    }

    # Generate Excel
    await ctx.send("⏳ Generating Excel report...")
    success = generate_excel(ctx.guild, filters, sort_param)
    if success:
        try:
            await ctx.send("✅ Excel report generated!", file=discord.File(EXCEL_FILE_PATH))
        except Exception as e:
            await ctx.send(f"❌ Error sending Excel: {e}")
            print(f"Error sending Excel: {e}")
    else:
        await ctx.send("❌ Failed to generate Excel report.")

@bot.command(name="listexcels")
@admin_only()
async def list_excels(ctx):
    """Lists available Excel (.xlsx) files."""
    excel_files = [f for f in os.listdir('.') if f.endswith('.xlsx')]
    if excel_files:
        await ctx.send("📊 Available Excel files:\n" + "\n".join(excel_files))
    else:
        await ctx.send("ℹ️ No Excel files found.")

@bot.command(name="deleteexcel")
@admin_only()
async def delete_excel(ctx, filename: str):
    """Deletes the specified Excel file."""
    if not filename.endswith('.xlsx'):
        filename += '.xlsx'
    if os.path.exists(filename):
        try:
            os.remove(filename)
            await ctx.send(f"✅ Deleted: {filename}")
        except OSError as e:
            await ctx.send(f"❌ Error deleting file: {e}")
    else:
        await ctx.send(f"❌ File not found: {filename}")

@bot.command(name="stats")
async def stats(ctx, member: discord.Member = None):
    """Shows statistics for the specified user (or yourself)."""
    if not await stats_authorized_check(ctx):
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass
        return

    if member is None:
        member = ctx.author

    user_id = str(member.id)
    if user_id not in stats_data:
        await ctx.send(f"ℹ️ No statistics found for {member.display_name}.")
        return

    user_data = stats_data[user_id]
    events_joined = user_data.get("events", [])
    events_won = user_data.get("winners", [])
    twitter_links = user_data.get("twitter_links", [])
    total_message_count = user_data.get("total_message_count", 0)

    embed = discord.Embed(
        title=f"📊 Statistics for {member.display_name}",
        color=discord.Color.blue()
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="User ID", value=user_id, inline=True)
    embed.add_field(name="Total Messages", value=f"{total_message_count:,}", inline=True)
    embed.add_field(name="Twitter Links", value=str(len(twitter_links)), inline=True)
    embed.add_field(name="Events Joined", value=str(len(events_joined)), inline=True)
    embed.add_field(name="Events Won", value=str(len(events_won)), inline=True)

    # Add events joined if any
    if events_joined:
        events_text = ", ".join(sorted(events_joined))
        chunks = chunk_text_by_size(events_text)
        for i, chunk in enumerate(chunks):
            if i == 0:
                embed.add_field(name="Events Joined", value=chunk, inline=False)
            else:
                embed.add_field(name="Events Joined (continued)", value=chunk, inline=False)

    # Add events won if any
    if events_won:
        events_text = ", ".join(sorted(events_won))
        chunks = chunk_text_by_size(events_text)
        for i, chunk in enumerate(chunks):
            if i == 0:
                embed.add_field(name="Events Won", value=chunk, inline=False)
            else:
                embed.add_field(name="Events Won (continued)", value=chunk, inline=False)

    await ctx.send(embed=embed)

    # If there are Twitter links, send them in separate embeds
    if twitter_links:
        MAX_LINKS_PER_EMBED = 10
        num_embeds = (len(twitter_links) + MAX_LINKS_PER_EMBED - 1) // MAX_LINKS_PER_EMBED

        for i in range(num_embeds):
            start_idx = i * MAX_LINKS_PER_EMBED
            end_idx = min((i + 1) * MAX_LINKS_PER_EMBED, len(twitter_links))
            
            links_embed = discord.Embed(
                title=f"🔗 Twitter Links for {member.display_name} ({i+1}/{num_embeds})",
                color=discord.Color.blue()
            )
            
            for j, link in enumerate(twitter_links[start_idx:end_idx], 1):
                links_embed.add_field(name=f"Link {start_idx + j}", value=link, inline=False)
            
            await ctx.send(embed=links_embed)

@stats.error
async def stats_error(ctx, error):
    if isinstance(error, MemberNotFound):
        # Önce yetki kontrolü yap
        if await stats_authorized_check(ctx):
            await ctx.send("❌ Error: Member not found. Please provide a valid member mention or ID.")
        else:
            try:
                await ctx.message.delete()
            except (discord.Forbidden, discord.HTTPException):
                pass
    elif isinstance(error, CheckFailure):
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass
    elif isinstance(error, BadArgument):
        # Önce yetki kontrolü yap
        if await stats_authorized_check(ctx):
            await ctx.send("❌ Error: Invalid argument. Usage: `!stats [@user or ID]`")
        else:
            try:
                await ctx.message.delete()
            except (discord.Forbidden, discord.HTTPException):
                pass
    else:
        # Önce yetki kontrolü yap
        if await stats_authorized_check(ctx):
            await ctx.send(f"❌ Unexpected error: {error}")
        else:
            try:
                await ctx.message.delete()
            except (discord.Forbidden, discord.HTTPException):
                pass
        print(f"Error (stats): {error}")

@bot.command(name="filteruserid")
@admin_only()
async def filter_user_id(ctx, *args):
    """
    Filters users based on numeric and role filters.
    Returns user IDs and display names, or just IDs if 'id' is specified.
    """
    numeric_filters = []
    role_filter_object = None
    nothaverole_filter_object = None
    id_only = False

    # Process arguments
    i = 0
    while i < len(args):
        arg = args[i]
        # Check for ID-only flag
        if arg.lower() == "id":
            id_only = True
        # Check for numeric filters
        else:
            numeric_filter = parse_numeric_filter(arg)
            if numeric_filter:
                numeric_filters.append(numeric_filter)
            # Check for nothaverole filter
            elif arg.lower() == "nothaverole" and i + 1 < len(args):
                i += 1
                next_arg = args[i]
                # Try to get role from mention or ID
                role = None
                if next_arg.startswith("<@&") and next_arg.endswith(">"):
                    role_id_str = next_arg[3:-1]
                    try:
                        role_id = int(role_id_str)
                        role = ctx.guild.get_role(role_id)
                    except ValueError:
                        pass
                else:
                    try:
                        role_id = int(next_arg)
                        role = ctx.guild.get_role(role_id)
                    except ValueError:
                        pass
                if role:
                    nothaverole_filter_object = role
                else:
                    await ctx.send(f"❌ Error: Could not find role from '{next_arg}'")
                    return
            # Check for role filter (by mention or ID)
            else:
                role = None
                if arg.startswith("<@&") and arg.endswith(">"):
                    role_id_str = arg[3:-1]
                    try:
                        role_id = int(role_id_str)
                        role = ctx.guild.get_role(role_id)
                    except ValueError:
                        pass
                else:
                    try:
                        role_id = int(arg)
                        role = ctx.guild.get_role(role_id)
                    except ValueError:
                        pass
                if role:
                    if role_filter_object is None:
                        role_filter_object = role
                    else:
                        await ctx.send("❌ Error: Only one role filter can be specified.")
                        return
                else:
                    await ctx.send(f"❌ Error: Unrecognized filter: '{arg}'")
                    return
        i += 1

    # Filter users
    filtered_users = []
    for user_id_str, data in stats_data.items():
        if not user_id_str.isdigit() or not isinstance(data, dict):
            continue
        try:
            user_id = int(user_id_str)
        except ValueError:
            continue
        member = ctx.guild.get_member(user_id)
        if not member:
            continue
        if not any(role.id in TARGET_ROLES for role in member.roles):
            continue

        # Apply numeric filters
        pass_numeric = True
        for (field, operator, filter_value) in numeric_filters:
            if field == "total_message_count":
                user_val = data.get("total_message_count", 0)
            elif field == "tweet_count":
                user_val = len(data.get("twitter_links", []))
            elif field == "joined":
                user_val = len(data.get("events", []))
            elif field == "won":
                user_val = len(data.get("winners", []))
            else:
                user_val = 0
            op_map = {
                ">":  lambda a, b: a > b,
                "<":  lambda a, b: a < b,
                ">=": lambda a, b: a >= b,
                "<=": lambda a, b: a <= b,
                "=":  lambda a, b: a == b,
                "!=": lambda a, b: a != b
            }
            if operator in op_map:
                if not op_map[operator](user_val, filter_value):
                    pass_numeric = False
                    break
            else:
                pass_numeric = False
                break
        if not pass_numeric:
            continue

        # Apply role filters
        if role_filter_object and (role_filter_object not in member.roles):
            continue
        if nothaverole_filter_object and (nothaverole_filter_object in member.roles):
            continue

        filtered_users.append((user_id_str, member.display_name))

    # Send results
    if not filtered_users:
        await ctx.send("ℹ️ No users match the specified filters.")
        return

    filtered_users.sort(key=lambda x: x[1].lower())  # Sort by display name
    result_lines = []
    
    if id_only:
        result_lines = [user_id for user_id, _ in filtered_users]
    else:
        result_lines = [f"{user_id} - {name}" for user_id, name in filtered_users]
    
    result_text = "\n".join(result_lines)
    chunks = chunk_text_by_size(result_text, 1900)  # Leave room for code block formatting
    
    for i, chunk in enumerate(chunks):
        if len(chunks) > 1:
            header = f"Filtered Users (Part {i+1}/{len(chunks)}, Total: {len(filtered_users)}):\n"
        else:
            header = f"Filtered Users (Total: {len(filtered_users)}):\n"
        await ctx.send(f"```\n{header}{chunk}\n```")

@bot.command(name="addevent")
@admin_only()
async def add_event(ctx, event_name: str, *user_ids: str):
    """Adds users to an event ('joined')."""
    if not event_name or not user_ids:
        await ctx.send("❌ Error: Event name and at least one user ID are required.")
        return

    added = []
    not_found = []
    already_added = []

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            if member:
                user_data = stats_data.setdefault(str(user_id), DEFAULT_USER_TEMPLATE())
                if event_name not in user_data.get("events", []):
                    user_data.setdefault("events", []).append(event_name)
                    added.append(f"{member.display_name} ({user_id})")
                else:
                    already_added.append(f"{member.display_name} ({user_id})")
            else:
                not_found.append(id_str)
        except ValueError:
            not_found.append(id_str)

    if added:
        save_stats()
        added_text = "\n".join(added)
        await ctx.send(f"✅ Added to event '{event_name}':\n{added_text}")
    
    if already_added:
        already_text = "\n".join(already_added)
        await ctx.send(f"ℹ️ Already in event '{event_name}':\n{already_text}")
    
    if not_found:
        not_found_text = ", ".join(not_found)
        await ctx.send(f"❌ Users not found: {not_found_text}")

@bot.command(name="eventwinners")
@admin_only()
async def event_winners(ctx, event_name: str, *user_ids: str):
    """Marks specified users as winners for an event."""
    if not event_name or not user_ids:
        await ctx.send("❌ Error: Event name and at least one user ID are required.")
        return

    added_winners = []
    not_found = []
    already_winners = []
    not_joined = []

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            if member:
                user_id_str = str(user_id)
                if user_id_str not in stats_data:
                    stats_data[user_id_str] = DEFAULT_USER_TEMPLATE()
                
                user_data = stats_data[user_id_str]
                
                # Check if user has joined the event
                if event_name not in user_data.get("events", []):
                    user_data.setdefault("events", []).append(event_name)
                    
                # Check if user is already a winner
                if event_name in user_data.get("winners", []):
                    already_winners.append(f"{member.display_name} ({user_id})")
                else:
                    user_data.setdefault("winners", []).append(event_name)
                    added_winners.append(f"{member.display_name} ({user_id})")
            else:
                not_found.append(id_str)
        except ValueError:
            not_found.append(id_str)

    if added_winners:
        save_stats()
        winners_text = "\n".join(added_winners)
        await ctx.send(f"🏆 Added winners for event '{event_name}':\n{winners_text}")
    
    if already_winners:
        already_text = "\n".join(already_winners)
        await ctx.send(f"ℹ️ Already winners in event '{event_name}':\n{already_text}")
    
    if not_joined:
        not_joined_text = "\n".join(not_joined)
        await ctx.send(f"ℹ️ Users automatically added to event '{event_name}' (weren't joined before):\n{not_joined_text}")
    
    if not_found:
        not_found_text = ", ".join(not_found)
        await ctx.send(f"❌ Users not found: {not_found_text}")

@bot.command(name="notjoined")
@admin_only()
async def not_joined(ctx, event_name: str, *user_ids: str):
    """Removes users from an event."""
    if not event_name or not user_ids:
        await ctx.send("❌ Error: Event name and at least one user ID are required.")
        return

    removed = []
    not_found = []
    not_in_event = []

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            if member:
                user_id_str = str(user_id)
                if user_id_str in stats_data:
                    user_data = stats_data[user_id_str]
                    
                    # Remove from events list
                    if event_name in user_data.get("events", []):
                        user_data["events"].remove(event_name)
                        removed.append(f"{member.display_name} ({user_id})")
                    else:
                        not_in_event.append(f"{member.display_name} ({user_id})")
                    
                    # Also remove from winners list if present
                    if event_name in user_data.get("winners", []):
                        user_data["winners"].remove(event_name)
            else:
                not_found.append(id_str)
        except ValueError:
            not_found.append(id_str)

    if removed:
        save_stats()
        removed_text = "\n".join(removed)
        await ctx.send(f"✅ Removed from event '{event_name}':\n{removed_text}")
    
    if not_in_event:
        not_in_text = "\n".join(not_in_event)
        await ctx.send(f"ℹ️ Not in event '{event_name}':\n{not_in_text}")
    
    if not_found:
        not_found_text = ", ".join(not_found)
        await ctx.send(f"❌ Users not found: {not_found_text}")

@bot.command(name="delevent")
@admin_only()
async def del_event(ctx, event_name: str):
    """Deletes an event from all records."""
    if not event_name:
        await ctx.send("❌ Error: Event name is required.")
        return

    event_name_std = standardize_event_name(event_name)
    affected_users = 0

    for user_id, data in stats_data.items():
        if not user_id.isdigit() or not isinstance(data, dict):
            continue
        
        modified = False
        
        # Check events list
        if "events" in data:
            events = data["events"]
            for i, e in enumerate(events[:]):
                if standardize_event_name(e) == event_name_std:
                    events.remove(e)
                    modified = True
        
        # Check winners list
        if "winners" in data:
            winners = data["winners"]
            for i, w in enumerate(winners[:]):
                if standardize_event_name(w) == event_name_std:
                    winners.remove(w)
                    modified = True
        
        if modified:
            affected_users += 1

    if affected_users > 0:
        save_stats()
        await ctx.send(f"✅ Event '{event_name}' deleted from {affected_users} user records.")
    else:
        await ctx.send(f"ℹ️ No records found for event '{event_name}'.")

@bot.command(name="copyevent")
@admin_only()
async def copy_event(ctx, event_name: str, channel_id: int):
    """Copies members from a voice/stage channel as 'joined'."""
    if not event_name:
        await ctx.send("❌ Error: Event name is required.")
        return

    channel = ctx.guild.get_channel(channel_id)
    if not channel or not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
        await ctx.send(f"❌ Error: Channel with ID {channel_id} is not a valid voice or stage channel.")
        return

    if not channel.members:
        await ctx.send(f"ℹ️ No members found in {channel.name}.")
        return

    added = []
    already_added = []
    
    for member in channel.members:
        if member.bot:
            continue
        
        if not any(role.id in TARGET_ROLES for role in member.roles):
            continue
            
        user_id = str(member.id)
        user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
        
        if event_name not in user_data.get("events", []):
            user_data.setdefault("events", []).append(event_name)
            added.append(f"{member.display_name} ({member.id})")
        else:
            already_added.append(f"{member.display_name} ({member.id})")

    if added:
        save_stats()
        added_count = len(added)
        await ctx.send(f"✅ Added {added_count} members from {channel.name} to event '{event_name}'.")
        
        # Send detailed list if not too long
        if added_count <= 20:
            added_text = "\n".join(added)
            await ctx.send(f"Added users:\n{added_text}")
        else:
            await ctx.send(f"Added {added_count} users (too many to list).")
    else:
        await ctx.send(f"ℹ️ No new members added from {channel.name} to event '{event_name}'.")
        
    if already_added:
        already_count = len(already_added)
        await ctx.send(f"ℹ️ {already_count} members were already in event '{event_name}'.")

@bot.command(name="winnerlist")
@admin_only()
async def winner_list(ctx, event_name: str):
    """Lists winners of an event (sends a file)."""
    if not event_name:
        await ctx.send("❌ Error: Event name is required.")
        return

    event_name_std = standardize_event_name(event_name)
    winners = []

    for user_id, data in stats_data.items():
        if not user_id.isdigit() or not isinstance(data, dict):
            continue
        
        # Check if user won this event
        user_winners = data.get("winners", [])
        is_winner = False
        for w in user_winners:
            if standardize_event_name(w) == event_name_std:
                is_winner = True
                break
                
        if is_winner:
            member = ctx.guild.get_member(int(user_id))
            if member and any(role.id in TARGET_ROLES for role in member.roles):
                winners.append((user_id, member.display_name))

    if not winners:
        await ctx.send(f"ℹ️ No winners found for event '{event_name}'.")
        return

    winners.sort(key=lambda x: x[1].lower())  # Sort by display name
    
    filename = f"winners_{sanitize_filename(event_name)}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"Winners for event '{event_name}' ({len(winners)} users):\n\n")
        for user_id, name in winners:
            f.write(f"{user_id} - {name}\n")
    
    await ctx.send(f"🏆 Winners list for '{event_name}':", file=discord.File(filename))
    
    try:
        os.remove(filename)
    except:
        pass

@bot.command(name="joinedlist")
@admin_only()
async def joined_list(ctx, event_name: str):
    """Lists users who only joined (but didn't win) (sends a file)."""
    if not event_name:
        await ctx.send("❌ Error: Event name is required.")
        return

    event_name_std = standardize_event_name(event_name)
    joined_only = []

    for user_id, data in stats_data.items():
        if not user_id.isdigit() or not isinstance(data, dict):
            continue
        
        # Check if user joined this event
        user_events = data.get("events", [])
        joined = False
        for e in user_events:
            if standardize_event_name(e) == event_name_std:
                joined = True
                break
                
        if not joined:
            continue
            
        # Check if user won this event
        user_winners = data.get("winners", [])
        won = False
        for w in user_winners:
            if standardize_event_name(w) == event_name_std:
                won = True
                break
                
        # Only include users who joined but didn't win
        if joined and not won:
            member = ctx.guild.get_member(int(user_id))
            if member and any(role.id in TARGET_ROLES for role in member.roles):
                joined_only.append((user_id, member.display_name))

    if not joined_only:
        await ctx.send(f"ℹ️ No users found who only joined (but didn't win) event '{event_name}'.")
        return

    joined_only.sort(key=lambda x: x[1].lower())  # Sort by display name
    
    filename = f"joined_{sanitize_filename(event_name)}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"Users who joined but didn't win event '{event_name}' ({len(joined_only)} users):\n\n")
        for user_id, name in joined_only:
            f.write(f"{user_id} - {name}\n")
    
    await ctx.send(f"✅ Joined-only list for '{event_name}':", file=discord.File(filename))
    
    try:
        os.remove(filename)
    except:
        pass

@bot.command(name="fixwinners")
@admin_only()
async def fix_winners(ctx, mod: str, event_name: str, *user_ids: str):
    """Fixes winner status issues ('joined' or 'notjoined')."""
    if not mod or not event_name or not user_ids:
        await ctx.send("❌ Error: Mode, event name, and at least one user ID are required.")
        return
        
    mod = mod.lower()
    if mod not in ["joined", "notjoined"]:
        await ctx.send("❌ Error: Mode must be 'joined' or 'notjoined'.")
        return

    fixed = []
    not_found = []
    no_change = []

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            if member:
                user_id_str = str(user_id)
                if user_id_str not in stats_data:
                    stats_data[user_id_str] = DEFAULT_USER_TEMPLATE()
                
                user_data = stats_data[user_id_str]
                
                if mod == "joined":
                    # Make sure user is in event but keep winner status
                    if event_name not in user_data.get("events", []):
                        user_data.setdefault("events", []).append(event_name)
                        fixed.append(f"{member.display_name} ({user_id}) - Added to joined")
                    elif event_name not in user_data.get("winners", []):
                        no_change.append(f"{member.display_name} ({user_id}) - Already joined (not winner)")
                    else:
                        no_change.append(f"{member.display_name} ({user_id}) - Already joined (and winner)")
                        
                elif mod == "notjoined":
                    # Remove from events and winners
                    changed = False
                    if event_name in user_data.get("events", []):
                        user_data["events"].remove(event_name)
                        changed = True
                    if event_name in user_data.get("winners", []):
                        user_data["winners"].remove(event_name)
                        changed = True
                        
                    if changed:
                        fixed.append(f"{member.display_name} ({user_id}) - Removed from event")
                    else:
                        no_change.append(f"{member.display_name} ({user_id}) - Already not joined")
            else:
                not_found.append(id_str)
        except ValueError:
            not_found.append(id_str)

    if fixed:
        save_stats()
        fixed_text = "\n".join(fixed)
        await ctx.send(f"✅ Fixed winner status for event '{event_name}' (mode: {mod}):\n{fixed_text}")
    
    if no_change:
        no_change_text = "\n".join(no_change)
        await ctx.send(f"ℹ️ No changes needed for some users in event '{event_name}':\n{no_change_text}")
    
    if not_found:
        not_found_text = ", ".join(not_found)
        await ctx.send(f"❌ Users not found: {not_found_text}")

@bot.command(name="fixjoined")
@admin_only()
async def fix_joined(ctx, mod: str, event_name: str, *user_ids: str):
    """Fixes joined status issues ('winner' or 'notjoined')."""
    if not mod or not event_name or not user_ids:
        await ctx.send("❌ Error: Mode, event name, and at least one user ID are required.")
        return
        
    mod = mod.lower()
    if mod not in ["winner", "notjoined"]:
        await ctx.send("❌ Error: Mode must be 'winner' or 'notjoined'.")
        return

    fixed = []
    not_found = []
    no_change = []

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            if member:
                user_id_str = str(user_id)
                if user_id_str not in stats_data:
                    stats_data[user_id_str] = DEFAULT_USER_TEMPLATE()
                
                user_data = stats_data[user_id_str]
                
                if mod == "winner":
                    # Make user a winner (implies joined)
                    changed = False
                    if event_name not in user_data.get("events", []):
                        user_data.setdefault("events", []).append(event_name)
                        changed = True
                    if event_name not in user_data.get("winners", []):
                        user_data.setdefault("winners", []).append(event_name)
                        changed = True
                        
                    if changed:
                        fixed.append(f"{member.display_name} ({user_id}) - Made winner")
                    else:
                        no_change.append(f"{member.display_name} ({user_id}) - Already a winner")
                        
                elif mod == "notjoined":
                    # Remove from events and winners
                    changed = False
                    if event_name in user_data.get("events", []):
                        user_data["events"].remove(event_name)
                        changed = True
                    if event_name in user_data.get("winners", []):
                        user_data["winners"].remove(event_name)
                        changed = True
                        
                    if changed:
                        fixed.append(f"{member.display_name} ({user_id}) - Removed from event")
                    else:
                        no_change.append(f"{member.display_name} ({user_id}) - Already not joined")
            else:
                not_found.append(id_str)
        except ValueError:
            not_found.append(id_str)

    if fixed:
        save_stats()
        fixed_text = "\n".join(fixed)
        await ctx.send(f"✅ Fixed joined status for event '{event_name}' (mode: {mod}):\n{fixed_text}")
    
    if no_change:
        no_change_text = "\n".join(no_change)
        await ctx.send(f"ℹ️ No changes needed for some users in event '{event_name}':\n{no_change_text}")
    
    if not_found:
        not_found_text = ", ".join(not_found)
        await ctx.send(f"❌ Users not found: {not_found_text}")

@bot.command(name="fixnotjoined")
@admin_only()
async def fix_not_joined(ctx, mod: str, event_name: str, *user_ids: str):
    """Fixes not-joined status issues ('joined' or 'winner')."""
    if not mod or not event_name or not user_ids:
        await ctx.send("❌ Error: Mode, event name, and at least one user ID are required.")
        return
        
    mod = mod.lower()
    if mod not in ["joined", "winner"]:
        await ctx.send("❌ Error: Mode must be 'joined' or 'winner'.")
        return

    fixed = []
    not_found = []
    no_change = []

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            if member:
                user_id_str = str(user_id)
                if user_id_str not in stats_data:
                    stats_data[user_id_str] = DEFAULT_USER_TEMPLATE()
                
                user_data = stats_data[user_id_str]
                
                if mod == "joined":
                    # Make user joined but not winner
                    changed = False
                    if event_name not in user_data.get("events", []):
                        user_data.setdefault("events", []).append(event_name)
                        changed = True
                    if event_name in user_data.get("winners", []):
                        user_data["winners"].remove(event_name)
                        changed = True
                        
                    if changed:
                        fixed.append(f"{member.display_name} ({user_id}) - Made joined (not winner)")
                    else:
                        no_change.append(f"{member.display_name} ({user_id}) - Already joined (not winner)")
                        
                elif mod == "winner":
                    # Make user a winner (implies joined)
                    changed = False
                    if event_name not in user_data.get("events", []):
                        user_data.setdefault("events", []).append(event_name)
                        changed = True
                    if event_name not in user_data.get("winners", []):
                        user_data.setdefault("winners", []).append(event_name)
                        changed = True
                        
                    if changed:
                        fixed.append(f"{member.display_name} ({user_id}) - Made winner")
                    else:
                        no_change.append(f"{member.display_name} ({user_id}) - Already a winner")
            else:
                not_found.append(id_str)
        except ValueError:
            not_found.append(id_str)

    if fixed:
        save_stats()
        fixed_text = "\n".join(fixed)
        await ctx.send(f"✅ Fixed not-joined status for event '{event_name}' (mode: {mod}):\n{fixed_text}")
    
    if no_change:
        no_change_text = "\n".join(no_change)
        await ctx.send(f"ℹ️ No changes needed for some users in event '{event_name}':\n{no_change_text}")
    
    if not_found:
        not_found_text = ", ".join(not_found)
        await ctx.send(f"❌ Users not found: {not_found_text}")

@bot.command(name="bulkban")
@admin_only()
async def bulk_ban(ctx, *args):
    """Bans multiple users by ID with an optional reason."""
    if not args:
        await ctx.send("❌ Error: At least one user ID is required.")
        return
        
    # Check for reason parameter
    reason = "Bulk ban"
    user_ids = list(args)
    for i, arg in enumerate(args):
        if arg.startswith("reason="):
            reason = arg[7:]
            user_ids = list(args[:i])
            break
    
    if not user_ids:
        await ctx.send("❌ Error: At least one user ID is required.")
        return
        
    banned = []
    errors = []
    already_banned = []
    
    await ctx.send(f"⏳ Processing bulk ban for {len(user_ids)} users...")
    
    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            try:
                # Check if already banned
                try:
                    await ctx.guild.fetch_ban(discord.Object(id=user_id))
                    already_banned.append(str(user_id))
                    continue
                except discord.NotFound:
                    pass
                    
                # Attempt to ban
                await ctx.guild.ban(discord.Object(id=user_id), reason=f"{reason} (by {ctx.author})")
                banned.append(str(user_id))
            except discord.Forbidden:
                errors.append(f"{user_id} (Missing Permissions)")
            except discord.HTTPException as e:
                errors.append(f"{user_id} (HTTP Error: {e})")
        except ValueError:
            errors.append(f"{id_str} (Invalid ID)")
    
    # Send results
    if banned:
        banned_count = len(banned)
        await ctx.send(f"✅ Successfully banned {banned_count} users.")
        
        # Send banned IDs in chunks if many
        if banned_count > 20:
            await ctx.send(f"Banned {banned_count} users (too many to list).")
        else:
            banned_text = ", ".join(banned)
            await ctx.send(f"Banned users: {banned_text}")
    
    if already_banned:
        already_count = len(already_banned)
        already_text = ", ".join(already_banned) if already_count <= 20 else f"{already_count} users (too many to list)"
        await ctx.send(f"ℹ️ {already_count} users were already banned: {already_text}")
    
    if errors:
        error_count = len(errors)
        error_text = "\n".join(errors) if error_count <= 10 else f"{error_count} errors (too many to list)"
        await ctx.send(f"❌ Failed to ban {error_count} users:\n{error_text}")

@bot.command(name="bulkgiverole")
@admin_only()
async def bulk_give_role(ctx, role_id: int, *user_ids: str):
    """Gives the specified role to multiple users by ID."""
    if not user_ids:
        await ctx.send("❌ Error: At least one user ID is required.")
        return
        
    role = ctx.guild.get_role(role_id)
    if not role:
        await ctx.send(f"❌ Error: Role with ID {role_id} not found.")
        return
        
    given = []
    errors = []
    already_had = []
    not_found = []
    
    await ctx.send(f"⏳ Giving role {role.name} to {len(user_ids)} users...")
    
    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            member = ctx.guild.get_member(user_id)
            
            if not member:
                not_found.append(str(user_id))
                continue
                
            if role in member.roles:
                already_had.append(f"{member.display_name} ({user_id})")
                continue
                
            try:
                await member.add_roles(role, reason=f"Bulk role assignment by {ctx.author}")
                given.append(f"{member.display_name} ({user_id})")
            except discord.Forbidden:
                errors.append(f"{user_id} (Missing Permissions)")
            except discord.HTTPException as e:
                errors.append(f"{user_id} (HTTP Error: {e})")
        except ValueError:
            errors.append(f"{id_str} (Invalid ID)")
    
    # Send results
    if given:
        given_count = len(given)
        await ctx.send(f"✅ Successfully gave role {role.name} to {given_count} users.")
        
        # Send given IDs in chunks if many
        if given_count > 20:
            await ctx.send(f"Role given to {given_count} users (too many to list).")
        else:
            given_text = "\n".join(given)
            await ctx.send(f"Role given to:\n{given_text}")
    
    if already_had:
        already_count = len(already_had)
        already_text = "\n".join(already_had) if already_count <= 10 else f"{already_count} users (too many to list)"
        await ctx.send(f"ℹ️ {already_count} users already had the role:\n{already_text}")
    
    if not_found:
        not_found_count = len(not_found)
        not_found_text = ", ".join(not_found) if not_found_count <= 20 else f"{not_found_count} users (too many to list)"
        await ctx.send(f"❌ {not_found_count} users not found: {not_found_text}")
    
    if errors:
        error_count = len(errors)
        error_text = "\n".join(errors) if error_count <= 10 else f"{error_count} errors (too many to list)"
        await ctx.send(f"❌ Failed for {error_count} users:\n{error_text}")

# --- EVENT HANDLERS ---

@bot.event
async def on_ready():
    print(f"Bot is ready! Logged in as {bot.user.name} ({bot.user.id})")
    print(f"Connected to {len(bot.guilds)} guilds")
    print(f"Authorized roles: {AUTHORIZED_ROLES}")
    print(f"Target roles: {TARGET_ROLES}")
    print(f"Stats authorized roles: {STATS_AUTHORIZED_ROLES}")
    print(f"Twitter log channel: {twitter_log_channel_id}")

@bot.event
async def on_message(message):
    # Don't process commands in message handler
    if message.author.bot:
        return

    # Process message count for target role users
    if message.guild and any(role.id in TARGET_ROLES for role in message.author.roles):
        user_id = str(message.author.id)
        user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
        user_data["total_message_count"] = user_data.get("total_message_count", 0) + 1
        
        # Check for Twitter/X links if monitoring is enabled
        if twitter_log_channel_id is not None and message.channel.id == twitter_log_channel_id:
            content = message.content.strip()
            url_pattern = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[A-Za-z0-9_]+/status/[0-9]+")
            match = url_pattern.search(content)
            if match and content == match.group(0):  # Only if the message is just the URL
                extracted_url = match.group(0)
                norm_url = extracted_url
                if norm_url.startswith("http://"):
                    norm_url = norm_url.replace("http://", "https://", 1)
                if "://twitter.com/" in norm_url:
                    norm_url = norm_url.replace("://twitter.com/", "://x.com/", 1)
                if "://www.x.com/" in norm_url:
                    norm_url = norm_url.replace("://www.x.com/", "://x.com/", 1)
                
                if norm_url not in posted_links_set:
                    posted_links_set.add(norm_url)
                    user_data.setdefault("twitter_links", []).append(norm_url)
        
        # Save periodically (every 10 messages per user)
        if user_data["total_message_count"] % 10 == 0:
            save_stats()

    # Process commands
    await bot.process_commands(message)

# --- ERROR HANDLING ---

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, CheckFailure):
        return  # Admin-only check failures are handled silently
    elif isinstance(error, MissingRequiredArgument):
        await ctx.send(f"❌ Missing required argument: {error.param.name}")
    elif isinstance(error, BadArgument):
        await ctx.send(f"❌ Invalid argument: {error}")
    elif isinstance(error, CommandError):
        await ctx.send(f"❌ Command error: {error}")
    else:
        print(f"Unhandled error: {error}")
        traceback.print_exc()
        await ctx.send(f"❌ An unexpected error occurred: {error}")

# --- BOT TOKEN ---
# Replace with your actual bot token
bot.run('YOUR_BOT_TOKEN_HERE')
