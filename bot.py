import discord
from discord.ext import commands, tasks
from discord.ext.commands import CheckFailure, MissingRequiredArgument, BadArgument, CommandError, MemberNotFound, RoleNotFound, MissingPermissions
import discord.ui
import json
import os
import xlsxwriter
import datetime
import re
import asyncio
from datetime import time, date, timedelta, timezone
import traceback
import string
from dotenv import load_dotenv
import time as time_module

# --- BOT SETUP ---
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
intents.members = True
intents.voice_states = True

class SilentBot(commands.Bot):
    async def on_command_error(self, ctx, error):
        # Silently delete message on CheckFailure or MissingPermissions errors
        if isinstance(error, commands.CheckFailure) or isinstance(error, commands.MissingPermissions):
            try:
                if ctx.guild:
                    await ctx.message.delete()
            except (discord.Forbidden, discord.HTTPException):
                pass # Ignore if deletion fails
            return # Don't proceed to the global on_command_error for these

        # Delegate other errors to the GlobalErrorHandler Cog
        # Add try-except as Cog might not be loaded before bot is ready
        try:
            cog = self.get_cog('GlobalErrorHandler')
            if cog:
                await cog.on_command_error(ctx, error)
            else: # If Cog not found (could happen at startup), print basic error
                print(f"GlobalErrorHandler Cog not found. Unhandled error: {error}")
                traceback.print_exception(type(error), error, error.__traceback__)
        except Exception as e:
            print(f"Error occurred while calling error handler: {e}")
            traceback.print_exception(type(error), error, error.__traceback__)


bot = SilentBot(command_prefix="!", intents=intents, case_insensitive=True)

# --- CONFIGURATION & DATA ---
STATS_FILE_PATH = "stats.json"
stats_data = {}
config_data = {}
posted_links_list = []
posted_links_set = set()
# << MODIFIED: Store lists of channel IDs >>
twitter_log_channel_ids = [] # List of Twitter log channel IDs
art_channel_ids = []       # List of Art channel IDs
AUTHORIZED_ROLES = [] # Initialize empty, load from config
TARGET_ROLES = [] # Initialize empty, load from config
STATS_AUTHORIZED_ROLES = [] # Initialize empty, load from config
stats_channel_id = None
stats_message_id = None
stats_cooldowns = {}
user_last_stats_click = {}
# << NEW: Track when cooldown message was last sent >>
user_last_cooldown_message_sent = {} # Stores {user_id_str: timestamp}

DEFAULT_USER_TEMPLATE = lambda: {
    "events": [],
    "winners": [],
    "twitter_links": [],
    "total_message_count": 0,
    "art_count": 0 # Art Counter
}

def load_data():
    """Loads statistics and configuration data from the JSON file."""
    global stats_data, config_data, posted_links_list, posted_links_set
    global twitter_log_channel_ids, art_channel_ids, AUTHORIZED_ROLES, TARGET_ROLES, STATS_AUTHORIZED_ROLES
    global stats_channel_id, stats_message_id, stats_cooldowns, user_last_stats_click
    global user_last_cooldown_message_sent # << NEW >>

    if os.path.exists(STATS_FILE_PATH):
        try:
            with open(STATS_FILE_PATH, "r", encoding='utf-8') as f:
                stats_data = json.load(f)
            print(f"'{STATS_FILE_PATH}' loaded successfully.")
        except (json.JSONDecodeError, IOError) as e:
            print(f"ERROR: Failed to load {STATS_FILE_PATH}: {e}. Starting with empty stats.")
            stats_data = {}
    else:
        print(f"'{STATS_FILE_PATH}' not found. Starting with empty stats.")
        stats_data = {}

    # Get config data or create default
    config_data = stats_data.setdefault("config", {})

    # << MODIFIED: Load lists of channel IDs >>
    twitter_log_channel_ids = config_data.get("twitter_log_channel_ids", []) # Load list or default empty
    art_channel_ids = config_data.get("art_channel_ids", [])             # Load list or default empty

    # Use .get with default empty list for roles
    AUTHORIZED_ROLES = config_data.get("track_authorized_roles", [])
    TARGET_ROLES = config_data.get("track_target_roles", [])
    STATS_AUTHORIZED_ROLES = config_data.get("stats_authorized_roles", [])
    stats_channel_id = config_data.get("stats_channel_id")
    stats_message_id = config_data.get("stats_message_id")
    # Ensure keys are strings when loading cooldowns
    stats_cooldowns = {str(k): v for k, v in config_data.get("stats_cooldowns", {}).items()}

    # Load user data
    posted_links_list = stats_data.setdefault("posted_twitter_links", [])
    posted_links_set = set(posted_links_list)
    # Ensure keys are strings when loading user click times
    user_last_stats_click = {str(k): v for k, v in stats_data.setdefault("user_last_stats_click", {}).items()}
    # << NEW: Load cooldown message sent times >>
    user_last_cooldown_message_sent = {str(k): v for k, v in stats_data.setdefault("user_last_cooldown_message_sent", {}).items()}


    # Check/update existing users for the new art_count field
    for uid, data in stats_data.items():
        # Skip special keys like 'config', process only user IDs
        if uid.isdigit() and isinstance(data, dict):
            data.setdefault("art_count", 0)


load_data() # Load data when the bot starts

# --- HELPER FUNCTIONS ---
def save_stats():
    """Saves the current statistics and configuration data to the JSON file."""
    global twitter_log_channel_ids, art_channel_ids, stats_channel_id, stats_message_id
    global user_last_cooldown_message_sent # << NEW >>
    try:
        # Update config data (taking from global variables)
        # << MODIFIED: Save lists of channel IDs >>
        config_data["twitter_log_channel_ids"] = twitter_log_channel_ids
        config_data["art_channel_ids"] = art_channel_ids
        config_data["track_authorized_roles"] = AUTHORIZED_ROLES
        config_data["track_target_roles"] = TARGET_ROLES
        config_data["stats_authorized_roles"] = STATS_AUTHORIZED_ROLES
        config_data["stats_channel_id"] = stats_channel_id
        config_data["stats_message_id"] = stats_message_id
        # Ensure keys are strings when saving cooldowns
        config_data["stats_cooldowns"] = {str(k): v for k, v in stats_cooldowns.items()}

        # Add config and other lists/dicts to the main stats_data
        stats_data["config"] = config_data
        stats_data["posted_twitter_links"] = sorted(list(posted_links_set))
        # Ensure keys are strings when saving user click times
        stats_data["user_last_stats_click"] = {str(k): v for k, v in user_last_stats_click.items()}
        # << NEW: Save cooldown message sent times >>
        stats_data["user_last_cooldown_message_sent"] = {str(k): v for k, v in user_last_cooldown_message_sent.items()}


        # Ensure all user data has art_count before saving (safety check)
        for uid, data in stats_data.items():
            if uid.isdigit() and isinstance(data, dict):
                data.setdefault("art_count", 0)

        # Write to file
        with open(STATS_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(stats_data, f, indent=2, ensure_ascii=False) # ensure_ascii=False for non-ASCII chars
        # print("Stats saved successfully.") # For debugging
    except IOError as e:
        print(f"ERROR: Could not save stats: {e}")
    except Exception as e:
        print(f"UNEXPECTED ERROR (save_stats): {e}")
        traceback.print_exc()

def admin_only():
    """Decorator check for authorized roles or bot owner."""
    async def predicate(ctx):
        if ctx.guild is None: return False # Doesn't work in DMs
        # Bot owner is always authorized
        if await bot.is_owner(ctx.author):
            return True
        # Authorized if user has one of the roles in AUTHORIZED_ROLES
        # Ensure ctx.author.roles exists (usually does in guild context)
        if isinstance(ctx.author, discord.Member) and any(role.id in AUTHORIZED_ROLES for role in ctx.author.roles):
            return True
        # If not authorized, raise CheckFailure (SilentBot might delete message)
        raise commands.CheckFailure("User is not an admin or does not have authorized roles.")
    return commands.check(predicate)


async def is_admin(member: discord.Member) -> bool:
    """Checks if a member is the bot owner or has an authorized role."""
    if not isinstance(member, discord.Member): # Cannot check roles for User object (e.g., in DMs)
        return False
    if await bot.is_owner(member):
        return True
    # Check if roles exist (member might have left)
    if hasattr(member, 'roles') and any(role.id in AUTHORIZED_ROLES for role in member.roles):
        return True
    return False

# << MODIFIED: stats_authorized_check - Removed bot owner check >>
async def stats_authorized_check(ctx):
    """
    Checks if the user is authorized to use the !stats command.
    Authorized if: has track authorized role OR has stats authorized role (if defined).
    Bot owner check is removed.
    """
    # Cannot check roles in DMs
    if not isinstance(ctx.author, discord.Member):
        return False

    # Check if user has general authorized roles
    if any(role.id in AUTHORIZED_ROLES for role in ctx.author.roles):
        return True

    # If stats role list is not empty, check if user has one of those roles
    if STATS_AUTHORIZED_ROLES:
        if any(role.id in STATS_AUTHORIZED_ROLES for role in ctx.author.roles):
            return True

    # If none of the above, user is not authorized
    return False

def standardize_event_name(name):
    """Trims and converts event name to lowercase."""
    return name.strip().lower()

def sanitize_filename(name):
    """Removes/replaces invalid filename characters."""
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in name if c in valid_chars)
    filename = filename.replace(' ', '_')
    # Replace multiple underscores with single (optional)
    filename = re.sub('_+', '_', filename)
    if not filename:
        filename = "event_list" # English default
    return filename[:100] # Truncate long names

def chunk_text_by_size(text, size=1024):
    """Splits text into chunks of up to 'size' characters, respecting lines."""
    lines = text.splitlines()
    chunks = []
    current_chunk = ""
    for line in lines:
        # If current chunk is empty or adding the new line doesn't exceed size
        if not current_chunk or len(current_chunk) + len(line) + 1 <= size:
            # Add newline before if not the first line in the chunk
            if current_chunk:
                current_chunk += "\n" + line
            else:
                current_chunk = line
        else:
            # Current chunk is full, add to list and start new chunk
            chunks.append(current_chunk)
            current_chunk = line
    # Add the last remaining chunk
    if current_chunk:
        chunks.append(current_chunk)

    # If no chunks, return a list with an empty string (embed fields cannot be None)
    return chunks if chunks else [""] # Or []? "" is safer for embed fields.

def parse_cooldown_duration(duration_str: str) -> int | None:
    """Parses duration strings like '5m', '1h', '2d', '0' into seconds."""
    duration_str = duration_str.lower().strip()
    if duration_str == '0': return 0 # No cooldown
    match = re.match(r"^(\d+)\s*([mhd])$", duration_str) # Allow optional space
    if not match: return None # Invalid format
    value, unit = match.groups()
    try:
        value = int(value)
        if value < 0: return None # Negative duration is invalid
    except ValueError: return None # Should not happen with regex, but safety check
    if unit == 'm': return value * 60
    elif unit == 'h': return value * 3600
    elif unit == 'd': return value * 86400
    else: return None # Should not happen

# Updated filter parser to handle both simple and range filters
def parse_numeric_filter(filter_str):
    """Parses numeric filter strings (e.g., msgcount>100, 5<artcount<10)."""
    filter_str = filter_str.strip()
    # Regex for simple filters (keyword op value)
    simple_pattern = re.compile(r"(?i)^(msgcount|twtcount|joined|won|artcount)\s*(!=|>=|<=|>|<|=)\s*(\d+)$")
    # Regex for range filters (value1 <[=] keyword <[=] value2) - currently only for artcount
    # Makes range operators flexible: 5<artcount<10, 5<=artcount<10, 5<artcount<=10, 5<=artcount<=10
    range_pattern = re.compile(r"(?i)^(\d+)\s*(<|<=)\s*(artcount)\s*(<|<=)\s*(\d+)$")

    # Try simple pattern first
    m_simple = simple_pattern.match(filter_str)
    if m_simple:
        keyword, operator, value_str = m_simple.groups()
        try:
            value = int(value_str)
            # Field name mapping (art_count added)
            field_map = {"msgcount": "total_message_count", "twtcount": "tweet_count",
                         "joined": "joined", "won": "won", "artcount": "art_count"}
            field = field_map.get(keyword.lower())
            if not field: return None
            return (field, operator, value) # Return tuple for simple filter
        except ValueError: return None

    # If simple pattern fails, try range pattern
    m_range = range_pattern.match(filter_str)
    if m_range:
        val1_str, op1_raw, keyword, op2_raw, val2_str = m_range.groups()
        try:
            val1 = int(val1_str)
            val2 = int(val2_str)
            # Only allow range for artcount for now
            if keyword.lower() != "artcount": return None

            # Convert operators (e.g., 5 < artcount becomes artcount > 5)
            # to match Python's comparison operators
            lower_op = ">=" if op1_raw == "<=" else ">"
            upper_op = op2_raw # Stays as "<" or "<="

            # Ensure lower bound is less than upper bound
            if val1 >= val2: return None # Invalid range

            # Return 5-element tuple for range filter: (field, lower_op, lower_val, upper_op, upper_val)
            return ("art_count", lower_op, val1, upper_op, val2)
        except ValueError: return None

    return None # No match found

def generate_excel(guild, filters=None, sort_key=None):
    """Generates an Excel report with filters."""
    excel_filename = f"{sanitize_filename(guild.name)}_statistics_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx" # English filename part
    if os.path.exists(excel_filename):
        try: os.remove(excel_filename)
        except OSError as e:
            print(f"Could not delete existing Excel: {excel_filename}, Error: {e}") # English comment
            return None

    numeric_filters = filters.get("numeric_filters", []) if filters else [] # Contains both simple and range filters
    role_filter_object = filters.get("role_filter") if filters else None
    not_have_role_object = filters.get("nothaverole") if filters else None

    event_list_set = set()
    user_data_for_excel = []
    processed_user_ids = set()
    current_stats_data = stats_data.copy() # Work on a copy of stats_data for safety

    # Operator mapping
    op_map = {">": lambda a, b: a > b, "<": lambda a, b: a < b, ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b, "=": lambda a, b: a == b, "!=": lambda a, b: a != b}

    for user_id_str, data in current_stats_data.items():
        # Process only user IDs (skip config, etc.)
        if not user_id_str.isdigit() or not isinstance(data, dict) or user_id_str in processed_user_ids:
            continue
        try: user_id = int(user_id_str)
        except ValueError: continue

        member = guild.get_member(user_id)
        # Skip if member not in server or (if target roles defined) doesn't have target role
        if not member or (TARGET_ROLES and not any(role.id in TARGET_ROLES for role in member.roles)):
            continue

        # --- Apply Filters ---
        pass_filters = True
        # Role Filters
        if role_filter_object and (role_filter_object not in member.roles): pass_filters = False
        if not_have_role_object and (not_have_role_object in member.roles): pass_filters = False

        # Numeric Filters (Simple and Range)
        if pass_filters:
            for filt in numeric_filters:
                user_val = 0
                field = filt[0] # Field name is always the first element

                # Get user's value for the field
                if field == "total_message_count": user_val = data.get("total_message_count", 0)
                elif field == "tweet_count": user_val = len(data.get("twitter_links", []))
                elif field == "joined": user_val = len(data.get("events", []))
                elif field == "won": user_val = len(data.get("winners", []))
                elif field == "art_count": user_val = data.get("art_count", 0) # Get art count

                # Apply filter based on tuple length
                if len(filt) == 3: # Simple filter: (field, op, value)
                    _, operator, filter_value = filt
                    if operator not in op_map or not op_map[operator](user_val, filter_value):
                        pass_filters = False; break
                elif len(filt) == 5: # Range filter: (field, lower_op, lower_val, upper_op, upper_val)
                    _, lower_op, lower_val, upper_op, upper_val = filt
                    # Check lower bound
                    if lower_op not in op_map or not op_map[lower_op](user_val, lower_val):
                        pass_filters = False; break
                    # Check upper bound
                    if upper_op not in op_map or not op_map[upper_op](user_val, upper_val):
                        pass_filters = False; break

        if not pass_filters: continue # Skip user if any filter fails
        # --- Filters End ---

        processed_user_ids.add(user_id_str) # Mark this user as processed

        # Collect all unique event names (joined or won)
        all_user_events = set(data.get("events", [])) | set(data.get("winners", []))
        event_list_set.update(standardize_event_name(e) for e in all_user_events)

        # Get and format user roles
        role_names = sorted([r.name for r in member.roles if r.name != "@everyone"])

        # Prepare data for Excel row
        user_data_for_excel.append({
            "member": member, "user_id": user_id_str, "roles": ", ".join(role_names),
            "joined_count": len(data.get("events", [])), "won_count": len(data.get("winners", [])),
            "total_message_count": data.get("total_message_count", 0),
            "tweet_count": len(data.get("twitter_links", [])),
            "art_count": data.get("art_count", 0), # Add art count
            "twitter_links_list": data.get("twitter_links", []), "raw_data": data # Raw data for event statuses
        })

    # Sort all unique event names found (for Excel columns)
    sorted_event_list = sorted(list(event_list_set))
    print(f"{len(user_data_for_excel)} members passed filters. Sorting...") # English comment

    # Sort the data
    if sort_key == "messages": user_data_for_excel.sort(key=lambda x: (-x["total_message_count"], x["member"].display_name.lower()))
    elif sort_key == "tweets": user_data_for_excel.sort(key=lambda x: (-x["tweet_count"], x["member"].display_name.lower()))
    # Sorting by art_count could be added:
    # elif sort_key == "artcount": user_data_for_excel.sort(key=lambda x: (-x["art_count"], x["member"].display_name.lower()))
    else: # Default sort by display name
        user_data_for_excel.sort(key=lambda x: x["member"].display_name.lower())

    print("Writing Excel file...") # English comment
    try:
        workbook = xlsxwriter.Workbook(excel_filename)
        sheet = workbook.add_worksheet("Statistics") # English sheet name

        # Headers (English headers)
        headers = ["User Name", "User ID", "Roles", "Joined Events Count", "Won Events Count", "Total Messages Sent", "Tweet Count", "Art Count"] + sorted_event_list + ["Posted Twitter Links"] # English headers
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        header_map = {head: idx for idx, head in enumerate(headers)} # Map header names to indices
        for col, head in enumerate(headers): sheet.write(0, col, head, header_format)

        # Cell Formats
        row_format = workbook.add_format({'valign': 'top', 'border': 1}) # Basic format
        wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top', 'border': 1}) # Wrap text
        num_format = workbook.add_format({'valign': 'top', 'num_format': '#,##0', 'border': 1}) # Number format
        center_format = workbook.add_format({'align': 'center', 'valign': 'top', 'border': 1}) # Center align
        url_format = workbook.add_format({'font_color': 'blue', 'underline': 1, 'valign': 'top', 'text_wrap': True, 'border': 1}) # URL format

        # Write data rows
        for row_idx, user_info in enumerate(user_data_for_excel):
            row = row_idx + 1 # Excel rows start at 1
            data = user_info["raw_data"]
            member = user_info["member"]

            # Write basic info (using English header keys)
            sheet.write(row, header_map["User Name"], member.display_name, row_format)
            sheet.write_string(row, header_map["User ID"], user_info["user_id"], row_format) # Write ID as text
            sheet.write(row, header_map["Roles"], user_info["roles"], wrap_format)
            sheet.write(row, header_map["Joined Events Count"], user_info["joined_count"], num_format)
            sheet.write(row, header_map["Won Events Count"], user_info["won_count"], num_format)
            sheet.write(row, header_map["Total Messages Sent"], user_info["total_message_count"], num_format)
            sheet.write(row, header_map["Tweet Count"], user_info["tweet_count"], num_format)
            sheet.write(row, header_map["Art Count"], user_info["art_count"], num_format) # Write art count

            # Event participation status (✅/🏆)
            user_events_std = {standardize_event_name(e) for e in data.get("events", [])}
            user_winners_std = {standardize_event_name(w) for w in data.get("winners", [])}
            for event_name_std in sorted_event_list:
                event_col = header_map.get(event_name_std) # Get index from map
                if event_col is not None:
                    status = ""
                    if event_name_std in user_winners_std: status = "🏆" # Won
                    elif event_name_std in user_events_std: status = "✅" # Joined
                    sheet.write(row, event_col, status, center_format) # Center aligned format

            # Twitter links (using English header key)
            tw_col_idx = header_map.get("Posted Twitter Links") # Use index
            if tw_col_idx is not None:
                joined_links = "\n".join(user_info["twitter_links_list"])
                # Write links as a string but use the URL format for visual indication
                # write_url doesn't work well for multiple links in one cell.
                sheet.write_string(row, tw_col_idx, joined_links, url_format) # Use URL format here

        # Set column widths (using English header map keys)
        sheet.set_column(header_map["User Name"], header_map["User Name"], 25)
        sheet.set_column(header_map["User ID"], header_map["User ID"], 20)
        sheet.set_column(header_map["Roles"], header_map["Roles"], 35)
        sheet.set_column(header_map["Joined Events Count"], header_map["Joined Events Count"], 18)
        sheet.set_column(header_map["Won Events Count"], header_map["Won Events Count"], 18)
        sheet.set_column(header_map["Total Messages Sent"], header_map["Total Messages Sent"], 20)
        sheet.set_column(header_map["Tweet Count"], header_map["Tweet Count"], 15)
        sheet.set_column(header_map["Art Count"], header_map["Art Count"], 15) # Art Count column width
        # Event columns (if any)
        if sorted_event_list:
            first_event_col = header_map.get(sorted_event_list[0])
            last_event_col = header_map.get(sorted_event_list[-1])
            if first_event_col is not None and last_event_col is not None:
                sheet.set_column(first_event_col, last_event_col, 15) # Width for all event columns
        # Twitter link column (if exists)
        tw_col_idx = header_map.get("Posted Twitter Links")
        if tw_col_idx is not None:
            sheet.set_column(tw_col_idx, tw_col_idx, 50) # Wider for links

        # Close the workbook (saves to disk)
        workbook.close()
        print(f"Excel '{excel_filename}' generated ({guild.name}). {len(user_data_for_excel)} members processed.") # English comment
        return excel_filename # Return filename on success
    except Exception as e:
        print(f"ERROR writing Excel: {e}") # English comment
        traceback.print_exc()
        # Try to close workbook and delete potentially corrupt file on error
        if 'workbook' in locals() and workbook:
            try: workbook.close()
            except: pass
        if os.path.exists(excel_filename):
            try: os.remove(excel_filename)
            except: pass
        return None # Return None on failure

async def generate_user_stats_embeds(member: discord.Member) -> list[discord.Embed]:
    """Generates statistics embeds for a given member."""
    user_id = str(member.id)
    embeds = []

    # Ensure user data exists and has art_count using setdefault
    user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
    # Ensure art_count exists for data loaded before this field was added
    user_data.setdefault("art_count", 0)

    # Check if all tracked stats are zero or empty lists
    # Show "no data" message if all values are 0 or empty
    if not any(user_data.get(key) for key in ["events", "winners", "twitter_links"]) and \
       user_data.get("total_message_count", 0) == 0 and \
       user_data.get("art_count", 0) == 0:
        embed = discord.Embed(title=f"📊 No Statistics Found for {member.display_name}", description="No data has been recorded for this user yet.", color=discord.Color.orange()) # English text
        embed.set_thumbnail(url=member.display_avatar.url)
        embeds.append(embed)
        return embeds

    # Get data
    events_joined = user_data.get("events", [])
    events_won = user_data.get("winners", [])
    twitter_links = user_data.get("twitter_links", [])
    total_message_count = user_data.get("total_message_count", 0)
    art_count = user_data.get("art_count", 0) # Get art count

    # Get and format user roles (excluding @everyone, sorted descending by ID)
    member_roles = sorted(
        [role.mention for role in member.roles if role.name != "@everyone"],
        key=lambda mention: int(mention.strip('<@&>')), # Sort by ID
        reverse=True # Higher roles usually preferred at the top
    )
    roles_text = ", ".join(member_roles) if member_roles else "No Roles" # English text

    # Main statistics embed (English field names)
    embed = discord.Embed(
        title=f"📊 Statistics for {member.display_name}", # English title
        color=member.color if member.color.value != 0 else discord.Color.blue() # Use member's color
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="User ID", value=user_id, inline=True)
    embed.add_field(name="Total Messages", value=f"{total_message_count:,}", inline=True)
    embed.add_field(name="Tweet Count", value=str(len(twitter_links)), inline=True)
    embed.add_field(name="Events Joined", value=str(len(events_joined)), inline=True)
    embed.add_field(name="Events Won", value=str(len(events_won)), inline=True)
    embed.add_field(name="Art Count", value=str(art_count), inline=True) # Art count field

    # Add roles (with chunking)
    role_chunks = chunk_text_by_size(roles_text, 1024) # Embed field value limit 1024
    for i, chunk in enumerate(role_chunks):
        embed.add_field(
            name="Roles" if i == 0 else "Roles (cont.)", # English text
            value=chunk if chunk else " ", # Use space if chunk is empty
            inline=False # Roles often look better on a separate line
        )

    # Add joined events (with chunking)
    if events_joined:
        # Remove duplicates and sort
        events_text = ", ".join(sorted(list(set(events_joined))))
        chunks = chunk_text_by_size(events_text, 1024)
        for i, chunk in enumerate(chunks):
            embed.add_field(
                name="Joined Events" if i == 0 else "Joined Events (cont.)", # English text
                value=chunk if chunk else " ",
                inline=False
            )

    # Add won events (with chunking)
    if events_won:
        # Remove duplicates and sort
        events_text = ", ".join(sorted(list(set(events_won))))
        chunks = chunk_text_by_size(events_text, 1024)
        for i, chunk in enumerate(chunks):
            embed.add_field(
                name="Won Events" if i == 0 else "Won Events (cont.)", # English text
                value=chunk if chunk else " ",
                inline=False
            )

    embeds.append(embed)

    # Split Twitter links into separate embeds if necessary
    if twitter_links:
        MAX_LINKS_PER_EMBED = 10 # How many links per embed?
        num_link_embeds = (len(twitter_links) + MAX_LINKS_PER_EMBED - 1) // MAX_LINKS_PER_EMBED

        for i in range(num_link_embeds):
            start_idx = i * MAX_LINKS_PER_EMBED
            end_idx = min((i + 1) * MAX_LINKS_PER_EMBED, len(twitter_links))

            # Create the link embed
            links_embed = discord.Embed(
                title=f"🔗 {member.display_name} Twitter Links ({i+1}/{num_link_embeds})", # English title part
                color=member.color if member.color.value != 0 else discord.Color.blue()
            )

            # Add numbered links
            link_texts = []
            for j, link in enumerate(twitter_links[start_idx:end_idx], 1):
                link_texts.append(f"{start_idx + j}. {link}")

            # Add all links for this embed to the description (more compact)
            links_value = "\n".join(link_texts)
            if len(links_value) > 4096: # Embed description limit
                 links_value = links_value[:4090] + "..."
            links_embed.description = links_value

            embeds.append(links_embed)

    return embeds

class StatsView(discord.ui.View):
    """A persistent view with the 'Show My Stats' button."""
    def __init__(self):
        # timeout=None makes the view persistent
        super().__init__(timeout=None)

    @discord.ui.button(label="Show My Stats", style=discord.ButtonStyle.primary, custom_id="show_my_stats_button") # English label
    async def show_stats_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Callback for the stats button."""
        user = interaction.user
        user_id_str = str(user.id)
        current_time = time_module.time() # Current time as Unix timestamp

        # --- Cooldown Check ---
        # Ensure user is a Member object to access roles
        if not isinstance(user, discord.Member):
             # Should not happen with button clicks in guilds, but safety check
             await interaction.response.send_message("Could not retrieve your roles.", ephemeral=True) # English text
             return

        user_roles_ids = {str(role.id) for role in user.roles} # User's role IDs as strings
        shortest_cooldown = float('inf') # Start with infinity
        has_specific_cooldown = False

        # Check each role defined in stats_cooldowns
        for role_id_str, cooldown_sec in stats_cooldowns.items():
            if role_id_str in user_roles_ids:
                # Make sure cooldown_sec is an int/float
                try:
                    current_role_cooldown = int(cooldown_sec)
                    if current_role_cooldown >= 0: # Ignore negative values
                         has_specific_cooldown = True
                         shortest_cooldown = min(shortest_cooldown, current_role_cooldown)
                except (ValueError, TypeError):
                     print(f"Warning: Invalid cooldown value ({cooldown_sec}) found for role ID {role_id_str}.") # English comment
                     continue # Skip invalid cooldown values

        # Use the shortest cooldown found if any specific one applies
        applicable_cooldown = shortest_cooldown if has_specific_cooldown and shortest_cooldown != float('inf') else 0

        # Get the user's last click time
        last_click_time = user_last_stats_click.get(user_id_str, 0.0) # Default to 0.0 if not found
        time_elapsed = current_time - last_click_time

        # Check if cooldown is active
        if applicable_cooldown > 0 and time_elapsed < applicable_cooldown:
            # << NEW: Cooldown Message Spam Prevention >>
            last_cooldown_msg_time = user_last_cooldown_message_sent.get(user_id_str, 0.0) # When msg was last sent

            # If a cooldown message was already sent *during this specific cooldown period*
            if last_cooldown_msg_time >= last_click_time:
                # Acknowledge interaction silently to prevent "Interaction failed"
                try:
                    await interaction.response.defer(ephemeral=True)
                except discord.NotFound: # Interaction might expire quickly
                    pass
                except Exception as e:
                    print(f"Error deferring interaction during cooldown msg check: {e}")
                return # Don't send another message

            # --- Send Cooldown Message (First time for this cooldown) ---
            remaining_time = applicable_cooldown - time_elapsed
            # Format remaining time for better readability
            if remaining_time > 3600: time_str = f"{remaining_time / 3600:.1f} hours"
            elif remaining_time > 60: time_str = f"{remaining_time / 60:.1f} minutes"
            else: time_str = f"{remaining_time:.0f} seconds"

            try:
                await interaction.response.send_message(
                    f"❌ You need to wait {time_str} more to use this button again.",
                    ephemeral=True # Only visible to the user
                )
                # Record that the message was sent now
                user_last_cooldown_message_sent[user_id_str] = current_time
                save_stats() # Save the updated message sent time
            except discord.NotFound: # Interaction might expire
                pass
            except Exception as e:
                print(f"Error sending cooldown message or saving state: {e}")
            return # Stop processing further

        # --- Proceed if cooldown check passed ---
        # Reset the cooldown message sent time if the user successfully uses the button
        # Do this *before* potentially long stat generation
        needs_save_after_stats = False
        if user_id_str in user_last_cooldown_message_sent:
            del user_last_cooldown_message_sent[user_id_str]
            needs_save_after_stats = True # Mark that save is needed later

        try:
            # Defer the interaction first, especially if generating stats takes time
            await interaction.response.defer(ephemeral=True, thinking=True)

            # Generate the stats embeds
            stats_embeds = await generate_user_stats_embeds(user)

            # Discord embed limit is 6000 characters
            # Send embeds in multiple messages if needed
            MAX_EMBEDS_PER_MESSAGE = 10  # Limit embeds per message
            MAX_CHARS_PER_MESSAGE = 5000  # Keep below Discord's 6000 char limit to be safe

            if len(stats_embeds) <= MAX_EMBEDS_PER_MESSAGE:
                # Calculate total characters in all embeds
                total_chars = sum(len(embed.title or "") + 
                                 len(embed.description or "") + 
                                 sum(len(field.name) + len(field.value) for field in embed.fields)
                                 for embed in stats_embeds)
                
                # If total characters are within limit, send all embeds at once
                if total_chars <= MAX_CHARS_PER_MESSAGE:
                    await interaction.followup.send(embeds=stats_embeds, ephemeral=True)
                else:
                    # Otherwise send embeds one by one
                    for i, embed in enumerate(stats_embeds):
                        await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                # If there are too many embeds, split them into multiple messages
                for i in range(0, len(stats_embeds), MAX_EMBEDS_PER_MESSAGE):
                    chunk = stats_embeds[i:i+MAX_EMBEDS_PER_MESSAGE]
                    # Calculate characters in this chunk
                    chunk_chars = sum(len(embed.title or "") + 
                                    len(embed.description or "") + 
                                    sum(len(field.name) + len(field.value) for field in embed.fields)
                                    for embed in chunk)
                    
                    if chunk_chars <= MAX_CHARS_PER_MESSAGE:
                        # Send chunk if within character limit
                        await interaction.followup.send(embeds=chunk, ephemeral=True)
                    else:
                        # Otherwise send embeds in this chunk one by one
                        for embed in chunk:
                            await interaction.followup.send(embed=embed, ephemeral=True)

            # Update the last click time
            user_last_stats_click[user_id_str] = current_time
            # Save stats (includes updated click time and potentially cleared cooldown msg state)
            save_stats()

        except Exception as e:
            print(f"ERROR (Stats Button Callback - Post Cooldown): {e}")
            traceback.print_exc()
            try:
                # Try to send an error message as followup
                if not interaction.is_expired():
                     await interaction.followup.send("❌ An error occurred while fetching your statistics.", ephemeral=True) # English text
            except Exception as inner_e:
                 print(f"ERROR (Stats Button Error Send): {inner_e}") # English comment


# --- ROLE MANAGEMENT COMMANDS ---
# (User messages translated to English)
@bot.command(name="settrackauthorizedrole")
@admin_only()
async def set_track_authorized_role(ctx, role: discord.Role):
    global AUTHORIZED_ROLES
    if role.id not in AUTHORIZED_ROLES:
        AUTHORIZED_ROLES.append(role.id)
        save_stats()
        await ctx.send(f"✅ Role {role.mention} has been added to track authorized roles.") # English text
    else:
        await ctx.send(f"ℹ️ Role {role.mention} is already set as a track authorized role.") # English text

@bot.command(name="settracktargetrole")
@admin_only()
async def set_track_target_role(ctx, *roles: discord.Role):
    global TARGET_ROLES
    added_roles = []
    if not roles: return await ctx.send("❌ Error: You must specify at least one role.") # English text
    for role in roles:
        if role.id not in TARGET_ROLES:
            TARGET_ROLES.append(role.id)
            added_roles.append(role.mention)
    if added_roles:
        save_stats()
        await ctx.send(f"✅ Added to track target roles: {', '.join(added_roles)}.") # English text
    else:
        await ctx.send("ℹ️ The specified roles were already target roles.") # English text

@bot.command(name="removeauthorizedrole")
@admin_only()
async def remove_authorized_role(ctx, *roles: discord.Role):
    global AUTHORIZED_ROLES
    removed = []
    if not roles: return await ctx.send("❌ Error: You must specify at least one role.") # English text
    for role in roles:
        if role.id in AUTHORIZED_ROLES:
            try:
                AUTHORIZED_ROLES.remove(role.id)
                removed.append(role.mention)
            except ValueError: pass
    if removed:
        save_stats()
        await ctx.send(f"✅ Removed from track authorized roles: {', '.join(removed)}.") # English text
    else:
        await ctx.send("ℹ️ No matching authorized roles found to remove.") # English text

@bot.command(name="removetargetrole")
@admin_only()
async def remove_target_role(ctx, *roles: discord.Role):
    global TARGET_ROLES
    removed = []
    if not roles: return await ctx.send("❌ Error: You must specify at least one role.") # English text
    for role in roles:
        if role.id in TARGET_ROLES:
            try:
                TARGET_ROLES.remove(role.id)
                removed.append(role.mention)
            except ValueError: pass
    if removed:
        save_stats()
        await ctx.send(f"✅ Removed from track target roles: {', '.join(removed)}.") # English text
    else:
        await ctx.send(f"ℹ️ The specified roles were not set as track target roles.") # English text

@bot.command(name="setstatsroleauthorized")
@admin_only()
async def set_stats_role_authorized(ctx, *roles: discord.Role):
    global STATS_AUTHORIZED_ROLES
    added = []
    if not roles: return await ctx.send("❌ Error: You must specify at least one role.") # English text
    for role in roles:
        if role.id not in STATS_AUTHORIZED_ROLES:
            STATS_AUTHORIZED_ROLES.append(role.id)
            added.append(role.mention)
    if added:
        save_stats()
        await ctx.send(f"✅ Added to stats authorized roles: {', '.join(added)}.") # English text
    else:
        await ctx.send("ℹ️ The specified roles were already authorized for `!stats`.") # English text

@bot.command(name="removestatsroleauthorized")
@admin_only()
async def remove_stats_role_authorized(ctx, *roles: discord.Role):
    global STATS_AUTHORIZED_ROLES
    removed = []
    if not roles: return await ctx.send("❌ Error: You must specify at least one role.") # English text
    for role in roles:
        if role.id in STATS_AUTHORIZED_ROLES:
            try:
                STATS_AUTHORIZED_ROLES.remove(role.id)
                removed.append(role.mention)
            except ValueError: pass
    if removed:
        save_stats()
        await ctx.send(f"✅ Removed from stats authorized roles: {', '.join(removed)}.") # English text
    else:
        await ctx.send(f"ℹ️ The specified roles were not set as stats authorized roles.") # English text

# --- COMMANDS ---
# << MODIFIED: Command to add an art channel >>
@bot.command(name="setartchannel", aliases=["addartchannel"]) # Added alias
@admin_only()
async def add_art_channel(ctx, channel: discord.TextChannel):
    """Adds a channel to the list of monitored Art Channels and scans its history."""
    global art_channel_ids, stats_data # << MODIFIED: Use list variable >>

    if not channel:
        return await ctx.send("❌ Error: Valid text channel not found.")

    if channel.id in art_channel_ids:
        return await ctx.send(f"ℹ️ {channel.mention} is already in the list of art channels.")

    # Add the channel to the list
    art_channel_ids.append(channel.id)
    save_stats() # Save the updated list first
    await ctx.send(f"✅ Added {channel.mention} to the list of Art Channels. Only media posts without text (for non-admins) will be allowed and counted in this channel.")

    # --- History Scan (Only runs for the newly added channel) ---
    scan_msg = await ctx.send(f"⏳ Scanning {channel.mention}'s history (up to 10k messages) for past art posts...")
    history_limit = 10000 # <<< YOU CAN CHANGE THIS NUMBER >>>
    history_posts_found_count = 0 # Total posts found in scan
    history_scan_users_updated = 0 # Users whose count was updated by scan
    history_changed = False # Flag if any user's count potentially changed
    scanned_users_art = {} # Track counts per user *during this scan*

    try:
        processed_messages = 0
        async for message in channel.history(limit=history_limit):
            processed_messages += 1
            # Ignore bots
            if message.author.bot:
                continue

            # Check if it's a valid media post
            has_media = bool(message.attachments or message.embeds)
            has_text = bool(message.content.strip())
            is_valid_art_post = has_media and not has_text

            if is_valid_art_post:
                # Check if author should be tracked
                author_member = message.author # History messages usually have Member
                author_id_str = str(message.author.id)
                should_track_author = False
                if isinstance(author_member, discord.Member) and TARGET_ROLES:
                    if any(role.id in TARGET_ROLES for role in author_member.roles):
                        should_track_author = True
                elif isinstance(author_member, discord.Member) and not TARGET_ROLES: # Track everyone if no target roles
                    should_track_author = True

                if should_track_author:
                    # Increment count for this user IN THIS SCAN
                    scanned_users_art[author_id_str] = scanned_users_art.get(author_id_str, 0) + 1
                    history_posts_found_count += 1 # Count total posts found in scan
                    history_changed = True # Mark that data *might* need updating

        # --- Update stats_data AFTER scan is complete using max() ---
        if history_changed:
            print(f"Art History Scan ({channel.name}): Found {history_posts_found_count} potential posts by {len(scanned_users_art)} users.")
            data_actually_changed = False # Flag if save_stats is really needed
            # Update the main stats_data with the counts found during the scan, ensuring we don't decrease the count
            for user_id_str, scanned_count in scanned_users_art.items():
                user_data = stats_data.setdefault(user_id_str, DEFAULT_USER_TEMPLATE())
                current_count = user_data.get("art_count", 0)
                # Set the count to the maximum of the current count and the count found in the scan
                new_count = max(current_count, scanned_count)
                if new_count != current_count:
                    user_data["art_count"] = new_count
                    print(f"Updated art_count for {user_id_str} from {current_count} to {new_count} based on history scan (used max).")
                    history_scan_users_updated += 1
                    data_actually_changed = True # Mark that a save is needed

            if data_actually_changed:
                save_stats() # Save stats only if counts were actually updated
                await scan_msg.edit(content=f"✅ Added {channel.mention} to Art Channels.\n✅ History scan complete. Updated art counts for {history_scan_users_updated} users based on {history_posts_found_count} past posts found (preserving existing count).")
            else:
                 await scan_msg.edit(content=f"✅ Added {channel.mention} to Art Channels.\n✅ History scan complete. Posts found in scan did not change existing counts.")

        else:
            await scan_msg.edit(content=f"✅ Added {channel.mention} to Art Channels.\n✅ History scan complete. No past art posts by tracked users found or counted in this channel.")

    except discord.Forbidden:
        await scan_msg.edit(content=f"❌ Error scanning history: Missing permission to read history in {channel.mention}.")
    except Exception as e:
        await scan_msg.edit(content=f"❌ An error occurred during history scan for {channel.mention}: {e}")
        print(f"Error during art channel history scan ({channel.name}): {e}")
        traceback.print_exc()

@add_art_channel.error # << MODIFIED: Error handler for the add command >>
async def add_art_channel_error(ctx, error):
    """Error handler for addartchannel command."""
    if isinstance(error, commands.ChannelNotFound):
        await ctx.send(f"❌ Error: Channel not found. Please provide a valid channel ID or #channel mention.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Error: Please provide a valid text channel ID or #channel mention.")
    elif isinstance(error, MissingRequiredArgument):
        await ctx.send("❌ Error: Channel ID or #channel mention is required. Usage: `!setartchannel <#channel or ID>`")
    elif isinstance(error, CheckFailure):
        pass # Permission error (handled silently by SilentBot)
    else:
        # Other unexpected errors go to the global handler
        print(f"Unhandled setartchannel error: {error}")
        # await ctx.send(f"An unexpected error occurred: {error}") # Don't send message (Global handler does)

# << MODIFIED: Command to remove an art channel >>
@bot.command(name="removeartchannel")
@admin_only()
async def remove_art_channel(ctx, channel: discord.TextChannel):
    """Removes a specific channel from the list of monitored Art Channels."""
    global art_channel_ids # << MODIFIED: Use list variable >>

    if not channel:
        return await ctx.send("❌ Error: Valid text channel not found.")

    if channel.id in art_channel_ids:
        art_channel_ids.remove(channel.id)
        save_stats() # Save the updated list
        await ctx.send(f"✅ Removed {channel.mention} from the list of Art Channels. Monitoring disabled for this channel.")
        print(f"Art channel removed (ID: {channel.id}).")
    else:
        await ctx.send(f"ℹ️ {channel.mention} was not found in the list of Art Channels.")

@remove_art_channel.error # << NEW: Error handler for the remove command >>
async def remove_art_channel_error(ctx, error):
    """Error handler for removeartchannel command."""
    if isinstance(error, commands.ChannelNotFound):
        await ctx.send(f"❌ Error: Channel not found. Please provide a valid channel ID or #channel mention to remove.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Error: Please provide a valid text channel ID or #channel mention to remove.")
    elif isinstance(error, MissingRequiredArgument):
        await ctx.send("❌ Error: Channel ID or #channel mention is required to remove. Usage: `!removeartchannel <#channel or ID>`")
    elif isinstance(error, CheckFailure):
        pass # Permission error (handled silently by SilentBot)
    else:
        # Other unexpected errors go to the global handler
        print(f"Unhandled removeartchannel error: {error}")


@bot.command(name="trackhelp", aliases=["thelp"])
@admin_only()
async def trackhelp(ctx):
    # << MODIFIED: Updated help text for channel commands >>
    help_fields = [
        {"name": "!stats [@user or ID]", "value": "Shows statistics for the specified user (or yourself) (Includes Art Count).", "inline": False},
        {"name": "!allstats [filters...] [sort_key]", "value": "Generates Excel report. Filters: msgcount>N, twtcount>=N, joined>=N, won>=N, artcount>N (or <, >=, <=, =), `5<artcount<10` (range), @Role, nothaverole @Rol. Sort: messages, tweets.", "inline": False},
        {"name": "!filteruserid [filters...] [id]", "value": "Filters users based on numeric/role filters (includes artcount). Add 'id' for ID-only output.", "inline": False},
        {"name": "!listexcels", "value": "Lists available Excel (.xlsx) files.", "inline": False},
        {"name": "!deleteexcel <filename.xlsx>", "value": "Deletes the specified Excel file.", "inline": False},
        {"name": "!twitterlog <#channel or ID>", "value": "Adds a channel for specific X.com link monitoring & scans its history.", "inline": False}, # MODIFIED
        {"name": "!removetwitterlog <#channel or ID>", "value": "Removes a specific channel from X.com link monitoring.", "inline": False}, # MODIFIED
        {"name": "!setartchannel <#channel or ID>", "value": "Adds an 'Art Channel' where only media posts are allowed, enables art counting, and scans its history.", "inline": False}, # MODIFIED (also alias: !addartchannel)
        {"name": "!removeartchannel <#channel or ID>", "value": "Removes a specific channel from art channel monitoring.", "inline": False}, # MODIFIED
        {"name": "--- Stats Button Settings ---", "value": "\u200b", "inline": False},
        {"name": "!setstatschannel <#channel or ID>", "value": "Sends 'Show My Stats' button to the channel.", "inline": False},
        {"name": "!setstatscooldown <@role or ID> <duration>", "value": "Sets stats button cooldown (e.g., 5m, 1h, 2d, 0).", "inline": False},
        {"name": "--- Role Management ---", "value": "\u200b", "inline": False},
        {"name": "!settrackauthorizedrole <@role or ID>", "value": "Allows role to use track commands.", "inline": False},
        {"name": "!settracktargetrole <@role or ID> [@role2...]", "value": "Sets target roles for tracking.", "inline": False},
        {"name": "!removeauthorizedrole <@role or ID> [@role2...]", "value": "Removes roles from track authorized list.", "inline": False},
        {"name": "!removetargetrole <@role or ID> [@role2...]", "value": "Removes roles from track target roles.", "inline": False},
        {"name": "!setstatsroleauthorized <@role or ID> [@role2...]", "value": "Allows role to use !stats command.", "inline": False},
        {"name": "!removestatsroleauthorized <@role or ID> [@role2...]", "value": "Removes role's access to !stats.", "inline": False},
        {"name": "--- Bulk Actions ---", "value": "\u200b", "inline": False},
        {"name": "!bulkban <id1> [id2...] [reason=...]", "value": "Bans multiple users by ID.", "inline": False},
        {"name": "!bulkgiverole <@role or ID> <id1> [id2...]", "value": "Gives role to multiple users by ID.", "inline": False},
        {"name": "--- Event Management ---", "value": "\u200b", "inline": False},
        {"name": "!addevent <event_name> <id...>", "value": "Adds users to an event ('joined').", "inline": False},
        {"name": "!eventwinners <event_name> <id...>", "value": "Marks users as winners for an event.", "inline": False},
        {"name": "!notjoined <event_name> <id...>", "value": "Removes users from an event.", "inline": False},
        {"name": "!delevent <event_name>", "value": "Deletes an event from all records.", "inline": False},
        {"name": "!copyevent <#channel or ID> <event_name>", "value": "Copies members from voice/stage channel as 'joined'.", "inline": False},
        {"name": "!winnerlist <event_name>", "value": "Lists winners of an event (sends file).", "inline": False},
        {"name": "!joinedlist <event_name>", "value": "Lists users who only joined (sends file).", "inline": False},
        {"name": "!fixwinners <mod> <event_name> <id...>", "value": "Fixes winner status (mod: joined, notjoined).", "inline": False},
        {"name": "!fixjoined <mod> <event_name> <id...>", "value": "Fixes joined status (mod: winner, notjoined).", "inline": False},
        {"name": "!fixnotjoined <mod> <event_name> <id...>", "value": "Fixes not-joined status (mod: joined, winner).", "inline": False}
    ]
    MAX_FIELDS_PER_EMBED = 25
    num_embeds = (len(help_fields) + MAX_FIELDS_PER_EMBED - 1) // MAX_FIELDS_PER_EMBED

    for i in range(num_embeds):
        start_idx = i * MAX_FIELDS_PER_EMBED
        end_idx = min((i + 1) * MAX_FIELDS_PER_EMBED, len(help_fields))
        title = f"📘 Admin Command List (Part {i+1}/{num_embeds})" if num_embeds > 1 else "📘 Admin Command List"
        description = "Available administrator commands (continued):" if i > 0 else "Available administrator commands:"
        embed = discord.Embed(title=title, description=description, color=discord.Color.teal())
        for field in help_fields[start_idx:end_idx]:
            embed.add_field(name=field["name"], value=field["value"], inline=field.get("inline", False))
        if i == num_embeds - 1: embed.set_footer(text=f"Bot Prefix: {bot.command_prefix}")
        await ctx.send(embed=embed)

# << MODIFIED: Command to add a twitter log channel >>
@bot.command(name="twitterlog", aliases=["addtwitterlog"]) # Added alias
@admin_only()
async def add_twitter_log_channel(ctx, channel: discord.TextChannel):
    """Adds a channel to the list of monitored X.com log channels and scans its history."""
    global twitter_log_channel_ids, posted_links_set # << MODIFIED: Use list variable >>
    if not channel: return await ctx.send(f"❌ Error: Valid text channel not found.")

    if channel.id in twitter_log_channel_ids:
        return await ctx.send(f"ℹ️ {channel.mention} is already in the list of X.com log channels.")

    # Add channel to list
    twitter_log_channel_ids.append(channel.id)
    await ctx.send(f"✅ Added {channel.mention} to the X.com log channels.\n⏳ Scanning {channel.mention}'s history (max 10k messages) for valid links...")

    # --- History Scan (Only for the newly added channel) ---
    url_pattern = re.compile(r"^https://x\.com/[A-Za-z0-9_]+/status/[0-9]+(?:\?[^\s]*)?$")
    history_limit = 10000 # <<< YOU CAN CHANGE THIS NUMBER >>>
    added_count = 0
    changed = False # Data changed? (New link added?)

    try:
        async for message in channel.history(limit=history_limit):
            # Skip bots and messages without content
            if message.author.bot or not message.content: continue
            content = message.content.strip()
            match = url_pattern.match(content) # Use match() for start-to-end check

            # Check if the entire message is a valid link format
            if match:
                extracted_url = match.group(0)
                norm_url = extracted_url # Regex ensures correct format

                # Add if not already posted
                if norm_url not in posted_links_set:
                    posted_links_set.add(norm_url) # Add to global set
                    # Add to user stats only if user is tracked
                    should_track_author = False
                    if isinstance(message.author, discord.Member) and TARGET_ROLES:
                         if any(role.id in TARGET_ROLES for role in message.author.roles):
                             should_track_author = True
                    elif isinstance(message.author, discord.Member) and not TARGET_ROLES: # Track everyone if no target roles
                         should_track_author = True

                    if should_track_author:
                        uid = str(message.author.id)
                        udata = stats_data.setdefault(uid, DEFAULT_USER_TEMPLATE())
                        udata.setdefault("twitter_links", []).append(norm_url)
                    added_count += 1
                    changed = True # Change requiring save occurred
    except discord.Forbidden:
        await ctx.send(f"❌ Error: Missing permission to read {channel.mention}'s history during scan.")
        # Don't revert adding the channel, admin might fix perms later
    except Exception as e:
        await ctx.send(f"❌ Error during history scan for {channel.mention}: {e}")
        print(f"Error (twitterlog history scan for {channel.name}): {e}")
        traceback.print_exc()

    # Save stats if links were added or channel list was modified (it was, we appended)
    save_stats()
    await ctx.send(f"✅ History scan for {channel.mention} complete. Added {added_count} new unique valid links to internal set. Channel is active!")

@add_twitter_log_channel.error # << MODIFIED: Error handler for add command >>
async def add_twitter_log_channel_error(ctx, error):
    if isinstance(error, commands.ChannelNotFound): await ctx.send(f"❌ Error: Channel not found.")
    elif isinstance(error, commands.BadArgument): await ctx.send("❌ Error: Please provide a valid text channel.")
    elif isinstance(error, MissingRequiredArgument): await ctx.send("❌ Error: Channel is required. Usage: `!twitterlog <#channel or ID>`")
    elif isinstance(error, CheckFailure): pass # Handled silently by SilentBot / Global Handler
    else:
        # Unexpected errors go to global handler
        print(f"Unhandled twitterlog error: {error}")

# << MODIFIED: Command to remove a specific twitter log channel >>
@bot.command(name="removetwitterlog")
@admin_only()
async def remove_twitter_log_channel(ctx, channel: discord.TextChannel):
    """Removes a specific channel from the list of monitored X.com log channels."""
    global twitter_log_channel_ids # << MODIFIED: Use list variable >>

    if not channel:
        return await ctx.send("❌ Error: Valid text channel not found.")

    if channel.id in twitter_log_channel_ids:
        twitter_log_channel_ids.remove(channel.id)
        save_stats()
        await ctx.send(f"✅ Removed {channel.mention} from the X.com log channels. Monitoring disabled for this channel.")
        print(f"X.com log channel removed (ID: {channel.id}).")
    else:
        await ctx.send(f"ℹ️ {channel.mention} was not found in the list of X.com log channels.")

@remove_twitter_log_channel.error # << NEW: Error handler for remove command >>
async def remove_twitter_log_channel_error(ctx, error):
    """Error handler for removetwitterlog command."""
    if isinstance(error, commands.ChannelNotFound):
        await ctx.send(f"❌ Error: Channel not found. Please provide a valid channel ID or #channel mention to remove.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Error: Please provide a valid text channel ID or #channel mention to remove.")
    elif isinstance(error, MissingRequiredArgument):
        await ctx.send("❌ Error: Channel ID or #channel mention is required to remove. Usage: `!removetwitterlog <#channel or ID>`")
    elif isinstance(error, CheckFailure):
        pass # Permission error (handled silently by SilentBot)
    else:
        # Other unexpected errors go to the global handler
        print(f"Unhandled removetwitterlog error: {error}")


@bot.command(name="allstats")
@admin_only()
async def allstats(ctx, *args):
    """Generates an Excel report based on filters (including artcount)."""
    # Updated to handle new artcount filter (including range)
    numeric_filters = [] # Holds both simple and range filters
    role_filter_object = None
    nothaverole_filter_object = None
    sort_param = None
    i = 0
    while i < len(args):
        arg = args[i]
        # Try parsing numeric filters (simple or range)
        numeric_filter = parse_numeric_filter(arg)
        if numeric_filter:
            numeric_filters.append(numeric_filter)
        elif arg.lower() in ["messages", "tweets"]: # Add other sort keys if needed
            sort_param = arg.lower()
        elif arg.lower() == "nothaverole" and i + 1 < len(args):
            i += 1
            next_arg = args[i]
            try: role = await commands.RoleConverter().convert(ctx, next_arg)
            except commands.RoleNotFound: return await ctx.send(f"❌ Error: Role for 'nothaverole' not found: '{next_arg}'") # English text
            except Exception as e: return await ctx.send(f"❌ Error processing 'nothaverole' role: {e}") # English text
            if role:
                if nothaverole_filter_object is None: nothaverole_filter_object = role
                else: return await ctx.send("❌ Error: Only one 'nothaverole' filter allowed.") # English text
        else: # Assume it might be a role filter
            try: role = await commands.RoleConverter().convert(ctx, arg)
            except commands.RoleNotFound:
                # If it's not a role and not a known keyword, it's an error
                if arg.lower() not in ["messages", "tweets"]: # Check against known non-role args
                     return await ctx.send(f"❌ Error: Unrecognized filter or argument: '{arg}'") # English text
                # Otherwise ignore (it might be a sort key handled above)
            except Exception as e: return await ctx.send(f"❌ Error processing role filter: {e}") # English text

            if role: # If RoleConverter succeeded
                if role_filter_object is None: role_filter_object = role
                else: return await ctx.send("❌ Error: Only one role filter allowed.") # English text
        i += 1

    filters = {"numeric_filters": numeric_filters, "role_filter": role_filter_object, "nothaverole": nothaverole_filter_object}
    msg = await ctx.send("⏳ Generating Excel report...") # English text
    # generate_excel now handles the filtering logic
    excel_filename = generate_excel(ctx.guild, filters, sort_param)

    if excel_filename and os.path.exists(excel_filename):
        try:
            await msg.edit(content="✅ Excel report generated! Sending file...") # English text
            await ctx.send(file=discord.File(excel_filename))
            try: os.remove(excel_filename) # Delete file after sending
            except OSError as e: print(f"Could not delete sent Excel file: {e}") # English comment
        except discord.HTTPException as e: # File too large or other HTTP error
            # Try to get file size (may not always work depending on error)
            file_size_bytes = 0
            try:
                file_size_bytes = os.path.getsize(excel_filename)
            except OSError:
                pass # Ignore if size cannot be retrieved

            # Format file size for display
            if file_size_bytes > 1024 * 1024:
                size_str = f"{file_size_bytes / (1024*1024):.2f} MB"
            elif file_size_bytes > 1024:
                size_str = f"{file_size_bytes / 1024:.2f} KB"
            else:
                size_str = f"{file_size_bytes} bytes"

            await msg.edit(content=f"❌ Error sending Excel (File size: {size_str} - Discord limit is ~25MB): {e.status} - {e.text}") # English text
            # Try to delete the large file
            if os.path.exists(excel_filename):
                try: os.remove(excel_filename)
                except: pass
        except Exception as e:
            await msg.edit(content=f"❌ An unexpected error occurred while sending the Excel file: {e}") # English text
            print(f"Excel sending error: {e}") # English comment
            traceback.print_exc()
    elif excel_filename is None: # generate_excel returned None (generation error)
         await msg.edit(content="❌ Failed to generate Excel report (an error occurred).") # English text
    else: # Filename exists but file doesn't (unexpected)
         await msg.edit(content="❌ Excel report generated but the file could not be found.") # English text


@bot.command(name="listexcels")
@admin_only()
async def list_excels(ctx):
    """Lists available Excel (.xlsx) files."""
    try:
        # Consider listing only files matching the bot's naming pattern for clarity
        # pattern = r"^[a-zA-Z0-9_-]+_statistics_\d{8}_\d{6}(?:_\d)?\.xlsx$"
        excel_files = [f for f in os.listdir('.') if f.endswith('.xlsx')] # List all .xlsx for now
        if excel_files:
            # Truncate list if too long
            if len(excel_files) > 20:
                 file_list = "\n".join(excel_files[:20]) + f"\n... ({len(excel_files) - 20} more)" # English text
            else:
                 file_list = "\n".join(excel_files)
            await ctx.send("📊 Available Excel files:\n```\n" + file_list + "\n```") # English text
        else:
            await ctx.send("ℹ️ No Excel files found.") # English text
    except Exception as e:
        await ctx.send(f"❌ Error listing files: {e}") # English text
        print(f"Error (listexcels): {e}") # English comment

@bot.command(name="deleteexcel")
@admin_only()
async def delete_excel(ctx, *, filename: str):
    """Deletes the specified Excel file."""
    # Security: Prevent path traversal attacks
    clean_filename = os.path.basename(filename) # Get only the filename part
    if clean_filename != filename or not clean_filename.endswith('.xlsx'):
        return await ctx.send("❌ Error: Invalid filename or format.") # English text

    # File path (assuming current directory)
    file_path = os.path.join(".", clean_filename) # Current directory

    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            await ctx.send(f"✅ Deleted: {clean_filename}") # English text
        except OSError as e:
            await ctx.send(f"❌ Error deleting file: {e}") # English text
        except Exception as e:
             await ctx.send(f"❌ Unexpected error: {e}") # English text
    else:
        await ctx.send(f"❌ File not found: {clean_filename}") # English text

@bot.command(name="stats")
async def stats(ctx, member: discord.Member = None):
    """Shows statistics for the specified user (or yourself) (Includes Art Count)."""
    # Authorization check (uses updated stats_authorized_check)
    if not await stats_authorized_check(ctx):
        # Don't send error, just try to silently delete invocation message
        try:
            if ctx.guild: await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException): pass
        return

    target_member = member or ctx.author # If member not specified, use command author

    # Use helper function to generate embeds
    stats_embeds = await generate_user_stats_embeds(target_member)

    # Send embeds one by one (to avoid potential combined length issues)
    for embed in stats_embeds:
        await ctx.send(embed=embed)

@stats.error
async def stats_error(ctx, error):
    """Error handler for stats command."""
    # Check authorization based on the *new* rules before sending error messages
    is_authorized_now = await stats_authorized_check(ctx)

    if isinstance(error, MemberNotFound):
        if is_authorized_now: await ctx.send("❌ Error: Member not found.") # English text
    elif isinstance(error, CheckFailure):
        # This handles the case where stats_authorized_check returned False
        pass # Already handled silently by deleting the message
    elif isinstance(error, BadArgument):
        if is_authorized_now: await ctx.send("❌ Error: Invalid argument.") # English text
    else: # Unexpected errors
        # Log regardless of authorization
        print(f"Error (stats): {error}") # English comment
        traceback.print_exc()
        # Only send generic error if the user would have been authorized
        if is_authorized_now: await ctx.send(f"❌ An unexpected error occurred: {error}") # English text

    # Attempt to delete unauthorized invocation message (redundant but safe)
    if not is_authorized_now and ctx.guild:
         try: await ctx.message.delete()
         except: pass


@bot.command(name="filteruserid")
@admin_only()
async def filter_user_id(ctx, *args):
    """Filters users based on numeric and role filters (including artcount)."""
    # Updated to handle artcount filter (including range)
    numeric_filters = [] # Holds both simple and range filters
    role_filter_object = None
    nothaverole_filter_object = None
    id_only = False
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.lower() == "id":
            id_only = True
        else:
            # Try parsing numeric filters (simple or range)
            numeric_filter = parse_numeric_filter(arg)
            if numeric_filter:
                numeric_filters.append(numeric_filter)
            elif arg.lower() == "nothaverole" and i + 1 < len(args):
                i += 1; next_arg = args[i]
                try: role = await commands.RoleConverter().convert(ctx, next_arg)
                except commands.RoleNotFound: return await ctx.send(f"❌ Error: Role for 'nothaverole' not found: '{next_arg}'") # English text
                except Exception as e: return await ctx.send(f"❌ Error processing 'nothaverole' role: {e}") # English text
                if role:
                    if nothaverole_filter_object is None: nothaverole_filter_object = role
                    else: return await ctx.send("❌ Error: Only one 'nothaverole' filter allowed.") # English text
            else: # Assume it might be a role filter
                try: role = await commands.RoleConverter().convert(ctx, arg)
                except commands.RoleNotFound: return await ctx.send(f"❌ Error: Unrecognized filter or argument: '{arg}'") # English text
                except Exception as e: return await ctx.send(f"❌ Error processing role filter: {e}") # English text
                if role:
                    if role_filter_object is None: role_filter_object = role
                    else: return await ctx.send("❌ Error: Only one role filter allowed.") # English text
        i += 1

    # Filter users
    filtered_users = []
    current_stats_data = stats_data.copy() # Work on a copy
    op_map = {">": lambda a, b: a > b, "<": lambda a, b: a < b, ">=": lambda a, b: a >= b, "<=": lambda a, b: a <= b, "=": lambda a, b: a == b, "!=": lambda a, b: a != b}

    for user_id_str, data in current_stats_data.items():
        if not user_id_str.isdigit() or not isinstance(data, dict): continue
        try: user_id = int(user_id_str)
        except ValueError: continue
        member = ctx.guild.get_member(user_id)
        if not member or (TARGET_ROLES and not any(role.id in TARGET_ROLES for role in member.roles)): continue

        # --- Apply Filters ---
        pass_filters = True
        # Role Filters
        if role_filter_object and (role_filter_object not in member.roles): pass_filters = False
        if not_have_role_object and (not_have_role_object in member.roles): pass_filters = False

        # Numeric Filters
        if pass_filters:
            for filt in numeric_filters:
                user_val = 0
                field = filt[0]
                # Get user value for the field
                if field == "total_message_count": user_val = data.get("total_message_count", 0)
                elif field == "tweet_count": user_val = len(data.get("twitter_links", []))
                elif field == "joined": user_val = len(data.get("events", []))
                elif field == "won": user_val = len(data.get("winners", []))
                elif field == "art_count": user_val = data.get("art_count", 0) # NEW

                # Apply filter based on tuple length
                if len(filt) == 3: # Simple filter
                    _, operator, filter_value = filt
                    if operator not in op_map or not op_map[operator](user_val, filter_value):
                        pass_filters = False; break
                elif len(filt) == 5: # Range filter
                    _, lower_op, lower_val, upper_op, upper_val = filt
                    # Check lower bound
                    if lower_op not in op_map or not op_map[lower_op](user_val, lower_val):
                        pass_filters = False; break
                    # Check upper bound
                    if upper_op not in op_map or not op_map[upper_op](user_val, upper_val):
                        pass_filters = False; break
        # --- Filters End ---

        if pass_filters: # Add if all filters passed
            filtered_users.append((user_id_str, member.display_name))

    # Send results
    if not filtered_users:
        return await ctx.send("ℹ️ No users match the specified filters.") # English text

    filtered_users.sort(key=lambda x: x[1].lower()) # Sort by display name

    result_lines = []
    if id_only:
        result_lines = [user_id for user_id, _ in filtered_users]
    else:
        result_lines = [f"{user_id} - {name}" for user_id, name in filtered_users]

    result_text = "\n".join(result_lines)
    # Split results into chunks of 1900 chars (leaving room for code block formatting)
    chunks = chunk_text_by_size(result_text, 1900)

    total_found = len(filtered_users)
    for i, chunk in enumerate(chunks):
        header = f"Filtered Users (Total: {total_found})" # English text
        if len(chunks) > 1:
            header = f"Filtered Users (Part {i+1}/{len(chunks)}, Total: {total_found})" # English text
        # Consider sending as plain text instead of code block if mentions are desired (but they won't ping)
        await ctx.send(f"```\n{header}\n\n{chunk}\n```")


# --- EVENT MANAGEMENT COMMANDS ---
# (User messages translated to English)
async def _modify_event(ctx, event_name: str, user_ids: tuple[str], action: str):
    if not event_name or not user_ids:
        return await ctx.send("❌ Error: Event name and at least one user ID are required.") # English text

    event_name_std = standardize_event_name(event_name)
    processed, not_found, no_change = [], [], []
    changed = False
    processed_ids = set()

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip())
            if user_id in processed_ids: continue
            processed_ids.add(user_id)
            member = ctx.guild.get_member(user_id)
            user_id_str = str(user_id)
            display_name = member.display_name if member else f"ID:{user_id}"

            if member or user_id_str in stats_data:
                user_data = stats_data.setdefault(user_id_str, DEFAULT_USER_TEMPLATE())
                events_list = user_data.setdefault("events", [])
                winners_list = user_data.setdefault("winners", [])
                events_std_set = {standardize_event_name(e) for e in events_list}
                winners_std_set = {standardize_event_name(w) for w in winners_list}
                user_modified = False

                if action == "addevent":
                    if event_name_std not in events_std_set:
                        events_list.append(event_name); processed.append(f"{display_name} ({user_id})"); user_modified = True
                    else: no_change.append(f"{display_name} ({user_id}) (already joined)") # English text
                elif action == "eventwinners":
                    added_to_joined = False
                    if event_name_std not in events_std_set:
                        events_list.append(event_name); added_to_joined = True; user_modified = True
                    if event_name_std not in winners_std_set:
                        winners_list.append(event_name)
                        msg = f"{display_name} ({user_id})" + (" (also added to joined)" if added_to_joined else "") # English text
                        processed.append(msg); user_modified = True
                    elif not added_to_joined: no_change.append(f"{display_name} ({user_id}) (already winner)") # English text
                elif action == "notjoined":
                    olen1, olen2 = len(events_list), len(winners_list)
                    user_data["events"] = [e for e in events_list if standardize_event_name(e) != event_name_std]
                    user_data["winners"] = [w for w in winners_list if standardize_event_name(w) != event_name_std]
                    if len(user_data["events"]) < olen1 or len(user_data["winners"]) < olen2:
                        processed.append(f"{display_name} ({user_id})"); user_modified = True
                    else: no_change.append(f"{display_name} ({user_id}) (not in event)") # English text

                if user_modified: changed = True
            else: not_found.append(id_str)
        except ValueError: not_found.append(id_str)

    if changed: save_stats()
    response = ""
    verb_map = {"addevent": "Added", "eventwinners": "Marked as winner", "notjoined": "Removed"} # English text
    if processed: response += f"✅ {verb_map[action]} for event '{event_name}':\n```\n" + "\n".join(processed) + "\n```\n" # English text
    if no_change: response += f"ℹ️ No change for event '{event_name}':\n```\n" + "\n".join(no_change) + "\n```\n" # English text
    if not_found: response += f"❌ Users not found/invalid: {', '.join(not_found)}\n" # English text
    if not response: response = "ℹ️ No valid users found to process or no changes needed." # English text
    # Send response in chunks if too long
    if len(response) > 2000:
        await ctx.send(f"Results for '{action}' on event '{event_name}' (split):") # English text
        for chunk in chunk_text_by_size(response, 1950):
            await ctx.send(chunk)
    elif response:
        await ctx.send(response)


@bot.command(name="addevent")
@admin_only()
async def add_event(ctx, event_name: str, *user_ids: str): await _modify_event(ctx, event_name, user_ids, "addevent")
@bot.command(name="eventwinners")
@admin_only()
async def event_winners(ctx, event_name: str, *user_ids: str): await _modify_event(ctx, event_name, user_ids, "eventwinners")
@bot.command(name="notjoined")
@admin_only()
async def not_joined(ctx, event_name: str, *user_ids: str): await _modify_event(ctx, event_name, user_ids, "notjoined")

@bot.command(name="delevent")
@admin_only()
async def del_event(ctx, *, event_name: str):
    if not event_name: return await ctx.send("❌ Error: Event name is required.") # English text
    event_name_std = standardize_event_name(event_name)
    affected_users_count, changed = 0, False
    user_ids_to_process = [uid for uid in stats_data.keys() if uid.isdigit()]

    for user_id in user_ids_to_process:
        data = stats_data[user_id]; modified = False
        if not isinstance(data, dict): continue
        olen1 = len(data.get("events", [])); new_events = [e for e in data.get("events", []) if standardize_event_name(e) != event_name_std]
        if len(new_events) < olen1: data["events"] = new_events; modified = True
        olen2 = len(data.get("winners", [])); new_winners = [w for w in data.get("winners", []) if standardize_event_name(w) != event_name_std]
        if len(new_winners) < olen2: data["winners"] = new_winners; modified = True
        if modified: affected_users_count += 1; changed = True

    if changed: save_stats(); await ctx.send(f"✅ Event '{event_name}' deleted from {affected_users_count} user records.") # English text
    else: await ctx.send(f"ℹ️ No records found for event '{event_name}' to delete.") # English text

@bot.command(name="copyevent")
@admin_only()
async def copy_event(ctx, channel: discord.VoiceChannel | discord.StageChannel, *, event_name: str):
    if not event_name: return await ctx.send("❌ Error: Event name is required.") # English text
    if not channel: return await ctx.send(f"❌ Error: Valid voice or stage channel not found.") # English text

    event_name_std = standardize_event_name(event_name)
    added, already_added, not_target_role = [], [], []
    changed = False
    current_members = channel.members
    if not current_members: return await ctx.send(f"ℹ️ No members found in {channel.mention}.") # English text

    for member in current_members:
        if member.bot: continue
        if TARGET_ROLES and not any(role.id in TARGET_ROLES for role in member.roles):
            not_target_role.append(f"{member.display_name} ({member.id})"); continue
        user_id = str(member.id); user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
        current_events_std = {standardize_event_name(e) for e in user_data.get("events", [])}
        if event_name_std not in current_events_std:
            user_data.setdefault("events", []).append(event_name); added.append(f"{member.display_name} ({member.id})"); changed = True
        else: already_added.append(f"{member.display_name} ({member.id})")

    if changed: save_stats()
    response = ""
    if added: response += f"✅ Added {len(added)} members from {channel.mention} to event '{event_name}'.\n" # English text
    if already_added: response += f"ℹ️ {len(already_added)} members were already in event '{event_name}'.\n" # English text
    if not_target_role: response += f"⚠️ {len(not_target_role)} members skipped (not in target roles).\n" # English text
    if not response: response = f"ℹ️ No new members found in {channel.mention} to add." # English text
    if len(response) > 2000: response = response[:1997] + "..."
    await ctx.send(response)

async def _generate_event_list_file(ctx, event_name: str, list_type: str):
    if not event_name: return await ctx.send("❌ Error: Event name is required.") # English text
    event_name_std = standardize_event_name(event_name)
    users_list = []; current_stats_data = stats_data.copy()

    for user_id, data in current_stats_data.items():
        if not user_id.isdigit() or not isinstance(data, dict): continue
        user_events_std = {standardize_event_name(e) for e in data.get("events", [])}
        user_winners_std = {standardize_event_name(w) for w in data.get("winners", [])}
        include = False
        if list_type == "winnerlist" and event_name_std in user_winners_std: include = True
        elif list_type == "joinedlist" and event_name_std in user_events_std and event_name_std not in user_winners_std: include = True
        if include:
            try:
                member = ctx.guild.get_member(int(user_id))
                if member and (not TARGET_ROLES or any(role.id in TARGET_ROLES for role in member.roles)):
                    users_list.append((user_id, member.display_name))
            except ValueError: continue

    list_name = "Winners" if list_type == "winnerlist" else "Joined Only" # English text
    if not users_list: return await ctx.send(f"ℹ️ No {list_name.lower()} found for event '{event_name}'.") # English text

    users_list.sort(key=lambda x: x[1].lower())
    filename = f"{list_type}_{sanitize_filename(event_name)}_{datetime.datetime.now().strftime('%Y%m%d')}.txt"
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"{list_name} for Event '{event_name}' ({len(users_list)} users):\n\n") # English text
            for uid, name in users_list: f.write(f"{uid} - {name}\n")
        await ctx.send(f"📄 {list_name} list for '{event_name}':", file=discord.File(filename)) # English text
    except Exception as e: await ctx.send(f"❌ Error creating/sending file: {e}") # English text
    finally:
        if os.path.exists(filename):
            try: os.remove(filename)
            except OSError as e: print(f"Could not delete temp file '{filename}': {e}") # English comment

@bot.command(name="winnerlist")
@admin_only()
async def winner_list(ctx, *, event_name: str): await _generate_event_list_file(ctx, event_name, "winnerlist")
@bot.command(name="joinedlist")
@admin_only()
async def joined_list(ctx, *, event_name: str): await _generate_event_list_file(ctx, event_name, "joinedlist")

# --- FIX COMMANDS ---
# (User messages translated to English)
async def _fix_event_status(ctx, mod: str, event_name: str, user_ids: tuple[str], fix_type: str):
    valid_mods = {"fixwinners": ["joined", "notjoined"], "fixjoined": ["winner", "notjoined"], "fixnotjoined": ["joined", "winner"]}
    if not mod or not event_name or not user_ids: return await ctx.send(f"❌ Error: Mode ({'/'.join(valid_mods[fix_type])}), event name, and IDs required.") # English text
    mod = mod.lower()
    if mod not in valid_mods[fix_type]: return await ctx.send(f"❌ Error: Mode must be one of: {', '.join(valid_mods[fix_type])}.") # English text

    event_name_std = standardize_event_name(event_name)
    fixed, not_found, no_change_needed, skipped = [], [], [], []
    changed = False; processed_ids = set()

    for id_str in user_ids:
        try:
            user_id = int(id_str.strip()); user_id_str = str(user_id)
            if user_id in processed_ids: continue
            processed_ids.add(user_id)
            member = ctx.guild.get_member(user_id)
            display_name = member.display_name if member else f"ID:{user_id}"

            if user_id_str in stats_data or member:
                user_data = stats_data.setdefault(user_id_str, DEFAULT_USER_TEMPLATE())
                events_list = user_data.setdefault("events", [])
                winners_list = user_data.setdefault("winners", [])
                is_winner = event_name_std in {standardize_event_name(w) for w in winners_list}
                is_joined = event_name_std in {standardize_event_name(e) for e in events_list}
                user_modified, action_taken = False, ""

                if fix_type == "fixwinners":
                    if not is_winner: skipped.append(f"{display_name} ({user_id}) (not a winner)"); continue # English text
                    if mod == "joined":
                        if not is_joined: events_list.append(event_name); user_modified = True; action_taken = "Added to 'joined' list" # English text
                        else: no_change_needed.append(f"{display_name} ({user_id}) (already 'joined')") # English text
                    elif mod == "notjoined":
                        olen1, olen2 = len(winners_list), len(events_list)
                        user_data["winners"] = [w for w in winners_list if standardize_event_name(w) != event_name_std]
                        user_data["events"] = [e for e in events_list if standardize_event_name(e) != event_name_std]
                        if len(user_data["winners"]) < olen1 or len(user_data["events"]) < olen2: user_modified = True; action_taken = "Completely removed from event" # English text
                        else: no_change_needed.append(f"{display_name} ({user_id}) (no records found?)") # English text
                elif fix_type == "fixjoined":
                    if not is_joined: skipped.append(f"{display_name} ({user_id}) (not joined)"); continue # English text
                    if mod == "winner":
                        if is_winner: skipped.append(f"{display_name} ({user_id}) (already winner)"); continue # English text
                        winners_list.append(event_name); user_modified = True; action_taken = "Added to 'winner' list" # English text
                    elif mod == "notjoined":
                        if is_winner: skipped.append(f"{display_name} ({user_id}) (is winner, cannot use 'notjoined')"); continue # English text
                        olen = len(events_list); user_data["events"] = [e for e in events_list if standardize_event_name(e) != event_name_std]
                        if len(user_data["events"]) < olen: user_modified = True; action_taken = "Removed from 'joined' list" # English text
                        else: no_change_needed.append(f"{display_name} ({user_id}) (no 'joined' record?)") # English text
                elif fix_type == "fixnotjoined":
                    if is_joined or is_winner: skipped.append(f"{display_name} ({user_id}) (already in event)"); continue # English text
                    if not member: skipped.append(f"{display_name} ({user_id}) (member not found)"); continue # English text
                    if mod == "joined": events_list.append(event_name); user_modified = True; action_taken = "Added to 'joined' list" # English text
                    elif mod == "winner": events_list.append(event_name); winners_list.append(event_name); user_modified = True; action_taken = "Added to 'joined' and 'winner' lists" # English text

                if user_modified: fixed.append(f"{display_name} ({user_id}) - {action_taken}"); changed = True
            else: not_found.append(id_str)
        except ValueError: not_found.append(id_str)

    if changed: save_stats()
    response = ""
    if fixed: response += f"✅ Fixed status for event '{event_name}' (mode: {mod}):\n```\n" + "\n".join(fixed) + "\n```\n" # English text
    if no_change_needed: response += f"ℹ️ No changes needed for these users in '{event_name}':\n```\n" + "\n".join(no_change_needed) + "\n```\n" # English text
    if skipped: response += f"⚠️ Skipped (status mismatch/not found):\n```\n" + "\n".join(skipped) + "\n```\n" # English text
    if not_found: response += f"❌ User IDs not found/invalid: {', '.join(not_found)}\n" # English text
    if not response: response = "ℹ️ No valid users found to process." # English text
    # Send response in chunks if too long
    if len(response) > 2000:
        await ctx.send(f"Results for '{fix_type}' (mode: {mod}) on event '{event_name}' (split):") # English text
        for chunk in chunk_text_by_size(response, 1950):
            await ctx.send(chunk)
    elif response:
        await ctx.send(response)


@bot.command(name="fixwinners")
@admin_only()
async def fix_winners(ctx, mod: str, event_name: str, *user_ids: str): await _fix_event_status(ctx, mod, event_name, user_ids, "fixwinners")
@bot.command(name="fixjoined")
@admin_only()
async def fix_joined(ctx, mod: str, event_name: str, *user_ids: str): await _fix_event_status(ctx, mod, event_name, user_ids, "fixjoined")
@bot.command(name="fixnotjoined")
@admin_only()
async def fix_not_joined(ctx, mod: str, event_name: str, *user_ids: str): await _fix_event_status(ctx, mod, event_name, user_ids, "fixnotjoined")

# --- BULK ACTIONS ---
# (User messages translated to English)
@bot.command(name="bulkban")
@commands.has_permissions(ban_members=True)
@admin_only()
async def bulk_ban(ctx, *args):
    if not args: return await ctx.send("❌ Error: At least one user ID required.") # English text
    reason = "Bulk ban" # English reason
    user_ids = list(args)
    for i in range(len(args) - 1, -1, -1):
        if args[i].lower().startswith("reason=") and len(args[i]) > 7: reason = args[i][7:]; user_ids = list(args[:i]); break
        elif args[i].lower() == "reason=": user_ids = list(args[:i]); break
    if not user_ids: return await ctx.send("❌ Error: At least one user ID required (excluding reason).") # English text

    banned, errors, already_banned, not_found_or_cant_ban = [], [], [], []
    msg = await ctx.send(f"⏳ Processing bulk ban for {len(user_ids)} users...") # English text

    for id_str in user_ids:
        await asyncio.sleep(0.1)
        try:
            user_id = int(id_str.strip())
            if user_id == ctx.author.id or user_id == bot.user.id: errors.append(f"{user_id} (Cannot ban self/bot)"); continue # English text
            target_user = discord.Object(id=user_id); name = f"ID:{user_id}"
            try: user_info = await bot.fetch_user(user_id); name = user_info.name
            except: pass
            try:
                await ctx.guild.fetch_ban(target_user); already_banned.append(f"{name} ({user_id})"); continue
            except discord.NotFound: pass
            except discord.Forbidden: errors.append(f"{name} ({user_id}) (No permission to check ban list)"); continue # English text
            except discord.HTTPException as e: errors.append(f"{name} ({user_id}) (HTTP Error checking ban: {e.status})"); continue # English text
            try:
                await ctx.guild.ban(target_user, reason=f"{reason} (Banned by: {ctx.author})", delete_message_days=0); banned.append(f"{name} ({user_id})") # English text in reason
            except discord.Forbidden: not_found_or_cant_ban.append(f"{name} ({user_id}) (No Permission/Higher Role/Not Found)") # English text
            except discord.NotFound: not_found_or_cant_ban.append(f"{id_str} (User not found)") # English text
            except discord.HTTPException as e: errors.append(f"{name} ({user_id}) (HTTP Error during ban: {e.status})") # English text
            except Exception as e: errors.append(f"{name} ({user_id}) (Unknown Error: {e})") # English text
        except ValueError: errors.append(f"'{id_str}' (Invalid ID)") # English text
        except Exception as e: errors.append(f"'{id_str}' (General Error: {e})") # English text

    response = ""
    if banned: response += f"✅ Successfully banned ({len(banned)}):\n```\n" + "\n".join(banned) + "\n```\n" # English text
    if already_banned: response += f"ℹ️ Already banned ({len(already_banned)}):\n```\n" + "\n".join(already_banned) + "\n```\n" # English text
    if not_found_or_cant_ban: response += f"⚠️ Not found or could not ban ({len(not_found_or_cant_ban)}):\n```\n" + "\n".join(not_found_or_cant_ban) + "\n```\n" # English text
    if errors: response += f"❌ Errors ({len(errors)}):\n```\n" + "\n".join(errors) + "\n```\n" # English text
    if not response: response = "ℹ️ No users found to process or report." # English text

    try:
        if len(response) <= 2000: await msg.edit(content=response)
        else:
            await msg.edit(content=f"Bulk ban results (split):"); # English text
            for chunk in chunk_text_by_size(response, 1950): await ctx.send(chunk)
    except discord.HTTPException as e:
        print(f"Error sending bulk ban results: {e}\n--- Bulk Ban Results ---\n{response}\n--- End Results ---") # English comment
        await ctx.send("❌ Error sending results message. Details logged.") # English text

@bot.command(name="bulkgiverole")
@commands.has_permissions(manage_roles=True)
@admin_only()
async def bulk_give_role(ctx, role: discord.Role, *user_ids: str):
    if not user_ids: return await ctx.send("❌ Error: At least one user ID required.") # English text
    if not role: return await ctx.send(f"❌ Error: Role not found.") # English text
    if ctx.guild.me.top_role <= role: return await ctx.send(f"❌ Error: Bot role too low to assign {role.mention}.") # English text
    if ctx.author.id != ctx.guild.owner_id and ctx.author.top_role <= role : return await ctx.send(f"❌ Error: Your role is too low to assign {role.mention}.") # English text

    given, errors, already_had, not_found = [], [], [], []
    msg = await ctx.send(f"⏳ Giving role {role.mention} to {len(user_ids)} users...") # English text

    for id_str in user_ids:
        await asyncio.sleep(0.1)
        try:
            user_id = int(id_str.strip()); member = ctx.guild.get_member(user_id)
            if not member: not_found.append(f"{id_str} (Not in server)"); continue # English text
            if role in member.roles: already_had.append(f"{member.display_name} ({user_id})"); continue
            try:
                await member.add_roles(role, reason=f"Bulk role assignment (Assigner: {ctx.author})"); given.append(f"{member.display_name} ({user_id})") # English text in reason
            except discord.Forbidden: errors.append(f"{member.display_name} ({user_id}) (No Permission)") # English text
            except discord.HTTPException as e: errors.append(f"{member.display_name} ({user_id}) (HTTP Error: {e.status})") # English text
            except Exception as e: errors.append(f"{member.display_name} ({user_id}) (Unknown Error: {e})") # English text
        except ValueError: errors.append(f"'{id_str}' (Invalid ID)") # English text
        except Exception as e: errors.append(f"'{id_str}' (General Error: {e})") # English text

    response = ""
    if given: response += f"✅ Gave role {role.mention} to ({len(given)}):\n```\n" + "\n".join(given) + "\n```\n" # English text
    if already_had: response += f"ℹ️ Already had role ({len(already_had)}):\n```\n" + "\n".join(already_had) + "\n```\n" # English text
    if not_found: response += f"⚠️ Not found in server ({len(not_found)}):\n```\n" + "\n".join(not_found) + "\n```\n" # English text
    if errors: response += f"❌ Errors ({len(errors)}):\n```\n" + "\n".join(errors) + "\n```\n" # English text
    if not response: response = "ℹ️ No users found to process or report." # English text

    try:
        if len(response) <= 2000: await msg.edit(content=response)
        else:
            await msg.edit(content=f"Bulk role results ({role.mention}) (split):") # English text
            for chunk in chunk_text_by_size(response, 1950): await ctx.send(chunk)
    except discord.HTTPException as e:
        print(f"Error sending bulk role results: {e}\n--- Bulk Role Results ---\n{response}\n--- End Results ---") # English comment
        await ctx.send("❌ Error sending results message. Details logged.") # English text

# --- STATS BUTTON RELATED COMMANDS ---
# (User messages translated to English)
@bot.command(name="setstatschannel")
@admin_only()
async def set_stats_channel(ctx, channel: discord.TextChannel):
    global stats_channel_id, stats_message_id
    if not channel: return await ctx.send("❌ Error: Valid text channel not found.") # English text

    if stats_channel_id and stats_message_id:
        try:
            old_channel = bot.get_channel(stats_channel_id) or await bot.fetch_channel(stats_channel_id)
            if old_channel: old_msg = await old_channel.fetch_message(stats_message_id); await old_msg.delete()
        except (discord.NotFound, discord.Forbidden, Exception) as e: print(f"Could not delete old stats button message ({stats_message_id} in {stats_channel_id}): {e}") # English comment
        finally: stats_message_id = None

    stats_channel_id = channel.id
    try:
        view = StatsView(); sent_message = await channel.send("📊 Click the button below to see your statistics.", view=view) # English text
        stats_message_id = sent_message.id; save_stats()
        await ctx.send(f"✅ Stats button sent to {channel.mention} (ID: {stats_message_id}).") # English text
    except (discord.Forbidden, Exception) as e:
        await ctx.send(f"❌ Error sending button: {e}") # English text
        stats_channel_id, stats_message_id = None, None; save_stats()
        if not isinstance(e, discord.Forbidden): traceback.print_exc()

@set_stats_channel.error
async def set_stats_channel_error(ctx, error):
    if isinstance(error, commands.ChannelNotFound): await ctx.send(f"❌ Error: Channel not found.") # English text
    elif isinstance(error, commands.BadArgument): await ctx.send("❌ Error: Please provide a valid text channel.") # English text
    elif isinstance(error, MissingRequiredArgument): await ctx.send("❌ Error: Channel is required.") # English text
    elif isinstance(error, CheckFailure): pass # Handled silently
    else: await ctx.send(f"Unexpected error: {error}"); print(f"Error (setstatschannel): {error}"); traceback.print_exc() # English text

@bot.command(name="setstatscooldown")
@admin_only()
async def set_stats_cooldown(ctx, role: discord.Role, duration: str):
    global stats_cooldowns
    if not role: return await ctx.send("❌ Error: Valid role not found.") # English text
    cooldown_seconds = parse_cooldown_duration(duration)
    if cooldown_seconds is None: return await ctx.send("❌ Error: Invalid duration format (e.g., 5m, 1h, 2d, 0).") # English text

    role_id_str = str(role.id); stats_cooldowns[role_id_str] = cooldown_seconds; save_stats()

    if cooldown_seconds == 0: await ctx.send(f"✅ Stats button cooldown removed for role {role.mention}.") # English text
    else:
        delta = timedelta(seconds=cooldown_seconds); parts = []
        if delta.days > 0: parts.append(f"{delta.days}d") # d -> day
        hrs, rem = divmod(delta.seconds, 3600); mins, secs = divmod(rem, 60)
        if hrs > 0: parts.append(f"{hrs}h") # h -> hour
        if mins > 0: parts.append(f"{mins}m") # m -> minute
        if secs > 0 and not parts: parts.append(f"{secs}s") # s -> second
        duration_display = "".join(parts) or f"{cooldown_seconds}s"
        await ctx.send(f"✅ Stats button cooldown for role {role.mention} set to {duration_display}.") # English text

@set_stats_cooldown.error
async def set_stats_cooldown_error(ctx, error):
    if isinstance(error, commands.RoleNotFound): await ctx.send(f"❌ Error: Role not found.") # English text
    elif isinstance(error, commands.BadArgument): await ctx.send("❌ Error: Invalid role or duration format.") # English text
    elif isinstance(error, MissingRequiredArgument): await ctx.send(f"❌ Missing argument: {error.param.name}.") # English text
    elif isinstance(error, CheckFailure): pass # Handled silently
    else: await ctx.send(f"Unexpected error: {error}"); print(f"Error (setstatscooldown): {error}"); traceback.print_exc() # English text

# --- EVENT HANDLERS ---
@bot.event
async def on_ready():
    """Runs when the bot is ready."""
    print("-" * 30)
    print(f"Bot ready! Logged in as: {bot.user.name} ({bot.user.id})") # English text
    print(f"Guilds: {len(bot.guilds)}") # English text
    # Load config (including art_channel_id)
    load_data() # Load data first
    print(f"Track Auth Roles: {AUTHORIZED_ROLES}") # English text
    print(f"Track Target Roles: {TARGET_ROLES}") # English text
    print(f"Stats Auth Roles: {STATS_AUTHORIZED_ROLES}") # English text
    print(f"X.com Log Channels: {twitter_log_channel_ids or 'None Set'}") # << MODIFIED: Print list >>
    print(f"Art Channels: {art_channel_ids or 'None Set'}")             # << MODIFIED: Print list >>
    print(f"Stats Button Channel: {stats_channel_id or 'Not Set'} (Msg ID: {stats_message_id or 'None'})") # English text
    cooldown_display = []
    for rid, secs in stats_cooldowns.items():
        # Short cooldown display (English units)
        cooldown_str = f"{int(secs)}s"
        if int(secs) == 0: cooldown_str = "0s"
        elif int(secs) % 86400 == 0: cooldown_str = f"{int(secs)//86400}d"
        elif int(secs) % 3600 == 0: cooldown_str = f"{int(secs)//3600}h"
        elif int(secs) % 60 == 0: cooldown_str = f"{int(secs)//60}m"
        cooldown_display.append(f"{rid}:{cooldown_str}")
    print(f"Stats Cooldowns: {', '.join(cooldown_display) or 'None'}") # English text
    print("-" * 30)
    # Add persistent view when bot is ready
    bot.add_view(StatsView())
    print("Persistent StatsView registered.") # English comment
    # Add the Global Error Handler Cog AFTER other setup
    await bot.add_cog(GlobalErrorHandler(bot))


# << MODIFIED: on_message - Check lists for channels >>
@bot.event
async def on_message(message):
    """Runs when a message is sent."""
    # Ignore bots and DMs
    if message.author.bot or not message.guild:
        return

    user_id = str(message.author.id)
    member = message.author # This is a Member object in guild context

    # --- Determine if user should be tracked ---
    should_track = False
    # Ensure member object for role check
    if isinstance(member, discord.Member) and TARGET_ROLES:
        if any(role.id in TARGET_ROLES for role in member.roles):
            should_track = True
    elif isinstance(member, discord.Member) and not TARGET_ROLES: # Track everyone if no target roles
        should_track = True


    # --- X.com Log Channel Logic ---
    # << MODIFIED: Check if channel ID is in the list >>
    if twitter_log_channel_ids and message.channel.id in twitter_log_channel_ids:
        content = message.content.strip()
        url_pattern = re.compile(r"^https://x\.com/[A-Za-z0-9_]+/status/[0-9]+(?:\?[^\s]*)?$")
        match = url_pattern.match(content) # Use match() for start-to-end check
        is_author_admin = await is_admin(member) # Check admin status once

        if match: # Message is a valid link format
            extracted_url = match.group(0)
            norm_url = extracted_url # Regex ensures correct format

            if norm_url in posted_links_set: # Duplicate link
                if not is_author_admin:
                    try: await message.delete()
                    except (discord.Forbidden, discord.HTTPException) as e: print(f"Error deleting duplicate link message: {e}") # English comment
            else: # New link
                posted_links_set.add(norm_url)
                link_added_to_stats = False
                if should_track: # Add to user stats only if tracked
                    user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
                    user_data.setdefault("twitter_links", []).append(norm_url)
                    link_added_to_stats = True
                save_stats() # Save immediately for new links (updates set and potentially user data)
                if not link_added_to_stats: print(f"Added untracked user's link {norm_url} to global set.") # English comment
        elif not is_author_admin: # Message is not a valid link format and author is not admin
            try: await message.delete()
            except (discord.Forbidden, discord.HTTPException) as e: print(f"Error deleting invalid message in x.com log channel: {e}") # English comment

        # Don't process commands or other logic for messages in the Twitter log channel
        return # Stop further processing for this channel

    # --- Art Channel Logic ---
    # << MODIFIED: Check if channel ID is in the list >>
    elif art_channel_ids and message.channel.id in art_channel_ids:
        is_author_admin = await is_admin(member) # Check admin status

        # Check for valid media post (attachments OR embeds exist, BUT text content does NOT)
        has_media = bool(message.attachments or message.embeds) # Are there attachments or embeds?
        has_text = bool(message.content.strip()) # Is there non-whitespace text?

        is_valid_art_post = has_media and not has_text

        if is_valid_art_post:
            # Valid post, increment counter if user should be tracked
            if should_track:
                user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
                user_data["art_count"] = user_data.get("art_count", 0) + 1
                save_stats() # Save immediately after a valid art post
        elif not is_author_admin:
            # Invalid post (no media, or media + text) and not admin, delete silently
            try:
                await message.delete()
            except (discord.Forbidden, discord.HTTPException) as e:
                print(f"Error deleting invalid message in art channel: {e}") # English comment

        # Don't process commands or other logic for messages in the art channel
        return # Stop further processing for this channel

    # --- General Message Counter Update ---
    # This block is reached ONLY if the message was NOT in the twitter log or art channel
    else:
        if should_track:
            user_data = stats_data.setdefault(user_id, DEFAULT_USER_TEMPLATE())
            # Only increment total_message_count here
            user_data["total_message_count"] = user_data.get("total_message_count", 0) + 1
            # Periodic save for message count
            if user_data["total_message_count"] % 50 == 0:
                 save_stats()


    # --- Process Commands ---
    # Process commands ONLY if the message wasn't handled and returned by channel-specific logic above
    await bot.process_commands(message)


# --- GLOBAL ERROR HANDLING (Cog) ---
class GlobalErrorHandler(commands.Cog):
    """Cog to handle command errors globally."""
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        """Handles errors not caught by specific command error handlers."""

        # Avoid reprocessing CheckFailure/MissingPermissions potentially handled by SilentBot
        if isinstance(error, (commands.CheckFailure, commands.MissingPermissions)):
             # Optional: Log that it reached here, but don't send message to user
             # print(f"Silent error reached global handler: {type(error)}") # English comment
             return

        # Get the original error
        original_error = getattr(error, 'original', error)

        # Handle common user-facing errors (English messages)
        if isinstance(original_error, commands.CommandNotFound):
            # Silently ignore unknown commands
            pass
        elif isinstance(original_error, MissingRequiredArgument):
            await ctx.send(f"❌ Missing argument: `{original_error.param.name}`. Check `!thelp`.")
        elif isinstance(original_error, (BadArgument, commands.BadUnionArgument, RoleNotFound, MemberNotFound, commands.ChannelNotFound, commands.UserNotFound)):
            await ctx.send(f"❌ Invalid argument provided: {original_error}. Check `!thelp`.")
        elif isinstance(original_error, commands.CommandOnCooldown):
            await ctx.send(f"⏳ Command on cooldown. Try again in {original_error.retry_after:.1f}s.", delete_after=5)
        elif isinstance(original_error, commands.UserInputError): # Other user input errors
            await ctx.send(f"❌ Incorrect command usage: {original_error}. Check `!thelp`.")
        elif isinstance(original_error, commands.BotMissingPermissions):
            perms = ", ".join(original_error.missing_permissions).replace('_', ' ').title()
            await ctx.send(f"❌ Bot is missing required permissions: `{perms}`")
        elif isinstance(original_error, commands.NoPrivateMessage):
            await ctx.send("❌ This command cannot be used in private messages.")
        elif isinstance(original_error, discord.Forbidden): # General API permission error
            await ctx.send(f"❌ Discord API Error: Insufficient Permissions. Check bot roles/permissions.")
            print(f"Forbidden Error: Cmd: {ctx.command}, Err: {original_error}") # English comment
        elif isinstance(original_error, discord.HTTPException): # Other API errors
            await ctx.send(f"❌ Discord API Error: {original_error.status} - {original_error.text}")
            print(f"HTTP Error: Cmd: {ctx.command}, Status: {original_error.status}, Text: {original_error.text}") # English comment
        # Log all other unexpected errors to console
        else:
            print(f"Unhandled command error (Command: {ctx.command}): {original_error}") # English comment
            traceback.print_exception(type(original_error), original_error, original_error.__traceback__)
            # Send a generic error message
            try:
                await ctx.send("❌ An unexpected error occurred. Please contact an administrator.") # English text
            except Exception as e:
                print(f"Error sending 'unexpected error' message: {e}") # English comment


# --- BOT TOKEN & RUN ---
async def main():
    """Main async function to start the bot."""
    load_dotenv() # Load variables from .env file
    BOT_TOKEN = os.getenv('DISCORD_TOKEN')

    if not BOT_TOKEN:
        print("ERROR: Bot token not found in DISCORD_TOKEN environment variable.") # English text
        return

    async with bot:
        # Load data initially
        load_data()
        # Start the bot
        try:
            await bot.start(BOT_TOKEN)
        except discord.LoginFailure:
            print("ERROR: Invalid bot token.") # English text
        except discord.PrivilegedIntentsRequired:
            print("ERROR: Enable Privileged Intents (Members, Message Content) in the Developer Portal.") # English text
        except Exception as e:
            print(f"Unexpected error running bot: {e}") # English text
            traceback.print_exc()

if __name__ == "__main__":
    # Use asyncio.run() to start the bot's main function
    try:
        # Optional: Prevent ProactorEventLoop error on Windows
        # if os.name == 'nt':
        #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped manually.") # English text

