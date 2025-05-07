# Discord Event & Stats Tracker Bot

## Description

This Discord bot is designed to track user participation in events, monitor specific channels for Twitter links and art submissions, and provide comprehensive statistics. It features role-based access control for commands, data persistence through a JSON file, and the ability to generate detailed Excel reports.

## Features

* **Event Tracking**:
    * Add users to events (mark as 'joined').
    * Mark users as winners for events.
    * Remove users from events.
    * Delete event records.
    * Copy members from voice/stage channels to an event's 'joined' list.
    * List event winners and participants.
    * Fix/correct event participation or winner status.
* **Twitter Link Monitoring**:
    * Designate specific channels for logging X.com (formerly Twitter) links.
    * Automatically scan history of newly added log channels for past links.
    * Prevent duplicate link posting by non-admin users in log channels.
    * Track unique links posted by users.
* **Art Channel Monitoring**:
    * Designate specific channels as 'Art Channels'.
    * In Art Channels, only allow posts with media (attachments/embeds) and no accompanying text from non-admin users.
    * Count valid art posts for tracked users.
    * Automatically scan history of newly added art channels for past art posts and update counts.
* **User Statistics**:
    * Display individual user statistics including:
        * Total messages sent.
        * Number of unique Twitter links posted.
        * Number of events joined.
        * Number of events won.
        * Number of art posts.
    * Access stats via the `!stats` command or a persistent "Show My Stats" button.
    * Role-based cooldowns for the stats button.
* **Excel Reporting**:
    * Generate comprehensive Excel reports (`.xlsx`) of user statistics.
    * Filter reports by:
        * Message count, tweet count, events joined/won, art count (using operators like `>`, `<`, `=`, `>=`, `<=`, `!=`).
        * Art count ranges (e.g., `5<artcount<10`).
        * User roles (must have a specific role, must *not* have a specific role).
    * Sort reports by message count or tweet count.
    * List and delete generated Excel files.
* **Role Management & Access Control**:
    * Define "Track Authorized Roles" who can use administrative bot commands.
    * Define "Track Target Roles" to specify which users' activities and stats are tracked. If no target roles are set, all non-bot users are tracked.
    * Define "Stats Authorized Roles" who can use the `!stats` command (in addition to Track Authorized Roles).
* **Bulk Actions**:
    * Bulk ban users by ID with an optional reason.
    * Bulk assign a role to multiple users by ID.
* **Configuration**:
    * Manage authorized roles, target roles, log channels, art channels, and stats button settings via commands.
    * All configuration and user data are saved to a `stats.json` file.
* **Error Handling**:
    * Silent handling of permission errors for certain user actions (e.g., deleting their own message).
    * Global error handler for commands, providing user-friendly feedback.
* **Data Persistence**:
    * Statistics and configuration are saved in `stats.json`.
    * Data is loaded on bot startup and saved after relevant changes.

## Prerequisites

* Python 3.8+
* A Discord Bot Token
* Discord.py library and other dependencies (see `requirements.txt` if available, or install manually).

## Setup & Installation

1.  **Clone the repository or download the `bot.py` file.**
    ```bash
    # If it were a git repository
    # git clone <repository_url>
    # cd <repository_name>
    ```
2.  **Install Dependencies**:
    It's highly recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install discord.py python-dotenv xlsxwriter
    ```
    (Note: Based on the imports in `bot.py`: `discord`, `json`, `os`, `xlsxwriter`, `datetime`, `re`, `asyncio`, `string`, `dotenv`, `time`).

3.  **Create a `.env` file** in the same directory as `bot.py` and add your Discord bot token:
    ```env
    DISCORD_TOKEN=YOUR_BOT_TOKEN_HERE
    ```

4.  **Configure Privileged Intents**:
    Ensure your bot has the following Privileged Gateway Intents enabled in the Discord Developer Portal:
    * **Presence Intent** (Might not be strictly needed based on current code, but good for future expansion)
    * **Server Members Intent** (Crucial for accessing member information, roles, etc.)
    * **Message Content Intent** (Crucial for reading message content for commands, links, etc.)

5.  **Run the bot**:
    ```bash
    python bot.py
    ```

## Configuration

The bot is configured primarily through Discord commands. Key configurations are stored in `stats.json`.

* **Initial Setup**:
    1.  Invite the bot to your Discord server with necessary permissions (Manage Roles, Ban Members, Read Messages, Send Messages, Manage Messages, Read Message History).
    2.  Use `!settrackauthorizedrole <@role>` to grant a role permission to use admin commands. The bot owner always has admin permissions.
    3.  (Optional) Use `!settracktargetrole <@role1> [@role2...]` to specify which roles should be tracked. If not set, all non-bot members are tracked.

* **Key Configuration Commands (for users with Track Authorized Role)**:
    * `!settrackauthorizedrole <@role or ID>`: Adds a role to the list of roles that can use administrative commands.
    * `!settracktargetrole <@role or ID> [@role2...]`: Sets roles whose members' activities will be tracked.
    * `!removeauthorizedrole <@role or ID> [@role2...]`: Removes a role from track authorized list.
    * `!removetargetrole <@role or ID> [@role2...]`: Removes a role from track target list.
    * `!setstatsroleauthorized <@role or ID> [@role2...]`: Allows members with this role to use the `!stats` command.
    * `!removestatsroleauthorized <@role or ID> [@role2...]`: Removes `!stats` command access for the role.
    * `!twitterlog <#channel or ID>`: Designates a channel for X.com link logging.
    * `!removetwitterlog <#channel or ID>`: Removes a channel from X.com link logging.
    * `!setartchannel <#channel or ID>` (alias: `!addartchannel`): Designates a channel for art submissions and counting.
    * `!removeartchannel <#channel or ID>`: Removes a channel from art monitoring.
    * `!setstatschannel <#channel or ID>`: Sends/moves the "Show My Stats" button to the specified channel.
    * `!setstatscooldown <@role or ID> <duration>`: Sets a cooldown (e.g., `5m`, `1h`, `2d`, `0` for no cooldown) for the stats button for a specific role.

## Usage (Key Commands)

*(Default prefix: `!`)*

**Admin Commands (requires Track Authorized Role or Bot Owner)**:

* **Help**:
    * `!trackhelp` (or `!thelp`): Displays the list of all admin commands.
* **Statistics & Reporting**:
    * `!allstats [filters...] [sort_key]`: Generates an Excel report of all tracked users.
        * *Filters*: `msgcount>N`, `twtcount>=N`, `joined<=N`, `won=N`, `artcount>N`, `5<artcount<10`, `@RoleNameOrID`, `nothaverole @RoleNameOrID`.
        * *Sort Keys*: `messages`, `tweets`.
    * `!filteruserid [filters...] [id]`: Lists user IDs (and names, unless `id` is specified) matching filters.
    * `!listexcels`: Lists generated Excel files currently on the server.
    * `!deleteexcel <filename.xlsx>`: Deletes a specific Excel file.
* **Event Management**:
    * `!addevent <event_name> <id1> [id2...]`: Adds users to an event.
    * `!eventwinners <event_name> <id1> [id2...]`: Marks users as winners for an event.
    * `!notjoined <event_name> <id1> [id2...]`: Removes users from an event.
    * `!delevent <event_name>`: Deletes an event and all its records.
    * `!copyevent <#voice/stage_channel or ID> <event_name>`: Adds all members from a voice/stage channel to an event.
    * `!winnerlist <event_name>`: Sends a text file listing event winners.
    * `!joinedlist <event_name>`: Sends a text file listing users who joined but didn't win an event.
    * `!fixwinners <joined|notjoined> <event_name> <id...>`: Modifies status of winners.
    * `!fixjoined <winner|notjoined> <event_name> <id...>`: Modifies status of joined members.
    * `!fixnotjoined <joined|winner> <event_name> <id...>`: Adds users not in event to joined/winner.
* **Bulk Actions**:
    * `!bulkban <id1> [id2...] [reason=...]`: Bans multiple users by ID.
    * `!bulkgiverole <@role or ID> <id1> [id2...]`: Assigns a role to multiple users by ID.
* **(Configuration commands listed in "Configuration" section above)**

**User Commands**:

* `!stats [@user or ID]`: Shows statistics for the mentioned user, or yourself if no user is specified. (Requires Stats Authorized Role, Track Authorized Role, or being the bot owner).
* **"Show My Stats" Button**: If configured by an admin (`!setstatschannel`), users can click this button in the designated channel to see their own stats (subject to role-based cooldowns).

## Data Persistence

* All user statistics (message counts, events, winners, Twitter links, art counts), posted Twitter links (global set), and bot configuration (authorized roles, target roles, log channels, art channels, stats button settings, cooldowns) are stored in a JSON file named `stats.json`.
* This file is created automatically if it doesn't exist.
* Data is loaded when the bot starts and saved to the file whenever significant changes occur (e.g., new event entry, configuration change, periodic message count save).

## Error Handling

* The bot includes a global error handler for commands, providing feedback for common issues like missing arguments, invalid inputs, permissions, or command cooldowns.
* For certain actions like deleting messages in restricted channels (e.g., duplicate links in Twitter log, invalid posts in art channels by non-admins), the bot attempts to perform the action silently and logs errors to the console if deletion fails.
* Unhandled errors are printed to the console with a traceback, and a generic error message is sent to the user.

## Dependencies

* **discord.py**: The main library for Discord API interaction.
* **python-dotenv**: For managing environment variables (like the bot token).
* **XlsxWriter**: For generating Excel (`.xlsx`) files.
* Standard Python libraries: `json`, `os`, `datetime`, `re`, `asyncio`, `time`, `string`, `traceback`.
