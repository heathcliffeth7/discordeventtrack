# 📊 discordeventtrack

A Discord bot designed to manage and track event participation and winners within your server.  
It supports admin-only commands, role-based filtering, and automatic Excel export for easy reporting.

---

## 🚀 Features

- Add or remove users from events  
- Mark users as winners  
- Fix wrong entries (joined/won/not joined)  
- Copy users from a voice channel into an event  
- Export full statistics to Excel (`.xlsx`)  
- View individual user stats  
- Role-based permission system  
- Auto-updates member tracking every 30 minutes  

---

## 🔧 Setup & Requirements

1. Make sure you have **Python 3.8+** installed.
2. Install required libraries:
   ```bash
   pip install discord.py xlsxwriter

  AUTHORIZED_ROLES = [role_id_1, role_id_2]  # Can use commands
TARGET_ROLES = [role_id_3, role_id_4]      # Will be included in statistics
