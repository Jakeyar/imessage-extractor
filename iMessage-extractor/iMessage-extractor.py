# --- CONFIG guard (ensures name exists for linters/runtime) ---
if 'CONFIG' not in globals():
    class CONFIG:
        NORTH_AMERICA_ONLY: bool = True
        SKIP_SHORT_CODES: bool = True
        SEARCH_ALTERNATIVE_LOCATIONS: bool = True
# -------------------------------------------------------------

#!/usr/bin/env python3
"""
iMessage Export Script for macOS
Safely exports iMessage chat history and attachments with full transparency.
Enhanced with resume capability and progress tracking.
"""

import sqlite3
import os
import shutil
import datetime
import logging
import sys
import time
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# Set to True to simulate without actually copying files or writing outputs
DRY_RUN = False

# Set to True to skip files that already exist (resume mode)
RESUME_MODE = True

# Paths
CHAT_DB_PATH = os.path.expanduser("~/Library/Messages/chat.db")
ATTACHMENTS_PATH = os.path.expanduser("~/Library/Messages/Attachments")
OUTPUT_DIR = "/Volumes/Coding/Python/iMessageExtractor/iMessageExport"

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Set up dual logging to console and file."""
    # Create output directory if it doesn't exist (even in dry run for logging)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    log_file = os.path.join(OUTPUT_DIR, "export_debug.log")
    
    # Create logger
    logger = logging.getLogger('imessage_export')
    logger.setLevel(logging.DEBUG)
    
    # Console handler - only show important info
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_format)
    
    # File handler - detailed logs
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')  # Append mode
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def sanitize_filename(name):
    """Sanitize a string to be safe for use as a filename/folder name."""
    if not name:
        return "Unknown"
    
    # Replace problematic characters
    problematic_chars = [':', '/', '\\', '<', '>', '"', '|', '?', '*']
    for char in problematic_chars:
        name = name.replace(char, '_')
    
    # Remove leading/trailing whitespace and dots
    name = name.strip(' .')
    
    # Limit length
    if len(name) > 100:
        name = name[:100]
    
    return name if name else "Unknown"

def format_timestamp(timestamp):
    """Convert Mac timestamp to readable format."""
    # Mac timestamps are seconds since 2001-01-01 00:00:00 UTC
    mac_epoch = datetime.datetime(2001, 1, 1)
    if timestamp:
        try:
            dt = mac_epoch + datetime.timedelta(seconds=timestamp / 1000000000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OverflowError):
            return "Unknown Time"
    return "Unknown Time"

def print_progress(current, total, operation="Processing"):
    """Print progress bar with percentage."""
    if total == 0:
        return
    
    percentage = (current / total) * 100
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    
    print(f"\r{operation}: [{bar}] {percentage:.1f}% ({current}/{total})", end='', flush=True)

def is_short_code_number(identifier):
    """Check if identifier is a short code (2FA, etc.) - 6 digits or less."""
    if not identifier:
        return False
    
    # Remove common formatting characters
    clean_number = identifier.replace('+', '').replace('-', '').replace(' ', '').replace('(', '').replace(')', '')
    
    # Check if it's all digits and 6 or fewer characters
    if clean_number.isdigit() and len(clean_number) <= 6:
        return True
    
    return False

def is_plugin_attachment(filename):
    """Check if this is a plugin/app attachment rather than a regular file."""
    if not filename:
        return False
    
    plugin_extensions = [
        '.pluginPayloadAttachment',  # iMessage app data
        '.balloon',                  # Message effects
        '.app',                      # App store links
        '.handwriting',              # Digital touch/handwriting
        '.digitaltouchdata'          # Digital touch data
    ]
    
    return any(filename.lower().endswith(ext.lower()) for ext in plugin_extensions)

def print_progress(current, total, operation="Processing"):
    """Print progress bar with percentage."""
    if total == 0:
        return
    
    percentage = (current / total) * 100
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    
    print(f"\r{operation}: [{bar}] {percentage:.1f}% ({current}/{total})", end='', flush=True)

# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

class iMessageExporter:
    def __init__(self, logger):
        self.logger = logger
        self.stats = {
            'conversations': 0,
            'messages': 0,
            'attachments_found': 0,
            'attachments_copied': 0,
            'attachments_skipped': 0,
            'attachments_failed': 0,
            'contacts_processed': 0
        }
        
    def connect_to_database(self):
        """Connect to the iMessage database."""
        if not os.path.exists(CHAT_DB_PATH):
            self.logger.error(f"❌ iMessage database not found at: {CHAT_DB_PATH}")
            self.logger.error("💡 Make sure you're running this on macOS with iMessage enabled")
            return None
    
    def connect_to_database(self):
        """Connect to the iMessage database."""
        if not os.path.exists(CHAT_DB_PATH):
            self.logger.error(f"❌ iMessage database not found at: {CHAT_DB_PATH}")
            self.logger.error("💡 Make sure you're running this on macOS with iMessage enabled")
            return None
            
        try:
            conn = sqlite3.connect(CHAT_DB_PATH)
            self.logger.info(f"✅ Connected to iMessage database")
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"❌ Failed to connect to database: {e}")
            if "unable to open database file" in str(e).lower():
                self.logger.error("")
                self.logger.error("🔒 PERMISSION ERROR - Full Disk Access Required!")
                self.logger.error("📋 To fix this:")
                self.logger.error("   1. Open System Settings → Privacy & Security → Full Disk Access")
                self.logger.error("   2. Click the + button")
                self.logger.error("   3. Add 'Terminal' (or your Python app)")
                self.logger.error("   4. Quit and reopen Terminal")
                self.logger.error("   5. Run this script again")
                self.logger.error("")
            return None
            
        try:
            conn = sqlite3.connect(CHAT_DB_PATH)
            self.logger.info(f"✅ Connected to iMessage database")
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"❌ Failed to connect to database: {e}")
            if "unable to open database file" in str(e).lower():
                self.logger.error("")
                self.logger.error("🔒 PERMISSION ERROR - Full Disk Access Required!")
                self.logger.error("📋 To fix this:")
                self.logger.error("   1. Open System Settings → Privacy & Security → Full Disk Access")
                self.logger.error("   2. Click the + button")
                self.logger.error("   3. Add 'Terminal' (or your Python app)")
                self.logger.error("   4. Quit and reopen Terminal")
                self.logger.error("   5. Run this script again")
                self.logger.error("")
            return None
    
    def get_contacts_and_chats(self, conn):
        """Get all contacts and chat information."""
        self.logger.info("🔍 Analyzing chat database...")
        
        # Get total count first
        count_query = "SELECT COUNT(DISTINCT chat_identifier) FROM chat"
        cursor = conn.cursor()
        cursor.execute(count_query)
        total_chats = cursor.fetchone()[0]
        
        query = """
        SELECT DISTINCT
            c.chat_identifier,
            c.display_name,
            c.room_name,
            h.id as phone_or_email
        FROM chat c
        LEFT JOIN chat_handle_join chj ON c.ROWID = chj.chat_id
        LEFT JOIN handle h ON chj.handle_id = h.ROWID
        ORDER BY c.chat_identifier
        """
        
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Group by chat identifier
            chats = {}
            for row in results:
                chat_id, display_name, room_name, phone_or_email = row
                
                if chat_id not in chats:
                    chats[chat_id] = {
                        'display_name': display_name or room_name,
                        'handles': set(),
                        'chat_identifier': chat_id
                    }
                
                if phone_or_email:
                    chats[chat_id]['handles'].add(phone_or_email)
            
            self.logger.info(f"📊 Found {len(chats)} conversations to process")
            return chats
            
        except sqlite3.Error as e:
            self.logger.error(f"❌ Error fetching contacts: {e}")
            return {}
    
    def get_messages_for_chat(self, conn, chat_identifier):
        """Get all messages for a specific chat."""
        query = """
        SELECT 
            m.date,
            m.text,
            m.is_from_me,
            h.id as sender_handle,
            m.ROWID as message_id
        FROM message m
        JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
        JOIN chat c ON cmj.chat_id = c.ROWID
        LEFT JOIN handle h ON m.handle_id = h.ROWID
        WHERE c.chat_identifier = ?
        ORDER BY m.date ASC
        """
        
        try:
            cursor = conn.cursor()
            cursor.execute(query, (chat_identifier,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.debug(f"Error fetching messages for {chat_identifier}: {e}")
            return []
    
    def get_attachments_for_message(self, conn, message_id):
        """Get all attachments for a specific message."""
        query = """
        SELECT 
            a.filename,
            a.transfer_name,
            a.mime_type,
            a.total_bytes
        FROM message_attachment_join maj
        JOIN attachment a ON maj.attachment_id = a.ROWID
        WHERE maj.message_id = ?
        """
        
        try:
            cursor = conn.cursor()
            cursor.execute(query, (message_id,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.debug(f"Error fetching attachments for message {message_id}: {e}")
            return []
    
    def create_contact_folder(self, chat_info):
        """Create and return the folder path for a contact."""
        # Determine the best display name
        display_name = chat_info.get('display_name', '')
        handles = list(chat_info.get('handles', []))
        chat_id = chat_info.get('chat_identifier', '')
        
        if display_name:
            if handles:
                folder_name = f"{display_name} ({handles[0]})"
            else:
                folder_name = display_name
        elif handles:
            folder_name = handles[0]
        else:
            folder_name = chat_id or "Unknown Contact"
        
        # Sanitize the folder name
        folder_name = sanitize_filename(folder_name)
        
        # Create folder path
        attachments_dir = os.path.join(OUTPUT_DIR, "attachments")
        contact_folder = os.path.join(attachments_dir, folder_name)
        
        if not DRY_RUN:
            os.makedirs(contact_folder, exist_ok=True)
        
        return contact_folder

    def export_conversations(self):
        """Main export function."""
        start_time = time.time()
        self.logger.info("🚀 Starting iMessage export process...")
        self.logger.info(f"📂 Output directory: {OUTPUT_DIR}")
        
        if CONFIG.NORTH_AMERICA_ONLY:
            self.logger.info("🇺🇸 REGION MODE - US/Canada numbers only")
        
        if CONFIG.SKIP_SHORT_CODES:
            self.logger.info("📱 FILTERING MODE - Skipping 2FA/short code conversations")
        
        if CONFIG.SEARCH_ALTERNATIVE_LOCATIONS:
            self.logger.info("🔍 ENHANCED MODE - Searching alternative locations for missing files")
        
        if RESUME_MODE:
            self.logger.info("⏭️  RESUME MODE - Skipping existing files")
        
        if DRY_RUN:
            self.logger.info("🧪 DRY RUN MODE - No files will be modified")
        
        # Connect to database
        conn = self.connect_to_database()
        if not conn:
            return False
        
        try:
            # Get all chats
            chats = self.get_contacts_and_chats(conn)
            if not chats:
                self.logger.warning("⚠️  No chats found in database")
                return False
            
            # Prepare output
            if not DRY_RUN:
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                os.makedirs(os.path.join(OUTPUT_DIR, "attachments"), exist_ok=True)
            
            messages_content = []
            total_chats = len(chats)
            current_chat = 0
            
            # Process each chat
            for chat_id, chat_info in chats.items():
                current_chat += 1
                
                # Show progress
                display_name = chat_info.get('display_name', chat_id)
                print_progress(current_chat, total_chats, f"Processing chats")
                
                # Get messages for this chat
                messages = self.get_messages_for_chat(conn, chat_id)
                if not messages:
                    continue
                
                self.stats['conversations'] += 1
                self.stats['contacts_processed'] += 1
                
                # Create contact folder for attachments
                contact_folder = self.create_contact_folder(chat_info)
                
                # Format conversation header with contact name
                display_name = chat_info.get('display_name', '')
                handles = list(chat_info.get('handles', []))
                
                # Try to get real contact name
                contact_name = None
                primary_handle = None
                for handle in handles:
                    contact_name = self.get_contact_name(handle)
                    if contact_name:
                        primary_handle = handle
                        break
                
                if not contact_name and chat_id:
                    contact_name = self.get_contact_name(chat_id)
                    primary_handle = chat_id
                
                # Build header
                if contact_name:
                    if primary_handle:
                        header = f"=== Conversation with {contact_name} ({primary_handle}) ==="
                    else:
                        header = f"=== Conversation with {contact_name} ==="
                elif display_name and handles:
                    header = f"=== Conversation with {display_name} ({handles[0]}) ==="
                elif display_name:
                    header = f"=== Conversation with {display_name} ==="
                elif handles:
                    header = f"=== Conversation with {handles[0]} ==="
                else:
                    header = f"=== Conversation with {chat_id} ==="
                
                messages_content.append(header)
                
                # Process each message
                for msg_date, msg_text, is_from_me, sender_handle, message_id in messages:
                    self.stats['messages'] += 1
                    
                    # Track message timeframe
                    if msg_date:
                        if self.stats['earliest_message'] is None or msg_date < self.stats['earliest_message']:
                            self.stats['earliest_message'] = msg_date
                        if self.stats['latest_message'] is None or msg_date > self.stats['latest_message']:
                            self.stats['latest_message'] = msg_date
                    
                    # Format timestamp
                    timestamp = format_timestamp(msg_date)
                    
                    # Determine sender with contact name resolution
                    if is_from_me:
                        sender = "You"
                    else:
                        # Try to get real contact name for sender
                        sender_contact_name = self.get_contact_name(sender_handle) if sender_handle else None
                        if sender_contact_name:
                            sender = sender_contact_name
                        else:
                            sender = chat_info.get('display_name') or sender_handle or "Unknown"
                    
                    # Get attachments for this message
                    attachments = self.get_attachments_for_message(conn, message_id)
                    
                    # Format message text
                    if msg_text and msg_text.strip():
                        messages_content.append(f"[{timestamp}] {sender}: {msg_text}")
                    
                    # Process attachments
                    for attachment in attachments:
                        self.stats['attachments_found'] += 1
                        
                        success, result = self.copy_attachment(attachment, contact_folder)
                        
                        if success == "plugin":
                            # Plugin attachment - log but don't treat as failure
                            messages_content.append(f"[{timestamp}] {sender}: [App Data: {result}]")
                        elif success:
                            self.stats['attachments_copied'] += 1
                            messages_content.append(f"[{timestamp}] {sender}: [Attachment: {result}]")
                        else:
                            self.stats['attachments_failed'] += 1
                            attachment_name = os.path.basename(attachment[0]) if attachment[0] else "unknown"
                            messages_content.append(f"[{timestamp}] {sender}: [Missing Attachment: {attachment_name}]")
                
                messages_content.append("")  # Empty line between conversations
            
            print()  # New line after progress bar
            
            # Write messages file
            self.logger.info("💾 Writing chat history...")
            messages_file = os.path.join(OUTPUT_DIR, "messages.txt")
            if not DRY_RUN:
                with open(messages_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(messages_content))
                self.logger.info(f"✅ Chat history exported to: messages.txt")
            else:
                self.logger.info(f"[DRY RUN] Would write messages to: messages.txt")
            
            # Calculate time taken
            elapsed_time = time.time() - start_time
            
            # Print summary
            self.print_summary(elapsed_time)
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during export: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False
        
        finally:
            conn.close()
    
    def print_summary(self, elapsed_time):
        """Print export summary statistics."""
        print("\n" + "="*60)
        print("📊 EXPORT SUMMARY")
        print("="*60)
        print(f"⏱️  Time taken: {elapsed_time:.1f} seconds")
        
        # Show timeframe
        if self.stats['earliest_message'] and self.stats['latest_message']:
            earliest = format_timestamp(self.stats['earliest_message'])
            latest = format_timestamp(self.stats['latest_message'])
            print(f"📅 Message timeframe: {earliest} to {latest}")
        
        print(f"👥 Contacts processed: {self.stats['contacts_processed']}")
        print(f"💬 Conversations exported: {self.stats['conversations']}")
        print(f"📝 Total messages: {self.stats['messages']}")
        print(f"📎 Attachments found: {self.stats['attachments_found']}")
        print(f"✅ Attachments copied: {self.stats['attachments_copied']}")
        if self.stats['plugin_attachments_skipped'] > 0:
            print(f"📱 App/plugin data skipped: {self.stats['plugin_attachments_skipped']}")
        if self.stats['attachments_found_alternative'] > 0:
            print(f"🔍 Attachments found via search: {self.stats['attachments_found_alternative']}")
        if self.stats['attachments_skipped'] > 0:
            print(f"⏭️  Attachments skipped (already exist): {self.stats['attachments_skipped']}")
        if self.stats['attachments_failed'] > 0:
            print(f"❌ Attachments failed: {self.stats['attachments_failed']}")
        
        if DRY_RUN:
            print("\n🧪 This was a DRY RUN - no files were actually modified")
        else:
            print(f"\n📂 Export completed! Check: {OUTPUT_DIR}")
        
        # Write failed files report
        if self.stats['failed_files'] and not DRY_RUN:
            try:
                failed_file = os.path.join(OUTPUT_DIR, "failed_attachments.txt")
                with open(failed_file, 'w', encoding='utf-8') as f:
                    f.write("FAILED ATTACHMENTS REPORT\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(f"Total failed: {len(self.stats['failed_files'])}\n\n")
                    for i, failed in enumerate(self.stats['failed_files'], 1):
                        f.write(f"{i}. {failed}\n")
                print(f"📋 Failed attachments logged to: failed_attachments.txt")
            except Exception as e:
                print(f"⚠️  Could not write failed attachments file: {e}")
                # At least show some examples in console
                print(f"💡 Sample failed files:")
                for failed in self.stats['failed_files'][:5]:
                    print(f"   - {failed}")
                if len(self.stats['failed_files']) > 5:
                    print(f"   ... and {len(self.stats['failed_files']) - 5} more")
        elif self.stats['failed_files']:
            # Show examples even in dry run
            print(f"💡 Sample failed files (first 5):")
            for failed in self.stats['failed_files'][:5]:
                print(f"   - {failed}")
            if len(self.stats['failed_files']) > 5:
                print(f"   ... and {len(self.stats['failed_files']) - 5} more")
        
        # Log to file
        self.logger.debug(f"Export completed - {self.stats}")
    
    def copy_attachment(self, attachment_info, destination_folder):
        """Copy an attachment to the destination folder."""
        filename, transfer_name, mime_type, total_bytes = attachment_info
        
        if not filename:
            return False, "No filename provided"
        
        # The filename in the database is usually a path
        source_path = os.path.expanduser(filename)
        
        # Extract just the filename for destination
        dest_filename = os.path.basename(filename)
        if not dest_filename:
            dest_filename = transfer_name or "unknown_attachment"
        
        dest_path = os.path.join(destination_folder, dest_filename)
        
        # Check if file already exists (resume mode)
        if RESUME_MODE and os.path.exists(dest_path):
            self.stats['attachments_skipped'] += 1
            return True, dest_filename  # Skip, but count as success
        
        if not os.path.exists(source_path):
            return False, f"Source file not found: {source_path}"
        
        try:
            if not DRY_RUN:
                shutil.copy2(source_path, dest_path)
            
            return True, dest_filename
        
        except (IOError, OSError) as e:
            return False, f"Copy failed: {e}"
    
    def export_conversations(self):
        """Main export function."""
        start_time = time.time()
        self.logger.info("🚀 Starting iMessage export process...")
        self.logger.info(f"📂 Output directory: {OUTPUT_DIR}")
        
        if CONFIG.NORTH_AMERICA_ONLY:
            self.logger.info("🇺🇸 REGION MODE - US/Canada numbers only")
        
        if CONFIG.SKIP_SHORT_CODES:
            self.logger.info("📱 FILTERING MODE - Skipping 2FA/short code conversations")
        
        if CONFIG.SEARCH_ALTERNATIVE_LOCATIONS:
            self.logger.info("🔍 ENHANCED MODE - Searching alternative locations for missing files")
        
        if RESUME_MODE:
            self.logger.info("⏭️  RESUME MODE - Skipping existing files")
        
        if DRY_RUN:
            self.logger.info("🧪 DRY RUN MODE - No files will be modified")
        
        # Connect to database
        conn = self.connect_to_database()
        if not conn:
            return False
        
        try:
            # Get all chats
            chats = self.get_contacts_and_chats(conn)
            if not chats:
                self.logger.warning("⚠️  No chats found in database")
                return False
            
            # Prepare output
            if not DRY_RUN:
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                os.makedirs(os.path.join(OUTPUT_DIR, "attachments"), exist_ok=True)
            
            messages_content = []
            total_chats = len(chats)
            current_chat = 0
            
            # Process each chat
            for chat_id, chat_info in chats.items():
                current_chat += 1
                
                # Show progress
                display_name = chat_info.get('display_name', chat_id)
                print_progress(current_chat, total_chats, f"Processing chats")
                
                # Get messages for this chat
                messages = self.get_messages_for_chat(conn, chat_id)
                if not messages:
                    continue
                
                self.stats['conversations'] += 1
                self.stats['contacts_processed'] += 1
                
                # Create contact folder for attachments
                contact_folder = self.create_contact_folder(chat_info)
                
                # Format conversation header with contact name
                display_name = chat_info.get('display_name', '')
                handles = list(chat_info.get('handles', []))
                
                # Try to get real contact name
                contact_name = None
                primary_handle = None
                for handle in handles:
                    contact_name = self.get_contact_name(handle)
                    if contact_name:
                        primary_handle = handle
                        break
                
                if not contact_name and chat_id:
                    contact_name = self.get_contact_name(chat_id)
                    primary_handle = chat_id
                
                # Build header
                if contact_name:
                    if primary_handle:
                        header = f"=== Conversation with {contact_name} ({primary_handle}) ==="
                    else:
                        header = f"=== Conversation with {contact_name} ==="
                elif display_name and handles:
                    header = f"=== Conversation with {display_name} ({handles[0]}) ==="
                elif display_name:
                    header = f"=== Conversation with {display_name} ==="
                elif handles:
                    header = f"=== Conversation with {handles[0]} ==="
                else:
                    header = f"=== Conversation with {chat_id} ==="
                
                messages_content.append(header)
                
                # Process each message
                total_messages = len(messages)
                for msg_idx, (msg_date, msg_text, is_from_me, sender_handle, message_id) in enumerate(messages):
                    self.stats['messages'] += 1
                    
                    # Track message timeframe
                    if msg_date:
                        if self.stats['earliest_message'] is None or msg_date < self.stats['earliest_message']:
                            self.stats['earliest_message'] = msg_date
                        if self.stats['latest_message'] is None or msg_date > self.stats['latest_message']:
                            self.stats['latest_message'] = msg_date
                    
                    # Format timestamp
                    timestamp = format_timestamp(msg_date)
                    
                    # Determine sender with contact name resolution
                    if is_from_me:
                        sender = "You"
                    else:
                        # Try to get real contact name for sender
                        sender_contact_name = self.get_contact_name(sender_handle) if sender_handle else None
                        if sender_contact_name:
                            sender = sender_contact_name
                        else:
                            sender = chat_info.get('display_name') or sender_handle or "Unknown"
                    
                    # Get attachments for this message
                    attachments = self.get_attachments_for_message(conn, message_id)
                    
                    # Format message text
                    if msg_text and msg_text.strip():
                        messages_content.append(f"[{timestamp}] {sender}: {msg_text}")
                    
                    # Process attachments
                    for attachment in attachments:
                        self.stats['attachments_found'] += 1
                        
                        success, result = self.copy_attachment(attachment, contact_folder)
                        
                        if success == "plugin":
                            # Plugin attachment - log but don't treat as failure
                            messages_content.append(f"[{timestamp}] {sender}: [App Data: {result}]")
                        elif success:
                            if result not in [a[0] for a in [att for att in attachments if self.stats['attachments_skipped'] > 0]]:
                                self.stats['attachments_copied'] += 1
                            messages_content.append(f"[{timestamp}] {sender}: [Attachment: {result}]")
                        else:
                            self.stats['attachments_failed'] += 1
                            attachment_name = os.path.basename(attachment[0]) if attachment[0] else "unknown"
                            messages_content.append(f"[{timestamp}] {sender}: [Missing Attachment: {attachment_name}]")
                
                messages_content.append("")  # Empty line between conversations
            
            print()  # New line after progress bar
            
            # Write messages file
            self.logger.info("💾 Writing chat history...")
            messages_file = os.path.join(OUTPUT_DIR, "messages.txt")
            if not DRY_RUN:
                with open(messages_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(messages_content))
                self.logger.info(f"✅ Chat history exported to: messages.txt")
            else:
                self.logger.info(f"[DRY RUN] Would write messages to: messages.txt")
            
            # Calculate time taken
            elapsed_time = time.time() - start_time
            
            # Print summary
            self.print_summary(elapsed_time)
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during export: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False
        
        finally:
            conn.close()
    
    def print_summary(self, elapsed_time):
        """Print export summary statistics."""
        print("\n" + "="*60)
        print("📊 EXPORT SUMMARY")
        print("="*60)
        print(f"⏱️  Time taken: {elapsed_time:.1f} seconds")
        
        # Show timeframe
        if self.stats['earliest_message'] and self.stats['latest_message']:
            earliest = format_timestamp(self.stats['earliest_message'])
            latest = format_timestamp(self.stats['latest_message'])
            print(f"📅 Message timeframe: {earliest} to {latest}")
        
        print(f"👥 Contacts processed: {self.stats['contacts_processed']}")
        print(f"💬 Conversations exported: {self.stats['conversations']}")
        print(f"📝 Total messages: {self.stats['messages']}")
        print(f"📎 Attachments found: {self.stats['attachments_found']}")
        print(f"✅ Attachments copied: {self.stats['attachments_copied']}")
        if self.stats['plugin_attachments_skipped'] > 0:
            print(f"📱 App/plugin data skipped: {self.stats['plugin_attachments_skipped']}")
        if self.stats['attachments_found_alternative'] > 0:
            print(f"🔍 Attachments found via search: {self.stats['attachments_found_alternative']}")
        if self.stats['attachments_skipped'] > 0:
            print(f"⏭️  Attachments skipped (already exist): {self.stats['attachments_skipped']}")
        if self.stats['attachments_failed'] > 0:
            print(f"❌ Attachments failed: {self.stats['attachments_failed']}")
        
        if DRY_RUN:
            print("\n🧪 This was a DRY RUN - no files were actually modified")
        else:
            print(f"\n📂 Export completed! Check: {OUTPUT_DIR}")
        
        # Write failed files report
        if self.stats['failed_files'] and not DRY_RUN:
            try:
                failed_file = os.path.join(OUTPUT_DIR, "failed_attachments.txt")
                with open(failed_file, 'w', encoding='utf-8') as f:
                    f.write("FAILED ATTACHMENTS REPORT\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(f"Total failed: {len(self.stats['failed_files'])}\n\n")
                    for i, failed in enumerate(self.stats['failed_files'], 1):
                        f.write(f"{i}. {failed}\n")
                print(f"📋 Failed attachments logged to: failed_attachments.txt")
            except Exception as e:
                print(f"⚠️  Could not write failed attachments file: {e}")
                # At least show some examples in console
                print(f"💡 Sample failed files:")
                for failed in self.stats['failed_files'][:5]:
                    print(f"   - {failed}")
                if len(self.stats['failed_files']) > 5:
                    print(f"   ... and {len(self.stats['failed_files']) - 5} more")
        elif self.stats['failed_files']:
            # Show examples even in dry run
            print(f"💡 Sample failed files (first 5):")
            for failed in self.stats['failed_files'][:5]:
                print(f"   - {failed}")
            if len(self.stats['failed_files']) > 5:
                print(f"   ... and {len(self.stats['failed_files']) - 5} more")
        
        # Log to file
        self.logger.debug(f"Export completed - {self.stats}")
    
    def export_conversations(self):
        """Main export function."""
        start_time = time.time()
        self.logger.info("🚀 Starting iMessage export process...")
        self.logger.info(f"📂 Output directory: {OUTPUT_DIR}")
        
        if DRY_RUN:
            self.logger.info("🧪 DRY RUN MODE - No files will be modified")
        
        if RESUME_MODE:
            self.logger.info("⏭️  RESUME MODE - Skipping existing files")
        
        # Connect to database
        conn = self.connect_to_database()
        if not conn:
            return False
        
        try:
            # Get all chats
            chats = self.get_contacts_and_chats(conn)
            if not chats:
                self.logger.warning("⚠️  No chats found in database")
                return False
            
            # Prepare output
            if not DRY_RUN:
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                os.makedirs(os.path.join(OUTPUT_DIR, "attachments"), exist_ok=True)
            
            messages_content = []
            total_chats = len(chats)
            current_chat = 0
            
            # Process each chat
            for chat_id, chat_info in chats.items():
                current_chat += 1
                
                # Show progress
                display_name = chat_info.get('display_name', chat_id)
                print_progress(current_chat, total_chats, f"Processing chats")
                
                # Get messages for this chat
                messages = self.get_messages_for_chat(conn, chat_id)
                if not messages:
                    continue
                
                self.stats['conversations'] += 1
                self.stats['contacts_processed'] += 1
                
                # Create contact folder for attachments
                contact_folder = self.create_contact_folder(chat_info)
                
                # Format conversation header
                display_name = chat_info.get('display_name', '')
                handles = list(chat_info.get('handles', []))
                if display_name and handles:
                    header = f"=== Conversation with {display_name} ({handles[0]}) ==="
                elif display_name:
                    header = f"=== Conversation with {display_name} ==="
                elif handles:
                    header = f"=== Conversation with {handles[0]} ==="
                else:
                    header = f"=== Conversation with {chat_id} ==="
                
                messages_content.append(header)
                
                # Process each message
                total_messages = len(messages)
                for msg_idx, (msg_date, msg_text, is_from_me, sender_handle, message_id) in enumerate(messages):
                    self.stats['messages'] += 1
                    
                    # Format timestamp
                    timestamp = format_timestamp(msg_date)
                    
                    # Determine sender
                    if is_from_me:
                        sender = "You"
                    else:
                        sender = chat_info.get('display_name') or sender_handle or "Unknown"
                    
                    # Get attachments for this message
                    attachments = self.get_attachments_for_message(conn, message_id)
                    
                    # Format message text
                    if msg_text and msg_text.strip():
                        messages_content.append(f"[{timestamp}] {sender}: {msg_text}")
                    
                    # Process attachments
                    for attachment in attachments:
                        self.stats['attachments_found'] += 1
                        
                        success, result = self.copy_attachment(attachment, contact_folder)
                        
                        if success:
                            if result not in [a[0] for a in [att for att in attachments if self.stats['attachments_skipped'] > 0]]:
                                self.stats['attachments_copied'] += 1
                            messages_content.append(f"[{timestamp}] {sender}: [Attachment: {result}]")
                        else:
                            self.stats['attachments_failed'] += 1
                            attachment_name = os.path.basename(attachment[0]) if attachment[0] else "unknown"
                            messages_content.append(f"[{timestamp}] {sender}: [Missing Attachment: {attachment_name}]")
                
                messages_content.append("")  # Empty line between conversations
            
            print()  # New line after progress bar
            
            # Write messages file
            self.logger.info("💾 Writing chat history...")
            messages_file = os.path.join(OUTPUT_DIR, "messages.txt")
            if not DRY_RUN:
                with open(messages_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(messages_content))
                self.logger.info(f"✅ Chat history exported to: messages.txt")
            else:
                self.logger.info(f"[DRY RUN] Would write messages to: messages.txt")
            
            # Calculate time taken
            elapsed_time = time.time() - start_time
            
            # Print summary
            self.print_summary(elapsed_time)
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during export: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False
        
        finally:
            conn.close()
    
    def print_summary(self, elapsed_time):
        """Print export summary statistics."""
        print("\n" + "="*60)
        print("📊 EXPORT SUMMARY")
        print("="*60)
        print(f"⏱️  Time taken: {elapsed_time:.1f} seconds")
        
        # Show timeframe
        if self.stats['earliest_message'] and self.stats['latest_message']:
            earliest = format_timestamp(self.stats['earliest_message'])
            latest = format_timestamp(self.stats['latest_message'])
            print(f"📅 Message timeframe: {earliest} to {latest}")
        
        print(f"👥 Contacts processed: {self.stats['contacts_processed']}")
        print(f"💬 Conversations exported: {self.stats['conversations']}")
        print(f"📝 Total messages: {self.stats['messages']}")
        print(f"📎 Attachments found: {self.stats['attachments_found']}")
        print(f"✅ Attachments copied: {self.stats['attachments_copied']}")
        if self.stats['plugin_attachments_skipped'] > 0:
            print(f"📱 App/plugin data skipped: {self.stats['plugin_attachments_skipped']}")
        if self.stats['attachments_found_alternative'] > 0:
            print(f"🔍 Attachments found via search: {self.stats['attachments_found_alternative']}")
        if self.stats['attachments_skipped'] > 0:
            print(f"⏭️  Attachments skipped (already exist): {self.stats['attachments_skipped']}")
        if self.stats['attachments_failed'] > 0:
            print(f"❌ Attachments failed: {self.stats['attachments_failed']}")
        
        if DRY_RUN:
            print("\n🧪 This was a DRY RUN - no files were actually modified")
        else:
            print(f"\n📂 Export completed! Check: {OUTPUT_DIR}")
        
        # Write failed files report
        if self.stats['failed_files'] and not DRY_RUN:
            failed_file = os.path.join(OUTPUT_DIR, "failed_attachments.txt")
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write("FAILED ATTACHMENTS REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Total failed: {len(self.stats['failed_files'])}\n\n")
                for i, failed in enumerate(self.stats['failed_files'], 1):
                    f.write(f"{i}. {failed}\n")
            print(f"📋 Failed attachments logged to: failed_attachments.txt")
        
        # Log to file
        self.logger.debug(f"Export completed - {self.stats}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function to run the export process."""
    print("📱 iMessage Export Script v2.0")
    print("=" * 50)
    
    # Setup logging
    logger = setup_logging()
    
    # Create exporter instance
    exporter = iMessageExporter(logger)
    
    # Check if we're on macOS
    if not sys.platform.startswith('darwin'):
        logger.warning("⚠️  This script is designed for macOS")
    
    # Run export
    success = exporter.export_conversations()
    
    if success:
        print("✅ Export completed successfully!")
        return 0
    else:
        print("❌ Export failed!")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
