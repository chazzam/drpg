from __future__ import annotations

import logging
from pathlib import Path
import json # For config saving/loading
import sys
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
import pyperclip # Added for clipboard access

# Import core drpg components
import os # Needed for default db path logic
from drpg.config import Config
from drpg.sync import DrpgSync
# Reuse the default db path logic if possible, or replicate it
# Let's replicate for simplicity here to avoid potential import cycles/issues
def _default_db_path_tui() -> Path:
    # Simple cross-platform default in user's home directory or config dir
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return config_home / "drpg" / "library.db"

from textual.app import App, ComposeResult
from textual.containers import Container, VerticalScroll
from textual.logging import TextualHandler # For TUI logging
from textual.app import App, ComposeResult
from textual.containers import Container, VerticalScroll
from textual.events import Key # Added for on_key
from textual.logging import TextualHandler # For TUI logging
from textual.reactive import var
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Log, Static, Switch, Label, ProgressBar # Added ProgressBar

# Configuration file path
CONFIG_FILE = Path.home() / ".drpg_tui_config.json"

# Default configuration
DEFAULT_CONFIG = {
    "library_path": str(Path.home() / "DRPG_TUI_Downloads"),
    "api_token": "", # Default to empty
    "use_checksums": False,
    "validate": False,
    "compatibility_mode": False,
    "omit_publisher": False,
    "threads": 5,
    "log_level": "INFO",
    "dry_run": False,
    "db_path": str(_default_db_path_tui()), # Add default db_path
}

# --- Configuration Loading/Saving ---
def load_config() -> dict:
    """Loads configuration from JSON file, returning defaults if not found or invalid."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                data = json.load(f)
                # Ensure all keys are present, add defaults if missing
                config = DEFAULT_CONFIG.copy()
                # Ensure loaded keys exist in defaults before updating
                valid_data = {k: v for k, v in data.items() if k in config}
                config.update(valid_data) # Overwrite defaults with loaded data
                # Ensure specific types
                config["threads"] = int(config.get("threads", 5))
                # Ensure paths are strings for JSON, but we'll convert later
                config["library_path"] = str(config.get("library_path", DEFAULT_CONFIG["library_path"]))
                config["db_path"] = str(config.get("db_path", DEFAULT_CONFIG["db_path"]))
                return config
        except (json.JSONDecodeError, IOError, ValueError, TypeError) as e: # Added TypeError
            # Use basic print for early errors before logging might be set up
            print(f"Error loading config file {CONFIG_FILE}: {e}. Using defaults.", file=sys.stderr)
            return DEFAULT_CONFIG.copy()
    else:
        return DEFAULT_CONFIG.copy()

def save_config(config_data: dict) -> None:
    """Saves configuration to JSON file, ensuring paths are strings."""
    try:
        # Create a copy to modify for saving
        save_data = config_data.copy()
        # Ensure paths are strings
        if isinstance(save_data.get("library_path"), Path):
             save_data["library_path"] = str(save_data["library_path"])
        if isinstance(save_data.get("db_path"), Path):
             save_data["db_path"] = str(save_data["db_path"])

        # Ensure parent directory for config file exists
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(CONFIG_FILE, "w") as f:
            json.dump(save_data, f, indent=4)
        logging.info(f"Configuration saved to {CONFIG_FILE}")
    except IOError as e:
        logging.error(f"Error saving config file {CONFIG_FILE}: {e}")

# --- Screens ---

class MainScreen(Screen):
    """Main screen with options."""
    BINDINGS = [
        ("s", "show_settings", "Settings"),
        ("q", "request_quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        config_data = self.app.config_data
        with Container(id="main-container"):
            yield Header()
            yield Label(f"Library Path: {config_data.get('library_path', 'Not Set')}", id="library-path-display")
            # Disable sync button if token is not set
            yield Button(
                "Sync Library",
                id="sync",
                variant="primary",
                disabled=not config_data.get("api_token")
            )
            yield Button("Settings", id="settings")
            yield Button("Quit", id="quit", variant="error")
            yield Footer()

    def on_mount(self) -> None:
        """Check token status on mount."""
        self.update_sync_button_status()

    def update_sync_button_status(self) -> None:
        """Enable/disable sync button based on API token presence."""
        try:
            sync_button = self.query_one("#sync", Button)
            sync_button.disabled = not self.app.config_data.get("api_token")
        except Exception as e:
            logging.error(f"Could not update sync button status: {e}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses on the main screen."""
        if event.button.id == "quit":
            self.app.action_quit()
        elif event.button.id == "sync":
            if self.app.config_data.get("api_token"):
                self.app.push_screen(SyncScreen())
            else:
                self.app.notify("API Token not set. Please configure in Settings.", title="Error", severity="error")
        elif event.button.id == "settings":
            self.app.action_show_settings()

    def action_show_settings(self) -> None:
        """Action to switch to the settings screen."""
        self.app.push_screen(SettingsScreen())

    def action_request_quit(self) -> None:
        """Action to request quitting the app."""
        self.app.action_quit()

    def update_library_path_display(self) -> None:
        """Updates the library path display."""
        try:
            path_display = self.query_one("#library-path-display", Label)
            path_display.update(f"Library Path: {self.app.config_data.get('library_path', 'Not Set')}")
            self.update_sync_button_status() # Also update sync button status
        except Exception as e:
            logging.error(f"Could not update main screen path display: {e}")


class SettingsScreen(Screen):
    """Screen for configuring DRPG settings."""
    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("ctrl+s", "save_settings", "Save"),
        # ("ctrl+v", "paste_api_token", "Paste Token"), # Removed screen binding, will handle in on_key
    ]

    def on_key(self, event: Key) -> None:
        """Handle key presses for the settings screen."""
        # Check if Ctrl+V was pressed and the API token input is focused
        if event.key == "ctrl+v":
            api_token_input = self.query_one("#api_token", Input)
            if self.app.focused is api_token_input:
                logging.info("Ctrl+V detected while API token input focused.")
                self.action_paste_api_token()
                event.prevent_default() # Stop the Input widget from also handling it

    def compose(self) -> ComposeResult:
        config_data = self.app.config_data
        yield Header()
        with VerticalScroll(id="settings-form"):
            yield Label("API Token:", classes="label")
            yield Input(config_data.get("api_token", ""), id="api_token", password=True)

            yield Label("Library Path:", classes="label")
            yield Input(config_data.get("library_path", ""), id="library_path")

            yield Label("Number of Download Threads:", classes="label")
            yield Input(str(config_data.get("threads", 5)), id="threads", type="integer")

            # TODO: Add log level selection (e.g., Select widget)
            yield Label(f"Log Level: {config_data.get('log_level', 'INFO')}", classes="label")

            with Container(classes="switch-container"):
                yield Switch(config_data.get("use_checksums", False), id="use_checksums")
                yield Label("Use Checksums (Slower, Precise)", classes="label inline")

            with Container(classes="switch-container"):
                yield Switch(config_data.get("validate", False), id="validate")
                yield Label("Validate Downloads (Uses Checksums)", classes="label inline")

            with Container(classes="switch-container"):
                yield Switch(config_data.get("compatibility_mode", False), id="compatibility_mode")
                yield Label("Use DriveThruRPG Naming Compatibility", classes="label inline")

            with Container(classes="switch-container"):
                yield Switch(config_data.get("omit_publisher", False), id="omit_publisher")
                yield Label("Omit Publisher Name in Path", classes="label inline")

            with Container(classes="switch-container"):
                yield Switch(config_data.get("dry_run", False), id="dry_run")
                yield Label("Dry Run (Don't Download)", classes="label inline")

        yield Button("Save", id="save", variant="success")
        yield Button("Back", id="back", variant="default")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "save":
            self.action_save_settings()

    def action_save_settings(self) -> None:
        new_config = self.app.config_data.copy()
        new_config["api_token"] = self.query_one("#api_token", Input).value
        new_config["library_path"] = self.query_one("#library_path", Input).value
        try:
            threads_value = int(self.query_one("#threads", Input).value)
            if threads_value > 0:
                new_config["threads"] = threads_value
            else:
                 self.app.notify("Threads must be a positive number.", title="Error", severity="error")
                 return
        except ValueError:
            self.app.notify("Invalid number for threads.", title="Error", severity="error")
            return

        new_config["use_checksums"] = self.query_one("#use_checksums", Switch).value
        new_config["validate"] = self.query_one("#validate", Switch).value
        new_config["compatibility_mode"] = self.query_one("#compatibility_mode", Switch).value
        new_config["omit_publisher"] = self.query_one("#omit_publisher", Switch).value
        new_config["dry_run"] = self.query_one("#dry_run", Switch).value

        # Preserve db_path (it's not editable here)
        new_config["db_path"] = self.app.config_data.get("db_path", DEFAULT_CONFIG["db_path"])

        # TODO: Update log level

        self.app.config_data = new_config # Update app's config
        save_config(self.app.config_data) # Save the updated config
        self.app.notify("Settings saved.", title="Success")

        try:
            main_screen = self.app.get_screen("main")
            main_screen.update_library_path_display()
        except Exception as e:
            logging.warning(f"Could not update main screen path display after save: {e}")

        self.app.pop_screen()

    def action_paste_api_token(self) -> None:
        """Paste clipboard content into the API token input."""
        try:
            clipboard_content = pyperclip.paste()
            if clipboard_content:
                api_token_input = self.query_one("#api_token", Input)
                api_token_input.value = clipboard_content
                self.app.notify("API Token pasted from clipboard.", title="Pasted")
            else:
                self.app.notify("Clipboard is empty.", title="Info", severity="information")
        except pyperclip.PyperclipException as e:
            logging.error(f"Clipboard error: {e}")
            error_message = f"Could not access clipboard: {e}"
            # Check if on Linux and provide a more specific hint
            if sys.platform.startswith('linux'):
                error_message += "\n\nOn Linux, this often means 'xclip' or 'xsel' is not installed.\nTry: sudo apt install xclip (or equivalent for your distribution)."
            # Use a longer timeout for the more detailed message
            self.app.notify(error_message, title="Clipboard Error", severity="error", timeout=10)
        except Exception as e: # Catch potential query errors etc.
            logging.error(f"Error during paste action: {e}")
            self.app.notify("An unexpected error occurred during paste.", title="Error", severity="error")


class SyncScreen(Screen):
    """Screen for displaying sync progress and logs."""
    BINDINGS = [
        ("escape", "request_pop_screen", "Back (if finished)"),
    ]

    sync_running = var(False) # Reactive variable to track sync status

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Syncing library...", id="sync-status")
        # TODO: Add ProgressBar widget
        yield Log(id="sync-log", highlight=True)
        yield Footer()

    def on_mount(self) -> None:
        """Start the sync process when the screen is mounted."""
        self.query_one("#sync-log", Log).clear() # Clear log on new sync
        self.run_sync_worker()

    def run_sync_worker(self) -> None:
        """Runs the DrpgSync logic in a background worker thread."""
        if self.sync_running:
            self.app.notify("Sync is already in progress.", title="Info")
            return

        log_widget = self.query_one("#sync-log", Log)
        status_widget = self.query_one("#sync-status", Static)
        status_widget.update("Starting sync...")
        self.sync_running = True

        # --- Prepare Config for DrpgSync ---
        # Create a Config object instance
        try:
            # Convert paths back to Path objects for DrpgSync
            library_path = Path(self.app.config_data["library_path"]).expanduser()
            db_path = Path(self.app.config_data["db_path"]).expanduser()

            # Ensure db directory exists before sync
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create Config instance dynamically
            sync_config = Config()
            for key, value in self.app.config_data.items():
                if hasattr(sync_config, key):
                    # Special handling for paths
                    if key == "library_path":
                        setattr(sync_config, key, library_path)
                    elif key == "db_path":
                         setattr(sync_config, key, db_path)
                    # Ensure threads is int
                    elif key == "threads":
                         setattr(sync_config, key, int(value))
                    else:
                        setattr(sync_config, key, value)
        except (KeyError, ValueError, TypeError, Exception) as e: # Catch potential errors during config creation
            log_widget.write(f"[bold red]Error creating config:[/bold red] {e}")
            status_widget.update("[bold red]Sync failed (Config Error)[/bold red]")
            self.sync_running = False
            return

        # --- Run Sync in Worker ---
        try:
            drpg_syncer = DrpgSync(sync_config)
            # Pass the log widget write method to the worker
            self.app.run_worker(
                self.sync_thread_target, # The function to run in the worker
                drpg_syncer,             # Argument for the target function
                thread=True,             # Run in a separate thread
                exclusive=True,          # Prevent other workers running
                group="sync_worker",     # Group for potential management
                description="Library synchronization",
            )
        except Exception as e:
            log_widget.write(f"[bold red]Error starting sync worker:[/bold red] {e}")
            status_widget.update("[bold red]Sync failed (Worker Error)[/bold red]")
            self.sync_running = False

    def sync_thread_target(self, syncer: DrpgSync) -> None:
        """The actual function executed by the background worker."""
        log_widget = self.query_one("#sync-log", Log)
        status_widget = self.query_one("#sync-status", Static)

        # --- Logging Setup for Worker Thread ---
        # Get the root logger used by drpg modules
        drpg_logger = logging.getLogger("drpg")
        # Use TextualHandler to forward logs to the widget
        # Note: TextualHandler is not thread-safe directly, use QueueHandler
        log_queue = Queue(-1) # Infinite queue size
        queue_handler = QueueHandler(log_queue)
        # Add handler ONLY for the duration of the sync
        drpg_logger.addHandler(queue_handler)
        # Set level based on config (ensure it's valid)
        log_level_name = self.app.config_data.get("log_level", "INFO").upper()
        log_level = getattr(logging, log_level_name, logging.INFO)
        drpg_logger.setLevel(log_level)

        # Listener thread in the main app thread to process the queue
        listener = QueueListener(log_queue, log_widget) # Pass log widget directly
        listener.start()

        # --- Execute Sync ---
        try:
            status_widget.update("Syncing...") # Update status via call_from_thread if needed, but direct might work
            syncer.sync() # This blocks the worker thread
            self.app.call_from_thread(status_widget.update, "[bold green]Sync finished![/bold green]")
        except Exception as e:
            # Log the exception to the TUI log widget
            drpg_logger.exception("An error occurred during synchronization.")
            self.app.call_from_thread(status_widget.update, f"[bold red]Sync failed:[/bold red] {e}")
        finally:
            # --- Cleanup ---
            self.sync_running = False
            # Stop the listener and remove the handler
            listener.stop()
            drpg_logger.removeHandler(queue_handler)
            # Reset logger level if necessary, or assume it's managed elsewhere
            # self.app.call_from_thread(self.enable_back_button) # Example

    def action_request_pop_screen(self) -> None:
        """Allow popping screen only if sync is not running."""
        if not self.sync_running:
            self.app.pop_screen()
        else:
            self.app.notify("Sync is still running.", title="Warning", severity="warning")


# --- Main App ---

class DrpgTuiApp(App[None]):
    """A Textual app to manage DriveThruRPG downloads."""

    CSS_PATH = "tui.css"
    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
    ]
    SCREENS = {
        "main": MainScreen,
        # SettingsScreen & SyncScreen pushed dynamically
    }
    MODES = {
        "main": "main",
    }

    def __init__(self):
        super().__init__()
        self.config_data = load_config()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.push_screen("main")

    def action_quit(self) -> None:
        """Action to quit the application."""
        # TODO: Check if sync is running and ask for confirmation?
        self.exit()

    def action_show_settings(self) -> None:
        """Pushes the settings screen."""
        self.push_screen(SettingsScreen())


# --- Entry Point ---

def run_tui() -> None:
    """Configure logging and run the Textual TUI application."""
    log_filename = Path.home() / ".drpg_tui.log" # Log to home dir
    # Configure root logger - TextualHandler will capture logs sent here
    # File logging for persistent logs
    logging.basicConfig(
        level=logging.INFO, # Set a base level; DrpgSync might override based on config
        handlers=[
            logging.FileHandler(log_filename, mode='a'),
            TextualHandler(), # This handler is used by Textual's Log widget capture
        ],
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True # Override any existing config
    )
    logging.info("--- Starting DRPG TUI Application ---")

    # Set httpx log level based on initial config or default
    # This needs to be done *after* basicConfig
    config = load_config()
    app_log_level_name = config.get("log_level", "INFO").upper()
    app_log_level = getattr(logging, app_log_level_name, logging.INFO)
    if app_log_level <= logging.DEBUG:
        httpx_log_level = logging.DEBUG
        httpx_deps_log_level = logging.INFO
    else:
        httpx_log_level = logging.WARNING
        httpx_deps_log_level = logging.WARNING

    logging.getLogger("httpx").setLevel(httpx_log_level)
    for name in ("httpcore", "hpack"):
        logging.getLogger(name).setLevel(httpx_deps_log_level)
    # Apply application key filter globally if needed, or ensure it's applied in cmd.py's setup
    # from drpg.cmd import application_key_filter # Might need adjustment if cmd is not imported
    # logging.getLogger("httpx").addFilter(application_key_filter) # Add filter if needed globally

    app = DrpgTuiApp()
    app.run()
    logging.info("--- Exiting DRPG TUI Application ---")


if __name__ == "__main__":
    run_tui()
