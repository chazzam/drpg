# dRPG downloads and keeps your purchases from DriveThruRPG up to date
[![Maintainability](https://api.codeclimate.com/v1/badges/b3128ba6938f92088135/maintainability)](https://codeclimate.com/github/glujan/drpg/maintainability)
![PyPI](https://img.shields.io/pypi/v/drpg?label=drpg)

## Installation

This script runs with Python 3.9 and newer.

You can install dRPG from PyPI:
```bash
pip install --user drpg
drpg --help  # For the command-line interface
drpg-tui     # To launch the new Terminal UI
```

This package provides two interfaces:
*   `drpg`: The original command-line interface (CLI).
*   `drpg-tui`: A new Terminal User Interface (TUI) for a more interactive experience.

## Usage (CLI)

1. Go to [your account settings](https://www.drivethrurpg.com/en/account/settings)
   and generate a new application key in the "Library App Keys" section.
2. Copy the key and run the script: `drpg --token <YOUR_DRPG_TOKEN>` - or set
   `DRPG_TOKEN` env variable and run `drpg`.
3. The script will download your library. Initial synchronization may take a while.
   On consecutive runs, it will only download new and changed files.

## Usage (TUI)

1. Launch the TUI by running: `drpg-tui`
2. Navigate to the `Settings` screen using the button or the `s` key.
3. Enter your DriveThruRPG API key, configure the desired library path, number of download threads, validation settings, and other options.
4. Save the settings (Ctrl+S or the Save button). The settings are stored in `~/.drpg_tui_config.json`.
5. Go back to the main screen (Escape or Back button).
6. Press the `Sync Library` button to start downloading/synchronizing your purchases. Progress and logs will be displayed directly in the TUI during the sync process.

## Compatibility

Because of the nature of using an undocumented API, this software may break
without a notice. Version number indicates a year and a month when the software
was proved to be working with a real DriveThruRPG account.

### File name compatibility

The DriveThruRPG client does some interesting things with the names of directories.
For example, if you buy a product from publisher "Game Designers' Workshop (GDW)"
the DriveThruRPG client app will download it to a directory with the unwieldy name
"Game Designers__039_ Workshop _GDW_".

By default, `drpg` gives directories more user friendly name. In the example above,
the directory would be "Game Designers' Workshop (GDW)". However, this causes a
problem if you intend to try to manage the same e-book library using both `drpg` and
the DriveThruRPG client app. When you run the former, you'll get a friendly name,
then when you run the latter it will download all the same files again and put them
in a directory with the unfriendly name.

You can use the command line option `--compatibility-mode` to make `drpg` use the
same naming scheme for files and directories as the DriveThruRPG client. We have
also done our best to imitate DriveThruRPG's bugs while in `--compatibility-mode`
but I'm sure there are some we missed.


### Advanced options

You can change where your files will be downloaded by using `--library-path
path/to/your/directory`.

By default the script does not compare files by md5 checksum to save time. You
can turn it on by using `--use-checksums`.

You can change a log level by using `--log-level=<YOUR_LOG_LEVEL>`. Choices are
DEBUG, INFO, WARNING, ERROR, CRITICAL.

You can do a "dry run" of the app by specifying `--dry-run`. This will determine
all the digital content you have purchased, but instead of downloading each file
it will print one line of information to show what file *would* have been downloaded
if the `--dry-run` flag wasn't on. Use this if you want to test out the app without
taking the time to download anything.

You can validate downloads by calculating checksums after download using `--validate` / `-v`.

You can specify the number of parallel download threads using `--threads <number>` / `-x <number>` (defaults to 5).

You can change the location of the library metadata database using `--db-path path/to/your/database.db`. Defaults to `~/.drpg/library.db`.

For more information, run the script with `--help`.

## Found a bug?

Pull requests and bug reports are welcomed! See [CONTRIBUTING.md](CONTRIBUTING.md)
for more details.
