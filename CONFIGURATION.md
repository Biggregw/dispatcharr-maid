CONFIGURATION SETUP GUIDE
=========================

This guide explains how to set up your private configuration files.

IMPORTANT: These files are ignored by git to protect your privacy!
------------------------------------------------------------------

The following files contain YOUR data and are NOT committed to GitHub:
- provider_names.json (your custom provider names)
- provider_map.json (auto-generated from Dispatcharr API)
- config.yaml (your channel groups and settings)
- jobs/ directory (your job history and workspaces)
- csv/ directory (your stream analysis data)
- .env file (your Dispatcharr credentials)

FIRST-TIME SETUP
================

Step 1: Copy Example Files
---------------------------
When you first clone this repository, copy the example files:

cd ~/dispatcharr-maid

# Copy and customize configuration
cp config.yaml.example config.yaml

# Copy and customize provider names (optional but recommended)
cp provider_names.json.example provider_names.json


Step 2: Configure .env File
----------------------------
Create your .env file with Dispatcharr credentials:

# Create .env file
nano .env

# Add these lines (replace with your actual values):
DISPATCHARR_BASE_URL=http://localhost:9191
DISPATCHARR_USER=your_username
DISPATCHARR_PASS=your_password
DISPATCHARR_TOKEN=  # Leave empty, will be auto-filled


Step 3: Configure config.yaml
------------------------------
Edit config.yaml to match your setup:

nano config.yaml

# Update these sections:
filters:
  channel_group_ids: [123, 456]  # Your actual group IDs
  specific_channel_ids: []  # Optional: specific channels


Step 4: Configure provider_names.json (OPTIONAL)
-------------------------------------------------
This file lets you assign custom names to your providers.

nano provider_names.json

# Format (provider ID → display name):
{
  "1": "My Main Provider",
  "2": "Backup Provider",
  "3": "Premium Streams",
  "4": "Sports Specialist"
}

How to find provider IDs:
1. Run any job in dispatcharr-maid
2. Check the results page
3. Note the provider IDs shown
4. Add them to provider_names.json with your preferred names

If you skip this step, providers will show their Dispatcharr names
or fall back to "Provider 1", "Provider 2", etc.


KEEPING YOUR DATA PRIVATE
==========================

The .gitignore File
-------------------
The repository includes a .gitignore file that prevents these
sensitive files from being committed to git:

✓ provider_names.json - Your private provider mappings
✓ provider_map.json - Auto-generated API data
✓ config.yaml - Your channel groups and settings
✓ jobs/ - Your job history with provider info
✓ csv/ - Your stream analysis results
✓ .env - Your Dispatcharr credentials

What Gets Committed to GitHub
------------------------------
✓ provider_names.json.example - Template for others to copy
✓ config.yaml.example - Template configuration
✓ All Python code files
✓ Documentation and guides
✓ README.md
✓ docker-compose files

What NEVER Gets Committed
--------------------------
✗ Your actual provider names
✗ Your Dispatcharr credentials
✗ Your channel group IDs
✗ Your job history
✗ Your stream analysis data


VERIFICATION
============

Before Committing to GitHub
----------------------------
Always check what will be committed:

git status

# Should NOT see:
# - provider_names.json
# - provider_map.json
# - config.yaml
# - .env
# - jobs/
# - csv/

# Should see:
# - provider_names.json.example
# - config.yaml.example
# - .gitignore
# - *.py files


If You Accidentally Commit Sensitive Data
------------------------------------------
If you accidentally commit sensitive files:

# Remove from git but keep local file
git rm --cached provider_names.json
git rm --cached config.yaml

# Commit the removal
git commit -m "Remove sensitive configuration files"

# Push the fix
git push origin main

Note: This only removes from future commits. The data may still
exist in git history. If you exposed credentials, change them!


UPDATING YOUR FORK
==================

When pulling updates from the main repository:

git pull origin main

Your local files (.env, config.yaml, provider_names.json) won't
be overwritten because they're ignored by git.

However, check for updates to example files:
- config.yaml.example
- provider_names.json.example

These may have new features or settings you want to add to your
actual config.yaml or provider_names.json.


FOR CONTRIBUTORS
================

If you're contributing code:

1. Never commit your actual config files
2. Update .example files if you add new config options
3. Test with fresh example files before submitting PR
4. Document any new configuration in this guide


TROUBLESHOOTING
===============

"Git wants to commit provider_names.json"
------------------------------------------
This means .gitignore isn't working. Possible causes:

1. File was committed before .gitignore was added:
   git rm --cached provider_names.json
   git commit -m "Remove provider_names.json"

2. .gitignore is incorrect:
   Check that provider_names.json is listed in .gitignore

3. Wrong directory:
   Make sure you're in the repository root


"I lost my config after git pull"
----------------------------------
Your config files should never be lost because they're ignored.
If they're gone:

1. Check if you're in the right directory
2. Restore from backup if you have one
3. Recreate from .example files


"Provider names showing after git push"
---------------------------------------
Check GitHub repository - click on files tab:
- Should NOT see provider_names.json
- Should see provider_names.json.example

If you see provider_names.json:
1. Follow "If You Accidentally Commit Sensitive Data" above
2. Consider changing provider account names in Dispatcharr


SUMMARY
=======

✓ Copy .example files to create your config
✓ Never commit actual config files
✓ .gitignore protects your privacy automatically
✓ Always verify with 'git status' before pushing
✓ Update .example files when adding new features

Your provider information and settings stay private! 🔒

