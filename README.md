# TrackingDB
Automation Pipeline for Tracking DB for high/low volume submissions

## Configuration

`.env` file (required)
This project reads configuration from a local `.env` file. 

Password management with Keyring (required)
This pipeline uses the keyring library to securely retrieve database passwords from the OS credential store instead of storing them in source code or .env.
How it works
At runtime, the script does:
* reads KEYRING_SERVICE_* and DB_USER_* from .env
* fetches password using:

keyring.get_password(SERVICE_NAME, USERNAME)

So the only secret is the password stored in your OS keyring.

Setting credentials in Keyring
https://jamie-southchurch.medium.com/storing-and-accessing-passwords-in-python-using-keyring-windows-10-and-linux-648cf07beef6

You must create keyring entries for each database login you use, using the exact pair:
* Service name: the KEYRING_SERVICE_* value in .env
* Username: the DB_USER_* value in .env
Windows (Credential Manager)
1. Open Credential Manager
2. Click Windows Credentials → Add a generic credential
3. Internet or network address = your keyring service name (example: mcr_PrepPlus)
4. User name = DB username (example: pp_mcrtracking_r)
5. Password = DB password


Running the pipeline
Run from the repository root:

## python main.py


Email (optional)
Email notifications are controlled via .env (for example EMAIL_ENABLED=true/false).
Emails are sent using an internal SMTP relay. If email is enabled, make sure:
* the SMTP host/port in .env is reachable from where you run this script
* the Email_Assignment table has ODS → email mappings
Common email-related failure modes:
* ODS is missing for a FIN (ODS_Assignment not mapped)
* ODS exists but no corresponding email in Email_Assignment
* SMTP relay not reachable from your machine/network

Logs (recommended)
Add logging so you can see:
* what files were found (new + retry)
* status of each file while processing
* whether email was sent or skipped (and why)
