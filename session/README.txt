Use SESSION_STRING for Render or other headless deployments.

Recommended:
1. Generate a Telethon StringSession locally.
2. Put it into .env as SESSION_STRING.
3. Deploy the project.

If SESSION_STRING is empty, the app will try interactive login in console.
