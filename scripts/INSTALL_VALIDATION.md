# Database Validation Script Installation

This script validates the production database daily and sends notifications if validation fails.

## Installation Steps

1. **Copy systemd service and timer files:**
   ```bash
   sudo cp scripts/ctv-db-validation.service /etc/systemd/system/
   sudo cp scripts/ctv-db-validation.timer /etc/systemd/system/
   ```

2. **Create environment file for email notification settings:**
   ```bash
   # Copy the template file
   sudo cp scripts/ctv-db-validation.env.template /etc/ctv-db-validation.env
   
   # Edit with your email settings
   sudo nano /etc/ctv-db-validation.env
   ```
   
   Fill in the email notification settings (see template for examples):
   ```bash
   # SMTP Server Configuration
   SMTP_SERVER=smtp.example.com
   SMTP_PORT=587
   SMTP_USE_TLS=true
   SMTP_SENDER_EMAIL=noreply@example.com
   SMTP_RECIPIENT_EMAILS=admin@example.com,team@example.com
   
   # Optional: SMTP Authentication (if required)
   SMTP_USERNAME=your-smtp-username
   SMTP_PASSWORD=your-smtp-password
   ```
   
   **Note:** For local mail server, you can use:
   ```bash
   SMTP_SERVER=localhost
   SMTP_PORT=25
   SMTP_USE_TLS=false
   SMTP_SENDER_EMAIL=noreply@ctv.local
   SMTP_RECIPIENT_EMAILS=admin@example.com
   ```

3. **Reload systemd and enable the timer:**
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable ctv-db-validation.timer
   sudo systemctl start ctv-db-validation.timer
   ```

4. **Verify the timer is active:**
   ```bash
   sudo systemctl status ctv-db-validation.timer
   sudo systemctl list-timers | grep ctv-db-validation
   ```

5. **Test the service manually:**
   ```bash
   sudo systemctl start ctv-db-validation.service
   sudo systemctl status ctv-db-validation.service
   ```

## Schedule

The validation runs daily at **3:00 AM** with a randomized delay of up to 15 minutes to avoid system load spikes.

## What It Validates

- ✅ Database integrity (PRAGMA integrity_check)
- ✅ Spot counts for closed years (2021-2024)
- ✅ All 12 months are closed for each year

## Notifications

If validation fails, email notifications are sent to the addresses specified in `SMTP_RECIPIENT_EMAILS`.

**Required environment variables:**
- `SMTP_SERVER` - SMTP server hostname (default: localhost)
- `SMTP_PORT` - SMTP server port (default: 25)
- `SMTP_SENDER_EMAIL` - Email address to send from
- `SMTP_RECIPIENT_EMAILS` - Comma-separated list of recipient email addresses

**Optional environment variables:**
- `SMTP_USE_TLS` - Use TLS encryption (true/false, default: false)
- `SMTP_USERNAME` - SMTP username for authentication
- `SMTP_PASSWORD` - SMTP password for authentication

## Logs

Logs are written to:
- Console output (captured by systemd journal)
- File: `/opt/apps/ctv-bookedbiz-db/logs/db_validation_YYYYMMDD.log` (if writable)

View logs:
```bash
# View recent logs
sudo journalctl -u ctv-db-validation.service -n 50

# View logs for today
sudo journalctl -u ctv-db-validation.service --since today
```

