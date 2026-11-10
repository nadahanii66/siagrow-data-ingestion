import logging
import os
import smtplib
import time
from email.message import EmailMessage

from pipeline import config

log = logging.getLogger("alerts")


def send_alert_email(subject, body):
    msg = EmailMessage()
    msg["Subject"] = f"[Siagrow Alert] {subject}"
    msg["From"] = os.getenv("SMTP_USER")
    msg["To"] = os.getenv("ALERT_TO")
    msg.set_content(body)

    for attempt in range(1, 4):
        try:
            with smtplib.SMTP(os.getenv("SMTP_HOST"), int(os.getenv("SMTP_PORT", "587")), timeout=20) as server:
                server.starttls()
                server.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD"))
                server.send_message(msg)
            log.info("Alert email sent: %s", subject)
            return True
        except Exception as error:
            log.warning("Alert email attempt %d failed: %s", attempt, error)
            if attempt < 3:
                time.sleep(2 ** attempt)
    log.error("Could not send alert email after 3 attempts: %s", subject)
    return False


if __name__ == "__main__":
    print(config.ROOT)
    ok = send_alert_email("Test", "Hello from the Siagrow pipeline. If you can read this, alerts work.")
    print("Sent!" if ok else "Failed, check the log above")