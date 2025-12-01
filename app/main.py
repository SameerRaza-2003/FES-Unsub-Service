import os
import json
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient, errors
from dotenv import load_dotenv
import httpx
import logging

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
UNSUBSCRIBE_REDIRECT_URL = os.getenv("UNSUBSCRIBE_REDIRECT_URL", None)

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("unsubscribe_service")

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(
    title="Unsubscribe & SES SNS Service",
    description="Handles manual unsubscribes and SES SNS events (bounce, complaint, delivery).",
    version="1.0.0"
)

# ----------------------------
# MongoDB setup
# ----------------------------
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    contacts_collection = db["contacts_email_tool"]
    sns_logs_collection = db["sns_logs"]  # For auditing SNS events
    client.server_info()  # Test connection
except errors.ServerSelectionTimeoutError as e:
    logger.error(f"Could not connect to MongoDB: {e}")
    raise e

# ----------------------------
# Templates
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# ----------------------------
# Routes
# ----------------------------

@app.get("/")
async def home():
    return {"status": "Unsubscribe & SNS service is running."}


@app.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(request: Request, email: str = None):
    """
    Manual unsubscribe endpoint
    """
    if not email:
        raise HTTPException(status_code=400, detail="Missing email parameter")

    update_result = contacts_collection.update_one(
        {"email": email},
        {"$set": {
            "unsubscribed": True,
            "unsubscribed_at": datetime.utcnow(),
            "source": "manual"
        }},
        upsert=True
    )

    logger.info(f"Manual unsubscribe for {email}, modified count: {update_result.modified_count}")

    if UNSUBSCRIBE_REDIRECT_URL:
        return RedirectResponse(url=UNSUBSCRIBE_REDIRECT_URL)

    return templates.TemplateResponse(
        "unsubscribed.html",
        {"request": request, "email": email}
    )


@app.post("/sns/notifications")
async def sns_notifications(request: Request):
    """
    Endpoint to handle SES SNS notifications for bounce, complaint, delivery
    """
    try:
        raw_body = await request.body()
        data = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        logger.error(f"Failed to parse request body: {e}")
        return Response(status_code=400)

    message_type = request.headers.get("x-amz-sns-message-type")
    if not message_type:
        logger.error("Missing x-amz-sns-message-type header")
        return Response(status_code=400)

    # Log incoming SNS event
    sns_logs_collection.insert_one({
        "message_type": message_type,
        "body": data,
        "received_at": datetime.utcnow()
    })

    # ----------------------------
    # 1️⃣ Subscription confirmation
    # ----------------------------
    if message_type == "SubscriptionConfirmation":
        subscribe_url = data.get("SubscribeURL")
        if subscribe_url:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.get(subscribe_url)
                logger.info(f"Subscription confirmed via {subscribe_url}")
            except Exception as e:
                logger.error(f"Failed to confirm subscription: {e}")
        return {"status": "Subscription confirmation processed"}

    # ----------------------------
    # 2️⃣ Notification events
    # ----------------------------
    if message_type == "Notification":
        try:
            message = json.loads(data["Message"])
            notification_type = message.get("notificationType")
        except Exception as e:
            logger.error(f"Failed to parse SNS Message: {e}")
            return Response(status_code=400)

        if notification_type == "Bounce":
            for recipient in message.get("bounce", {}).get("bouncedRecipients", []):
                email = recipient.get("emailAddress")
                if email:
                    contacts_collection.update_one(
                        {"email": email},
                        {"$set": {
                            "unsubscribed": True,
                            "bounce": True,
                            "unsubscribed_at": datetime.utcnow(),
                            "source": "bounce"
                        }},
                        upsert=True
                    )
                    logger.info(f"Bounce processed for {email}")

        elif notification_type == "Complaint":
            for recipient in message.get("complaint", {}).get("complainedRecipients", []):
                email = recipient.get("emailAddress")
                if email:
                    contacts_collection.update_one(
                        {"email": email},
                        {"$set": {
                            "unsubscribed": True,
                            "complaint": True,
                            "unsubscribed_at": datetime.utcnow(),
                            "source": "complaint"
                        }},
                        upsert=True
                    )
                    logger.info(f"Complaint processed for {email}")

        elif notification_type == "Delivery":
            for email in message.get("delivery", {}).get("recipients", []):
                contacts_collection.update_one(
                    {"email": email},
                    {"$set": {
                        "last_delivered_at": datetime.utcnow(),
                        "source_delivery": "ses_delivery"
                    }},
                    upsert=True
                )
                logger.info(f"Delivery logged for {email}")

        else:
            logger.warning(f"Unknown notificationType: {notification_type}")

        return {"status": "SNS notification processed"}

    logger.warning(f"Unhandled SNS message type: {message_type}")
    return Response(status_code=400)
