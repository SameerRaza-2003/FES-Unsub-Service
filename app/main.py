import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient
from dotenv import load_dotenv

# ----------------------------
# Load .env
# ----------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
UNSUBSCRIBE_REDIRECT_URL = os.getenv("UNSUBSCRIBE_REDIRECT_URL", None)

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(
    title="Unsubscribe Service",
    description="Microservice to mark emails as unsubscribed",
    version="1.0.0"
)

# ----------------------------
# MongoDB
# ----------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
contacts_collection = db["contacts_email_tool"]

# ----------------------------
# Templates
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")  # <-- fixed path
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# ----------------------------
# Routes
# ----------------------------
@app.get("/")
def home():
    return {"status": "Unsubscribe service is running."}


@app.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(request: Request, email: str = None):
    if not email:
        raise HTTPException(status_code=400, detail="Missing email parameter")

    # Update contact if exists
    contact = contacts_collection.find_one({"email": email})
    if contact:
        contacts_collection.update_one(
            {"email": email},
            {"$set": {"unsubscribed": True}}
        )

    # Redirect if set
    if UNSUBSCRIBE_REDIRECT_URL:
        return RedirectResponse(url=UNSUBSCRIBE_REDIRECT_URL)

    # Render HTML template
    return templates.TemplateResponse(
        "unsubscribed.html",
        {"request": request, "email": email}
    )
