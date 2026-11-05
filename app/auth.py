import base64
import hashlib
import io
import json
import logging
import os
import re
import secrets
import smtplib
import sqlite3
import time
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional, Tuple

import httpx
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("gateway.auth")
logger.setLevel(logging.INFO)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "gateway.db")

APP_ENV = os.getenv("APP_ENV", "production").strip().lower()
IS_DEV_ENV = APP_ENV in ("dev", "development", "local", "test")
COOKIE_SECURE = not IS_DEV_ENV
OTP_MAX_REQUESTS_PER_EMAIL = 5
OTP_MAX_REQUESTS_PER_IP = 20
OTP_REQUEST_WINDOW_MINUTES = 15
OTP_MAX_VERIFY_ATTEMPTS = 5
OAUTH_STATE_TTL_SECONDS = 600

# OAuth tokens are encrypted at rest. Set GATEWAY_OAUTH_ENCRYPTION_KEY to a
# Fernet key to allow rotating GATEWAY_ADMIN_TOKEN without losing stored tokens;
# otherwise the key is derived from the admin token for a zero-config default.
_ENC_PREFIX = "enc:v1:"


def _oauth_cipher() -> Fernet:
    configured = os.getenv("GATEWAY_OAUTH_ENCRYPTION_KEY", "").strip()
    if configured:
        return Fernet(configured.encode("utf-8"))
    seed = os.getenv("GATEWAY_ADMIN_TOKEN", "")
    derived = base64.urlsafe_b64encode(hashlib.sha256(f"oauth-tokens:{seed}".encode("utf-8")).digest())
    return Fernet(derived)


def encrypt_oauth_secret(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    if value.startswith(_ENC_PREFIX):
        return value
    return _ENC_PREFIX + _oauth_cipher().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_oauth_secret(value: Optional[str]) -> Optional[str]:
    if not value or not value.startswith(_ENC_PREFIX):
        return value
    try:
        return _oauth_cipher().decrypt(value[len(_ENC_PREFIX):].encode("utf-8")).decode("utf-8")
    except InvalidToken:
        logger.error("Stored OAuth token could not be decrypted; the encryption key changed.")
        return None


def public_user(user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Returns a user dict safe to send to clients (never includes the password hash)."""
    if not user:
        return None
    return {k: v for k, v in user.items() if k != "password_hash"}


# ---------------------------------------------------------------------------
# 1. Google OAuth Credentials Discovery
# ---------------------------------------------------------------------------
def load_google_client_config() -> Dict[str, Any]:
    """Find and parse client_secret*.json in workspace or environment variables."""
    # 1. Try finding client_secret_*.json
    for fname in os.listdir(PROJECT_ROOT):
        if fname.startswith("client_secret") and fname.endswith(".json"):
            fpath = os.path.join(PROJECT_ROOT, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    cfg = data.get("web") or data.get("installed")
                    if cfg:
                        logger.info(f"Loaded Google OAuth credentials from {fname}")
                        redirect_uris = cfg.get("redirect_uris", [])
                        redirect_uri = redirect_uris[0] if redirect_uris else "http://localhost:8000"
                        return {
                            "client_id": cfg.get("client_id"),
                            "client_secret": cfg.get("client_secret"),
                            "auth_uri": cfg.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
                            "token_uri": cfg.get("token_uri", "https://oauth2.googleapis.com/token"),
                            "redirect_uri": redirect_uri,
                            "source_file": fname,
                        }
            except Exception as e:
                logger.warning(f"Error reading {fname}: {e}")

    # 2. Fallback to .env
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000")
    if client_id and client_secret:
        return {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uri": redirect_uri,
            "source_file": ".env",
        }

    return {}


GOOGLE_CONFIG = load_google_client_config()


# ---------------------------------------------------------------------------
# 2. Database Initialization for Authentication
# ---------------------------------------------------------------------------
def init_auth_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            password_hash TEXT,
            picture TEXT,
            bg_color TEXT,
            auth_provider TEXT DEFAULT 'local',
            is_verified INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    try:
        cursor.execute("ALTER TABLE users ADD COLUMN bg_color TEXT")
    except Exception:
        pass

    try:
        cursor.execute("ALTER TABLE users ADD COLUMN google_picture TEXT")
    except Exception:
        pass

    try:
        cursor.execute("UPDATE users SET google_picture = picture WHERE (google_picture IS NULL OR google_picture = '') AND picture LIKE 'http%'")
    except Exception:
        pass

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS otp_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            otp_code TEXT NOT NULL,
            expires_at REAL NOT NULL,
            is_used INTEGER DEFAULT 0,
            attempts INTEGER DEFAULT 0,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for ddl in (
        "ALTER TABLE otp_verifications ADD COLUMN attempts INTEGER DEFAULT 0",
        "ALTER TABLE otp_verifications ADD COLUMN ip_address TEXT",
    ):
        try:
            cursor.execute(ddl)
        except Exception:
            pass

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            expires_at REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oauth_tokens (
            key TEXT PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            expires_at REAL,
            email TEXT,
            scopes TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oauth_states (
            state TEXT PRIMARY KEY,
            code_verifier TEXT NOT NULL,
            expires_at REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


init_auth_db()


# ---------------------------------------------------------------------------
# 3. Password Hashing & Security Helpers
# ---------------------------------------------------------------------------
def hash_password(password: str) -> str:
    """Cryptographically secure PBKDF2-HMAC-SHA256 password hash with unique salt."""
    salt = secrets.token_hex(16)
    key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000)
    return f"{salt}:{key.hex()}"


def verify_password(stored_hash: str, provided_password: str) -> bool:
    """Verify password against stored salt:hash."""
    try:
        salt, key_hex = stored_hash.split(":")
        new_key = hashlib.pbkdf2_hmac("sha256", provided_password.encode("utf-8"), salt.encode("utf-8"), 100000)
        return secrets.compare_digest(key_hex, new_key.hex())
    except Exception:
        return False


def is_valid_email(email: str) -> bool:
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return bool(re.match(pattern, email.strip()))


# ---------------------------------------------------------------------------
# 4. User and Session Database Accessors
# ---------------------------------------------------------------------------
def db_get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, email, name, password_hash, picture, auth_provider, is_verified, created_at, bg_color, google_picture FROM users WHERE lower(email) = lower(?)",
        (email.strip(),),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "email": row[1],
        "name": row[2],
        "password_hash": row[3],
        "picture": row[4],
        "auth_provider": row[5],
        "is_verified": bool(row[6]),
        "created_at": row[7],
        "bg_color": row[8] if len(row) > 8 else None,
        "google_picture": row[9] if len(row) > 9 else None,
    }


def db_get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, email, name, picture, auth_provider, is_verified, created_at, bg_color, google_picture FROM users WHERE id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "email": row[1],
        "name": row[2],
        "picture": row[3],
        "auth_provider": row[4],
        "is_verified": bool(row[5]),
        "created_at": row[6],
        "bg_color": row[7] if len(row) > 7 else None,
        "google_picture": row[8] if len(row) > 8 else None,
    }


def db_update_user_profile(
    user_id: str,
    name: str,
    picture: Optional[str] = None,
    bg_color: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    clean_name = name.strip()

    if picture in ("none", "google", "__google__", "default"):
        cursor.execute(
            """
            UPDATE users
            SET name = ?,
                picture = COALESCE(google_picture, picture),
                bg_color = CASE WHEN ? IS NOT NULL THEN ? ELSE bg_color END
            WHERE id = ?
            """,
            (clean_name, bg_color, bg_color, user_id),
        )
    else:
        cursor.execute(
            """
            UPDATE users
            SET name = ?,
                picture = CASE WHEN ? IS NOT NULL THEN ? ELSE picture END,
                bg_color = CASE WHEN ? IS NOT NULL THEN ? ELSE bg_color END
            WHERE id = ?
            """,
            (clean_name, picture, picture, bg_color, bg_color, user_id),
        )
    conn.commit()
    conn.close()
    return db_get_user_by_id(user_id)


def generate_avatar_image(
    name: str = "User",
    bg_hex: str = "#06b6d4",
    seed_val: int = 0,
    style: str = "cartoon",
    size: int = 256,
) -> str:
    """Generate a crisp, high-resolution supersampled cartoon character avatar using Pillow and return a PNG data URL."""
    scale = 2
    render_size = size * scale

    seed_text = f"{(name or 'User').strip()}_{seed_val}_{bg_hex}"
    h = hashlib.sha256(seed_text.encode("utf-8")).hexdigest()
    nums = [int(h[i : i + 4], 16) for i in range(0, len(h), 4)]

    def rnd(idx: int, count: int) -> int:
        return nums[idx % len(nums)] % count

    clean_hex = (bg_hex or "#06b6d4").lstrip("#")
    if len(clean_hex) == 3:
        clean_hex = "".join([c * 2 for c in clean_hex])
    try:
        bg_r = int(clean_hex[0:2], 16)
        bg_g = int(clean_hex[2:4], 16)
        bg_b = int(clean_hex[4:6], 16)
    except Exception:
        bg_r, bg_g, bg_b = (6, 182, 212)

    try:
        from PIL import Image, ImageDraw

        img = Image.new("RGBA", (render_size, render_size), (bg_r, bg_g, bg_b, 255))
        draw = ImageDraw.Draw(img)

        # Soft glowing background spotlight circle
        glow_r = max(0, min(255, int(bg_r * 0.85 + 20)))
        glow_g = max(0, min(255, int(bg_g * 0.85 + 20)))
        glow_b = max(0, min(255, int(bg_b * 0.85 + 20)))
        draw.ellipse(
            [render_size * 0.1, render_size * 0.32, render_size * 0.9, render_size * 1.12],
            fill=(glow_r, glow_g, glow_b, 150),
        )

        SKIN_TONES = [
            ("#fde0c5", "#f0b68e"),
            ("#fcd5b5", "#e8aa82"),
            ("#f5c49f", "#db986f"),
            ("#e0a37c", "#be7c54"),
            ("#b97a56", "#965b38"),
            ("#8c5438", "#6a3922"),
        ]
        skin_fill, skin_shadow = SKIN_TONES[rnd(0, len(SKIN_TONES))]

        HAIR_COLORS = [
            "#1e1b18", "#3b2314", "#5c3116", "#854d0e",
            "#d97706", "#f59e0b", "#0284c7", "#7c3aed",
            "#db2777", "#e11d48", "#10b981", "#475569"
        ]
        hair_color = HAIR_COLORS[rnd(1, len(HAIR_COLORS))]

        CLOTHES_COLORS = [
            "#1e293b", "#0f172a", "#3b82f6", "#6366f1", "#8b5cf6",
            "#ec4899", "#10b981", "#14b8a6", "#f97316", "#ef4444", "#334155"
        ]
        clothes_color = CLOTHES_COLORS[rnd(2, len(CLOTHES_COLORS))]

        cx = render_size // 2
        cy = render_size // 2

        # 1. Shoulders / Torso
        draw.chord(
            [render_size * 0.16, render_size * 0.67, render_size * 0.84, render_size * 1.35],
            start=180,
            end=360,
            fill=clothes_color,
        )
        # Hoodie inner collar / V-neck
        draw.polygon(
            [(cx - render_size * 0.08, render_size * 0.72), (cx + render_size * 0.08, render_size * 0.72), (cx, render_size * 0.83)],
            fill=(255, 255, 255, 210),
        )

        # 2. Neck
        draw.rectangle(
            [cx - render_size * 0.065, render_size * 0.60, cx + render_size * 0.065, render_size * 0.75],
            fill=skin_shadow,
        )

        # 3. Ears
        ear_y = int(render_size * 0.49)
        ear_rad = int(render_size * 0.068)
        draw.ellipse([cx - render_size * 0.27, ear_y - ear_rad, cx - render_size * 0.15, ear_y + ear_rad], fill=skin_fill)
        draw.ellipse([cx - render_size * 0.24, ear_y - ear_rad * 0.5, cx - render_size * 0.17, ear_y + ear_rad * 0.5], fill=skin_shadow)
        draw.ellipse([cx + render_size * 0.15, ear_y - ear_rad, cx + render_size * 0.27, ear_y + ear_rad], fill=skin_fill)
        draw.ellipse([cx + render_size * 0.17, ear_y - ear_rad * 0.5, cx + render_size * 0.24, ear_y + ear_rad * 0.5], fill=skin_shadow)

        # 4. Head / Face (Cute rounded chibi head)
        face_box = [cx - render_size * 0.22, render_size * 0.27, cx + render_size * 0.22, render_size * 0.68]
        draw.rounded_rectangle(face_box, radius=int(render_size * 0.16), fill=skin_fill)

        # 5. Cheeks (Cute soft blush)
        blush_color = (244, 114, 182, 150)
        draw.ellipse([cx - render_size * 0.18, render_size * 0.51, cx - render_size * 0.10, render_size * 0.57], fill=blush_color)
        draw.ellipse([cx + render_size * 0.10, render_size * 0.51, cx + render_size * 0.18, render_size * 0.57], fill=blush_color)

        # 6. Eyes (Big cute cartoon eyes, wink, shades, or glasses)
        eye_style = rnd(3, 5)
        eye_y = int(render_size * 0.47)
        eye_dx = int(render_size * 0.095)
        eye_w = int(render_size * 0.046)
        eye_h = int(render_size * 0.056)

        if eye_style == 0:
            # Sparkle anime/chibi eyes
            for side in (-1, 1):
                ex = cx + side * eye_dx
                draw.ellipse([ex - eye_w, eye_y - eye_h, ex + eye_w, eye_y + eye_h], fill=(24, 24, 27, 255))
                draw.ellipse([ex - eye_w * 0.55, eye_y - eye_h * 0.65, ex + eye_w * 0.15, eye_y - eye_h * 0.1], fill=(255, 255, 255, 255))
                draw.ellipse([ex + eye_w * 0.15, eye_y + eye_h * 0.15, ex + eye_w * 0.5, eye_y + eye_h * 0.5], fill=(255, 255, 255, 220))
        elif eye_style == 1:
            # Happy squint curves `^ ^`
            for side in (-1, 1):
                ex = cx + side * eye_dx
                draw.arc([ex - eye_w, eye_y - eye_h * 0.6, ex + eye_w, eye_y + eye_h * 0.4], start=190, end=350, fill=(24, 24, 27, 255), width=int(render_size * 0.016))
        elif eye_style == 2:
            # Playful wink
            ex = cx - eye_dx
            draw.ellipse([ex - eye_w, eye_y - eye_h, ex + eye_w, eye_y + eye_h], fill=(24, 24, 27, 255))
            draw.ellipse([ex - eye_w * 0.55, eye_y - eye_h * 0.65, ex + eye_w * 0.15, eye_y - eye_h * 0.1], fill=(255, 255, 255, 255))
            ex_r = cx + eye_dx
            draw.arc([ex_r - eye_w, eye_y - eye_h * 0.3, ex_r + eye_w, eye_y + eye_h * 0.5], start=190, end=350, fill=(24, 24, 27, 255), width=int(render_size * 0.016))
        elif eye_style == 3:
            # Cool Sunglasses
            draw.rounded_rectangle([cx - render_size * 0.19, eye_y - eye_h, cx + render_size * 0.19, eye_y + eye_h * 0.8], radius=int(render_size * 0.025), fill=(15, 23, 42, 255))
            draw.line([cx - render_size * 0.05, eye_y, cx + render_size * 0.05, eye_y], fill=(15, 23, 42, 255), width=int(render_size * 0.015))
            draw.line([cx - render_size * 0.15, eye_y - eye_h * 0.5, cx - render_size * 0.08, eye_y + eye_h * 0.3], fill=(255, 255, 255, 120), width=int(render_size * 0.012))
            draw.line([cx + render_size * 0.04, eye_y - eye_h * 0.5, cx + render_size * 0.11, eye_y + eye_h * 0.3], fill=(255, 255, 255, 120), width=int(render_size * 0.012))
        else:
            # Cute round glasses
            for side in (-1, 1):
                ex = cx + side * eye_dx
                draw.ellipse([ex - eye_w * 0.85, eye_y - eye_h * 0.85, ex + eye_w * 0.85, eye_y + eye_h * 0.85], fill=(24, 24, 27, 255))
                draw.ellipse([ex - eye_w * 0.4, eye_y - eye_h * 0.5, ex + eye_w * 0.1, eye_y - eye_h * 0.1], fill=(255, 255, 255, 255))
                draw.ellipse([ex - eye_w * 1.35, eye_y - eye_h * 1.2, ex + eye_w * 1.35, eye_y + eye_h * 1.2], outline=(30, 41, 59, 255), width=int(render_size * 0.016))
            draw.line([cx - eye_dx + eye_w * 1.35, eye_y, cx + eye_dx - eye_w * 1.35, eye_y], fill=(30, 41, 59, 255), width=int(render_size * 0.016))

        # 7. Eyebrows
        brow_y = eye_y - int(render_size * 0.085)
        for side in (-1, 1):
            bx = cx + side * eye_dx
            draw.arc([bx - eye_w, brow_y - eye_h * 0.3, bx + eye_w, brow_y + eye_h * 0.4], start=200, end=340, fill=(30, 41, 59, 255), width=int(render_size * 0.015))

        # 8. Nose
        draw.ellipse([cx - render_size * 0.012, render_size * 0.53, cx + render_size * 0.012, render_size * 0.548], fill=skin_shadow)

        # 9. Mouth
        mouth_style = rnd(4, 4)
        mouth_y = int(render_size * 0.58)
        if mouth_style == 0:
            draw.chord([cx - render_size * 0.06, mouth_y - render_size * 0.01, cx + render_size * 0.06, mouth_y + render_size * 0.05], start=0, end=180, fill=(159, 18, 57, 255))
            draw.chord([cx - render_size * 0.035, mouth_y + render_size * 0.02, cx + render_size * 0.035, mouth_y + render_size * 0.05], start=0, end=180, fill=(244, 114, 182, 255))
        elif mouth_style == 1:
            draw.arc([cx - render_size * 0.05, mouth_y - render_size * 0.02, cx, mouth_y + render_size * 0.02], start=10, end=170, fill=(30, 41, 59, 255), width=int(render_size * 0.015))
            draw.arc([cx, mouth_y - render_size * 0.02, cx + render_size * 0.05, mouth_y + render_size * 0.02], start=10, end=170, fill=(30, 41, 59, 255), width=int(render_size * 0.015))
        elif mouth_style == 2:
            draw.chord([cx - render_size * 0.06, mouth_y - render_size * 0.01, cx + render_size * 0.06, mouth_y + render_size * 0.045], start=0, end=180, fill=(30, 41, 59, 255))
            draw.rectangle([cx - render_size * 0.035, mouth_y, cx + render_size * 0.035, mouth_y + render_size * 0.015], fill=(255, 255, 255, 255))
        else:
            draw.arc([cx - render_size * 0.05, mouth_y - render_size * 0.02, cx + render_size * 0.05, mouth_y + render_size * 0.03], start=20, end=160, fill=(30, 41, 59, 255), width=int(render_size * 0.016))

        # 10. Hair / Headwear
        hair_style = rnd(5, 6)
        if hair_style == 0:
            # Spiky anime hair
            points = [
                (cx - render_size * 0.25, render_size * 0.38),
                (cx - render_size * 0.28, render_size * 0.24),
                (cx - render_size * 0.18, render_size * 0.26),
                (cx - render_size * 0.14, render_size * 0.14),
                (cx - render_size * 0.04, render_size * 0.20),
                (cx + render_size * 0.05, render_size * 0.12),
                (cx + render_size * 0.15, render_size * 0.22),
                (cx + render_size * 0.26, render_size * 0.16),
                (cx + render_size * 0.24, render_size * 0.36),
                (cx + render_size * 0.22, render_size * 0.42),
                (cx + render_size * 0.12, render_size * 0.33),
                (cx, render_size * 0.35),
                (cx - render_size * 0.12, render_size * 0.32),
                (cx - render_size * 0.22, render_size * 0.40),
            ]
            draw.polygon(points, fill=hair_color)
        elif hair_style == 1:
            # Fluffy cloud afro
            for (dx, dy, r) in [
                (0, -0.22, 0.16),
                (-0.16, -0.16, 0.14),
                (0.16, -0.16, 0.14),
                (-0.23, -0.05, 0.12),
                (0.23, -0.05, 0.12),
                (-0.22, 0.08, 0.10),
                (0.22, 0.08, 0.10),
            ]:
                draw.ellipse([cx + render_size * dx - render_size * r, cy + render_size * dy - render_size * r, cx + render_size * dx + render_size * r, cy + render_size * dy + render_size * r], fill=hair_color)
        elif hair_style == 2:
            # Bob with bangs
            draw.chord([cx - render_size * 0.26, render_size * 0.16, cx + render_size * 0.26, render_size * 0.56], start=160, end=380, fill=hair_color)
            draw.rectangle([cx - render_size * 0.26, render_size * 0.36, cx - render_size * 0.18, render_size * 0.56], fill=hair_color)
            draw.rectangle([cx + render_size * 0.18, render_size * 0.36, cx + render_size * 0.26, render_size * 0.56], fill=hair_color)
            draw.chord([cx - render_size * 0.20, render_size * 0.25, cx + render_size * 0.20, render_size * 0.40], start=0, end=180, fill=hair_color)
        elif hair_style == 3:
            # Beanie
            beanie_color = clothes_color
            draw.chord([cx - render_size * 0.25, render_size * 0.15, cx + render_size * 0.25, render_size * 0.48], start=170, end=370, fill=beanie_color)
            draw.rounded_rectangle([cx - render_size * 0.24, render_size * 0.29, cx + render_size * 0.24, render_size * 0.37], radius=int(render_size * 0.03), fill=hair_color)
            draw.ellipse([cx - render_size * 0.05, render_size * 0.11, cx + render_size * 0.05, render_size * 0.21], fill=(255, 255, 255, 230))
        elif hair_style == 4:
            # Floppy side-swept
            draw.chord([cx - render_size * 0.25, render_size * 0.18, cx + render_size * 0.25, render_size * 0.50], start=170, end=370, fill=hair_color)
            points = [
                (cx - render_size * 0.25, render_size * 0.34),
                (cx - render_size * 0.10, render_size * 0.18),
                (cx + render_size * 0.22, render_size * 0.22),
                (cx + render_size * 0.26, render_size * 0.38),
                (cx + render_size * 0.05, render_size * 0.36),
                (cx - render_size * 0.15, render_size * 0.42),
            ]
            draw.polygon(points, fill=hair_color)
        else:
            # Cat ears headband
            draw.chord([cx - render_size * 0.23, render_size * 0.20, cx + render_size * 0.23, render_size * 0.48], start=170, end=370, fill=hair_color)
            draw.arc([cx - render_size * 0.24, render_size * 0.20, cx + render_size * 0.24, render_size * 0.44], start=190, end=350, fill=(30, 41, 59, 255), width=int(render_size * 0.02))
            draw.polygon([(cx - render_size * 0.22, render_size * 0.28), (cx - render_size * 0.14, render_size * 0.12), (cx - render_size * 0.06, render_size * 0.25)], fill=hair_color)
            draw.polygon([(cx - render_size * 0.19, render_size * 0.26), (cx - render_size * 0.14, render_size * 0.16), (cx - render_size * 0.09, render_size * 0.24)], fill=(244, 114, 182, 240))
            draw.polygon([(cx + render_size * 0.06, render_size * 0.25), (cx + render_size * 0.14, render_size * 0.12), (cx + render_size * 0.22, render_size * 0.28)], fill=hair_color)
            draw.polygon([(cx + render_size * 0.09, render_size * 0.24), (cx + render_size * 0.14, render_size * 0.16), (cx + render_size * 0.19, render_size * 0.26)], fill=(244, 114, 182, 240))

        # Downsample for crisp anti-aliased finish
        try:
            resample_filter = Image.Resampling.LANCZOS
        except Exception:
            resample_filter = Image.ANTIALIAS  # type: ignore
        final_img = img.resize((size, size), resample_filter)

        buf = io.BytesIO()
        final_img.save(buf, format="PNG")
        return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()

    except Exception as err:
        logger.error(f"Cartoon avatar generator error: {err}")
        import urllib.parse
        svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}"><rect width="100%" height="100%" fill="#{clean_hex}"/><circle cx="{size//2}" cy="{int(size*0.45)}" r="{int(size*0.25)}" fill="#fcd5b5"/><circle cx="{int(size*0.42)}" cy="{int(size*0.43)}" r="{int(size*0.04)}" fill="#1e293b"/><circle cx="{int(size*0.58)}" cy="{int(size*0.43)}" r="{int(size*0.04)}" fill="#1e293b"/><path d="M {int(size*0.44)} {int(size*0.53)} Q {size//2} {int(size*0.6)} {int(size*0.56)} {int(size*0.53)}" stroke="#1e293b" stroke-width="3" fill="none"/></svg>'
        return f"data:image/svg+xml;utf8,{urllib.parse.quote(svg)}"


def db_create_user(
    name: str,
    email: str,
    password: Optional[str] = None,
    picture: Optional[str] = None,
    auth_provider: str = "local",
    is_verified: bool = False,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    uid = user_id or str(uuid.uuid4())
    pw_hash = hash_password(password) if password else None
    clean_email = email.strip().lower()

    google_pic = picture if (auth_provider == "google" and picture) else None

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO users (id, email, name, password_hash, picture, google_picture, auth_provider, is_verified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            name = excluded.name,
            google_picture = COALESCE(excluded.google_picture, users.google_picture),
            picture = COALESCE(users.picture, excluded.picture),
            password_hash = COALESCE(excluded.password_hash, users.password_hash),
            is_verified = MAX(users.is_verified, excluded.is_verified)
        """,
        (uid, clean_email, name.strip(), pw_hash, picture, google_pic, auth_provider, 1 if is_verified else 0),
    )
    conn.commit()
    conn.close()
    return db_get_user_by_email(clean_email)  # type: ignore


def db_delete_unverified_user(email: str) -> bool:
    """
    Rollback helper for failed signup dispatch. Removes an unverified account
    and any OTP rows issued for it, so a 503 never leaves a half-created user
    behind. Verified accounts are never touched.
    """
    clean_email = email.strip().lower()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM otp_verifications WHERE lower(email) = lower(?)",
        (clean_email,),
    )
    cursor.execute(
        "DELETE FROM users WHERE lower(email) = lower(?) AND is_verified = 0",
        (clean_email,),
    )
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def db_mark_user_verified(email: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_verified = 1 WHERE lower(email) = lower(?)", (email.strip(),))
    conn.commit()
    conn.close()


def db_create_session(user_id: str, duration_days: int = 7) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = time.time() + (duration_days * 86400)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO user_sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
        (token, user_id, expires_at),
    )
    conn.commit()
    conn.close()
    return token


def db_validate_session(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_id, expires_at FROM user_sessions WHERE token = ?",
        (token,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    user_id, expires_at = row
    if time.time() > expires_at:
        db_delete_session(token)
        return None
    return db_get_user_by_id(user_id)


def db_delete_session(token: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM user_sessions WHERE token = ?", (token,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# 5. OTP Storage & Verification
# ---------------------------------------------------------------------------
def check_otp_rate_limit(email: str, ip_address: Optional[str]) -> Tuple[bool, str]:
    """Caps OTP creation per email and per source IP inside a rolling window."""
    clean_email = email.strip().lower()
    window = f"-{OTP_REQUEST_WINDOW_MINUTES} minutes"

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM otp_verifications WHERE lower(email) = lower(?) AND created_at >= datetime('now', ?)",
        (clean_email, window),
    )
    email_count = cursor.fetchone()[0] or 0

    ip_count = 0
    if ip_address:
        cursor.execute(
            "SELECT COUNT(*) FROM otp_verifications WHERE ip_address = ? AND created_at >= datetime('now', ?)",
            (ip_address, window),
        )
        ip_count = cursor.fetchone()[0] or 0
    conn.close()

    if email_count >= OTP_MAX_REQUESTS_PER_EMAIL:
        return False, f"Too many verification codes requested. Try again in {OTP_REQUEST_WINDOW_MINUTES} minutes."
    if ip_address and ip_count >= OTP_MAX_REQUESTS_PER_IP:
        return False, f"Too many verification codes requested from this address. Try again in {OTP_REQUEST_WINDOW_MINUTES} minutes."
    return True, ""


def db_create_otp(email: str, expiry_minutes: int = 10, ip_address: Optional[str] = None) -> str:
    """Generate and store a 6-digit numeric OTP."""
    otp_code = f"{secrets.randbelow(900000) + 100000}"
    expires_at = time.time() + (expiry_minutes * 60)
    clean_email = email.strip().lower()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Invalidate previous unexpired OTPs for this email
    cursor.execute("UPDATE otp_verifications SET is_used = 1 WHERE lower(email) = lower(?)", (clean_email,))
    cursor.execute(
        "INSERT INTO otp_verifications (email, otp_code, expires_at, is_used, attempts, ip_address) VALUES (?, ?, ?, 0, 0, ?)",
        (clean_email, otp_code, expires_at, ip_address),
    )
    conn.commit()
    conn.close()
    return otp_code


def db_verify_otp(email: str, entered_code: str) -> Tuple[bool, str]:
    clean_email = email.strip().lower()
    clean_code = entered_code.strip()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, otp_code, expires_at, is_used, attempts
        FROM otp_verifications
        WHERE lower(email) = lower(?) AND is_used = 0
        ORDER BY id DESC LIMIT 1
        """,
        (clean_email,),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False, "No active OTP found. Please request a new verification code."

    otp_id, actual_code, expires_at, is_used, attempts = row
    if time.time() > expires_at:
        cursor.execute("UPDATE otp_verifications SET is_used = 1 WHERE id = ?", (otp_id,))
        conn.commit()
        conn.close()
        return False, "Verification code has expired. Please request a new one."

    if attempts >= OTP_MAX_VERIFY_ATTEMPTS:
        cursor.execute("UPDATE otp_verifications SET is_used = 1 WHERE id = ?", (otp_id,))
        conn.commit()
        conn.close()
        return False, "Too many incorrect attempts. Please request a new verification code."

    if not secrets.compare_digest(actual_code, clean_code):
        cursor.execute("UPDATE otp_verifications SET attempts = attempts + 1 WHERE id = ?", (otp_id,))
        conn.commit()
        conn.close()
        return False, "Incorrect verification code. Please check your email and try again."

    # Mark as used
    cursor.execute("UPDATE otp_verifications SET is_used = 1 WHERE id = ?", (otp_id,))
    conn.commit()
    conn.close()

    # Mark user as verified
    db_mark_user_verified(clean_email)
    return True, "Email successfully verified!"


# ---------------------------------------------------------------------------
# 6. OAuth Token Storage for Mail Sending
# ---------------------------------------------------------------------------
def db_save_oauth_tokens(
    key: str,
    access_token: str,
    refresh_token: Optional[str] = None,
    expires_in: int = 3600,
    email: Optional[str] = None,
    scopes: Optional[str] = None,
):
    expires_at = time.time() + expires_in
    access_token = encrypt_oauth_secret(access_token)
    refresh_token = encrypt_oauth_secret(refresh_token)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if refresh_token:
        cursor.execute(
            """
            INSERT INTO oauth_tokens (key, access_token, refresh_token, expires_at, email, scopes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token,
                expires_at = excluded.expires_at,
                email = COALESCE(excluded.email, oauth_tokens.email),
                scopes = COALESCE(excluded.scopes, oauth_tokens.scopes),
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, access_token, refresh_token, expires_at, email, scopes),
        )
    else:
        cursor.execute(
            """
            INSERT INTO oauth_tokens (key, access_token, expires_at, email, scopes, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                access_token = excluded.access_token,
                expires_at = excluded.expires_at,
                email = COALESCE(excluded.email, oauth_tokens.email),
                scopes = COALESCE(excluded.scopes, oauth_tokens.scopes),
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, access_token, expires_at, email, scopes),
        )
    conn.commit()
    conn.close()


def db_get_oauth_tokens(key: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT key, access_token, refresh_token, expires_at, email, scopes FROM oauth_tokens WHERE key = ?",
        (key,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "key": row[0],
        "access_token": decrypt_oauth_secret(row[1]),
        "refresh_token": decrypt_oauth_secret(row[2]),
        "expires_at": row[3],
        "email": row[4],
        "scopes": row[5],
    }


def db_create_oauth_state(state: str, code_verifier: str) -> None:
    """Persist a single-use OAuth state bound to its PKCE verifier."""
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM oauth_states WHERE expires_at < ?", (time.time(),))
    cursor.execute(
        "INSERT INTO oauth_states (state, code_verifier, expires_at) VALUES (?, ?, ?)",
        (state, code_verifier, time.time() + OAUTH_STATE_TTL_SECONDS),
    )
    conn.commit()
    conn.close()


def db_consume_oauth_state(state: str) -> Optional[str]:
    """Atomically consume a state row and return its PKCE verifier.

    Returns None for unknown, expired, or already-used states so a replayed
    callback cannot exchange the authorization code again.
    """
    if not state:
        return None
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    cursor.execute(
        "SELECT code_verifier, expires_at FROM oauth_states WHERE state = ?",
        (state,),
    )
    row = cursor.fetchone()
    cursor.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
    conn.commit()
    conn.close()
    if not row or row[1] < time.time():
        return None
    return row[0]


# ---------------------------------------------------------------------------
# 7. Email Dispatcher (Gmail API with OAuth2 + SMTP + Dev Fallback)
# ---------------------------------------------------------------------------
async def refresh_google_token_if_needed(key: str = "system_mailer") -> Optional[str]:
    """Retrieve or refresh valid Google Access Token with send scope."""
    tokens = db_get_oauth_tokens(key)
    if not tokens:
        return None

    # If access token has at least 5 minutes left
    if tokens["access_token"] and tokens["expires_at"] > time.time() + 300:
        return tokens["access_token"]

    refresh_token = tokens["refresh_token"]
    if not refresh_token:
        logger.warning("No refresh_token found for Google OAuth token refresh.")
        return tokens["access_token"]

    client_id = GOOGLE_CONFIG.get("client_id")
    client_secret = GOOGLE_CONFIG.get("client_secret")
    token_uri = GOOGLE_CONFIG.get("token_uri", "https://oauth2.googleapis.com/token")

    if not client_id or not client_secret:
        return None

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(
                token_uri,
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )
            if res.status_code == 200:
                data = res.json()
                new_access_token = data["access_token"]
                expires_in = data.get("expires_in", 3600)
                db_save_oauth_tokens(
                    key=key,
                    access_token=new_access_token,
                    refresh_token=refresh_token,
                    expires_in=expires_in,
                    email=tokens["email"],
                    scopes=tokens["scopes"],
                )
                logger.info(f"Successfully refreshed Google Access Token for {tokens.get('email')}")
                return new_access_token
            else:
                logger.error(f"Failed to refresh Google token: {res.status_code} {res.text}")
    except Exception as e:
        logger.error(f"Exception refreshing Google token: {e}")

    return tokens.get("access_token")


async def send_email_via_gmail_api(to_email: str, subject: str, html_body: str) -> bool:
    """Send real email using Google Gmail API with authorized OAuth tokens."""
    access_token = await refresh_google_token_if_needed("system_mailer")
    if not access_token:
        # Check any available token
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT key FROM oauth_tokens LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            access_token = await refresh_google_token_if_needed(row[0])

    if not access_token:
        return False

    message = MIMEMultipart("alternative")
    message["to"] = to_email
    message["subject"] = subject
    message["from"] = "LLM Gateway <me>"
    message.attach(MIMEText(html_body, "html"))

    raw_bytes = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={"raw": raw_bytes},
            )
            if res.status_code in (200, 202):
                logger.info(f"REAL GMAIL SENT to {to_email} via Gmail API! Message ID: {res.json().get('id')}")
                return True
            else:
                logger.error(f"Gmail API send error: {res.status_code} {res.text}")
                return False
    except Exception as e:
        logger.error(f"Exception sending email via Gmail API: {e}")
        return False


def build_otp_html_email(otp_code: str, recipient_email: str) -> str:
    """Generate high-tech dark theme HTML email for verification code."""
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LLM Gateway Verification Code</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      background-color: #070a12;
      color: #e2e8f0;
      margin: 0;
      padding: 32px 16px;
    }}
    .wrapper {{
      max-width: 480px;
      margin: 0 auto;
      background: #0f172a;
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 20px;
      padding: 36px 28px;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.7);
    }}
    .brand {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 15px;
      font-weight: 700;
      color: #38bdf8;
      letter-spacing: -0.3px;
      margin-bottom: 24px;
    }}
    h1 {{
      font-size: 24px;
      font-weight: 700;
      color: #ffffff;
      margin: 0 0 8px 0;
      letter-spacing: -0.5px;
    }}
    p {{
      font-size: 14px;
      line-height: 1.6;
      color: #94a3b8;
      margin: 0 0 20px 0;
    }}
    .otp-container {{
      background: rgba(14, 165, 233, 0.08);
      border: 1px solid rgba(14, 165, 233, 0.35);
      border-radius: 14px;
      padding: 22px;
      text-align: center;
      margin: 28px 0;
    }}
    .otp-digits {{
      font-family: 'SF Mono', Monaco, Consolas, monospace;
      font-size: 36px;
      font-weight: 800;
      letter-spacing: 8px;
      color: #38bdf8;
      text-shadow: 0 0 20px rgba(56, 189, 248, 0.4);
    }}
    .otp-label {{
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      color: #64748b;
      margin-top: 8px;
      font-weight: 600;
    }}
    .note {{
      font-size: 12px;
      color: #64748b;
      line-height: 1.5;
    }}
    .footer {{
      margin-top: 32px;
      padding-top: 20px;
      border-top: 1px solid rgba(255, 255, 255, 0.08);
      text-align: center;
      font-size: 11px;
      color: #475569;
    }}
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="brand">✦ LLM Gateway</div>
    <h1>Verify Your Email</h1>
    <p>Welcome! Use the one-time verification code below to confirm your email (<strong>{recipient_email}</strong>) and complete your registration:</p>
    
    <div class="otp-container">
      <div class="otp-digits">{otp_code}</div>
      <div class="otp-label">One-Time Password (OTP)</div>
    </div>

    <p class="note">This code will expire in <strong>10 minutes</strong>. If you did not sign up for LLM Gateway, please ignore this email.</p>
    
    <div class="footer">
      LLM Gateway System • High-Reliability Multi-Model Gateway
    </div>
  </div>
</body>
</html>
"""


async def dispatch_otp_email(to_email: str, otp_code: str) -> Dict[str, Any]:
    """
    Deliver OTP to user:
    1. Try Gmail API with OAuth scope https://www.googleapis.com/auth/gmail.send
    2. Try SMTP if configured in .env
    3. In development mode only, log the code to the server console.
       In production, refuse rather than silently degrade to an insecure path.
    """
    clean_email = to_email.strip().lower()
    subject = f"✦ {otp_code} is your LLM Gateway verification code"
    html_content = build_otp_html_email(otp_code, clean_email)

    # 1. Try Gmail API
    gmail_sent = await send_email_via_gmail_api(clean_email, subject, html_content)
    if gmail_sent:
        logger.info(f"[OTP DISPATCH] Real email sent to {clean_email} via Gmail API!")
        return {
            "success": True,
            "method": "gmail_api",
            "message": f"Verification code sent to {clean_email} via Gmail API",
        }

    # 2. Try SMTP fallback if provided
    smtp_host = os.getenv("SMTP_HOST")
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))

    if smtp_host and smtp_user and smtp_pass:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"LLM Gateway <{smtp_user}>"
            msg["To"] = clean_email
            msg.attach(MIMEText(html_content, "html"))

            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
            logger.info(f"[OTP DISPATCH] Real email sent to {clean_email} via SMTP!")
            return {
                "success": True,
                "method": "smtp",
                "message": f"Verification code sent to {clean_email} via SMTP",
            }
        except Exception as e:
            logger.warning(f"SMTP send failed: {e}")

    # 3. Development-only console fallback. Never return the code to the client.
    if IS_DEV_ENV:
        logger.info("============================================================")
        logger.info(f"[DEV / LOCAL OTP] Verification code for {clean_email}: {otp_code}")
        logger.info("============================================================")
        return {
            "success": True,
            "method": "dev_console",
            "message": f"Verification code generated for {clean_email}. Check the server console (development mode).",
        }

    logger.error(
        f"[OTP DISPATCH] No email delivery configured for {clean_email}. "
        "Set SMTP_HOST/SMTP_USER/SMTP_PASS or connect Gmail in production."
    )
    return {
        "success": False,
        "method": "none",
        "message": "Email delivery is not configured. Ask the administrator to configure SMTP or Gmail.",
    }
