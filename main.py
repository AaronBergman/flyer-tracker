import os
import csv
import io
import secrets
import string
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, func
from sqlalchemy.orm import DeclarativeBase, sessionmaker, relationship, Session
import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PRIVATE_URL") or os.environ.get("DATABASE_PUBLIC_URL") or ""

if DATABASE_URL:
    # Railway historically used postgres:// but SQLAlchemy needs postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = ""

# Log what we're working with on startup (mask credentials)
if DATABASE_URL:
    _masked = DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL
    print(f"DATABASE_URL found, connecting to: ...@{_masked}")
else:
    print("WARNING: No DATABASE_URL found in environment!")
    print(f"  Available env vars: {[k for k in os.environ if 'DATABASE' in k.upper() or 'PG' in k.upper() or 'POSTGRES' in k.upper()]}")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None
SessionLocal = sessionmaker(bind=engine) if engine else None


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Link(Base):
    __tablename__ = "links"

    id = Column(Integer, primary_key=True)
    slug = Column(String(100), unique=True, nullable=False, index=True)
    target_url = Column(Text, nullable=False)
    description = Column(Text, default="")
    posted_location = Column(String(500), default="")  # physical location of the flyer
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    scans = relationship("Scan", back_populates="link", order_by="Scan.scanned_at.desc()")


class Scan(Base):
    __tablename__ = "scans"

    id = Column(Integer, primary_key=True)
    link_id = Column(Integer, ForeignKey("links.id"), nullable=False, index=True)
    scanned_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Server-side IP geolocation (automatic, ~city-level)
    ip_address = Column(String(45))
    ip_city = Column(String(255))
    ip_region = Column(String(255))
    ip_country = Column(String(100))
    ip_lat = Column(Float)
    ip_lng = Column(Float)
    ip_isp = Column(String(255))

    # Request metadata
    user_agent = Column(Text)
    referer = Column(Text)

    # Client-side browser geolocation (precise, but requires user permission)
    browser_lat = Column(Float)
    browser_lng = Column(Float)
    browser_accuracy = Column(Float)  # meters

    link = relationship("Link", back_populates="scans")


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

_tables_created = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _tables_created
    if engine:
        try:
            Base.metadata.create_all(bind=engine)
            _tables_created = True
            print("Database tables created successfully.")
        except Exception as e:
            print(f"WARNING: Could not create tables on startup: {e}")
            print("Tables will be created on first request.")
    else:
        print("No database configured. Set DATABASE_URL env var.")
    yield


app = FastAPI(title="Flyer Tracker", lifespan=lifespan)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


def get_db():
    if not SessionLocal:
        raise Exception("No database configured")
    global _tables_created
    if not _tables_created and engine:
        try:
            Base.metadata.create_all(bind=engine)
            _tables_created = True
        except Exception:
            pass
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# IP Geolocation
# ---------------------------------------------------------------------------

async def geolocate_ip(ip: str) -> dict:
    """City-level geolocation via ip-api.com (free, no key, 45 req/min)."""
    if ip in ("127.0.0.1", "::1", "testclient", "unknown"):
        return {}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                f"http://ip-api.com/json/{ip}",
                params={"fields": "status,city,regionName,country,lat,lon,isp"},
            )
            data = resp.json()
            if data.get("status") == "success":
                return {
                    "city": data.get("city"),
                    "region": data.get("regionName"),
                    "country": data.get("country"),
                    "lat": data.get("lat"),
                    "lng": data.get("lon"),
                    "isp": data.get("isp"),
                }
    except Exception:
        pass
    return {}


def get_client_ip(request: Request) -> str:
    """Extract client IP, respecting proxy headers (Railway runs behind a proxy)."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def generate_slug(length: int = 8) -> str:
    """Generate a short random slug for auto-created links."""
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


# ---------------------------------------------------------------------------
# Routes — Diagnostics
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    """Check DB connection and show env var diagnostics."""
    db_vars = {k: "***" for k in os.environ if "DATABASE" in k.upper() or "PG" in k.upper() or "POSTGRES" in k.upper()}
    info = {
        "database_url_set": bool(DATABASE_URL),
        "database_url_host": DATABASE_URL.split("@")[-1].split("/")[0] if DATABASE_URL and "@" in DATABASE_URL else "not set",
        "env_vars_found": db_vars,
        "engine_created": engine is not None,
    }
    if engine:
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            info["db_connected"] = True
        except Exception as e:
            info["db_connected"] = False
            info["db_error"] = str(e)
    else:
        info["db_connected"] = False
        info["db_error"] = "No engine — DATABASE_URL not set"
    return JSONResponse(info)


# ---------------------------------------------------------------------------
# Routes — Tracking
# ---------------------------------------------------------------------------

@app.get("/t/{slug}")
async def track_scan(slug: str, request: Request, db: Session = Depends(get_db)):
    """Main tracking endpoint. QR codes point here."""
    link = db.query(Link).filter(Link.slug == slug).first()
    if not link:
        return HTMLResponse("<h1>Link not found</h1>", status_code=404)

    # IP geolocation (automatic, no user interaction)
    client_ip = get_client_ip(request)
    geo = await geolocate_ip(client_ip)

    # Log the scan
    scan = Scan(
        link_id=link.id,
        ip_address=client_ip,
        ip_city=geo.get("city"),
        ip_region=geo.get("region"),
        ip_country=geo.get("country"),
        ip_lat=geo.get("lat"),
        ip_lng=geo.get("lng"),
        ip_isp=geo.get("isp"),
        user_agent=request.headers.get("user-agent"),
        referer=request.headers.get("referer"),
    )
    db.add(scan)
    db.commit()
    db.refresh(scan)

    # Return a brief landing page that tries browser geo then redirects
    return templates.TemplateResponse("landing.html", {
        "request": request,
        "link": link,
        "scan_id": scan.id,
    })


@app.post("/t/{slug}/geo")
async def receive_browser_geo(slug: str, request: Request, db: Session = Depends(get_db)):
    """Callback for browser geolocation (sent via sendBeacon before redirect)."""
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False}, status_code=400)

    scan_id = data.get("scan_id")
    if scan_id:
        scan = db.query(Scan).filter(Scan.id == scan_id).first()
        if scan:
            scan.browser_lat = data.get("lat")
            scan.browser_lng = data.get("lng")
            scan.browser_accuracy = data.get("accuracy")
            db.commit()
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Routes — Dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not SessionLocal:
        db_vars = [k for k in os.environ if "DATABASE" in k.upper() or "PG" in k.upper()]
        return HTMLResponse(
            f"<h1>Database not configured</h1>"
            f"<p>Set <code>DATABASE_URL</code> environment variable.</p>"
            f"<p>DB-related env vars found: <code>{db_vars}</code></p>"
            f"<p>Check <a href='/health'>/health</a> for details.</p>",
            status_code=503,
        )
    db = SessionLocal()
    try:
        return await _dashboard_inner(request, db)
    except Exception as e:
        return HTMLResponse(
            f"<h1>Database connection error</h1>"
            f"<p>{type(e).__name__}: check <a href='/health'>/health</a> for details.</p>",
            status_code=503,
        )
    finally:
        db.close()


async def _dashboard_inner(request: Request, db: Session):
    links = db.query(Link).order_by(Link.created_at.desc()).all()

    scan_counts = {}
    for link in links:
        scan_counts[link.id] = (
            db.query(func.count(Scan.id)).filter(Scan.link_id == link.id).scalar()
        )

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "links": links,
        "scan_counts": scan_counts,
    })


@app.get("/dashboard/{slug}", response_class=HTMLResponse)
async def link_detail(slug: str, request: Request, db: Session = Depends(get_db)):
    link = db.query(Link).filter(Link.slug == slug).first()
    if not link:
        return HTMLResponse("<h1>Link not found</h1>", status_code=404)

    scans = (
        db.query(Scan)
        .filter(Scan.link_id == link.id)
        .order_by(Scan.scanned_at.desc())
        .all()
    )

    # Build map data points
    map_points = []
    for scan in scans:
        lat = scan.browser_lat or scan.ip_lat
        lng = scan.browser_lng or scan.ip_lng
        if lat and lng:
            map_points.append({
                "lat": lat,
                "lng": lng,
                "city": scan.ip_city or "Unknown",
                "time": scan.scanned_at.strftime("%b %d, %H:%M"),
                "precise": scan.browser_lat is not None,
            })

    # Unique cities
    cities = set()
    for scan in scans:
        if scan.ip_city:
            cities.add(scan.ip_city)

    base_url = str(request.base_url).rstrip("/")

    return templates.TemplateResponse("link_detail.html", {
        "request": request,
        "link": link,
        "scans": scans,
        "map_points": map_points,
        "unique_cities": len(cities),
        "base_url": base_url,
    })


# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------

@app.post("/api/links")
async def create_link(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    slug = data.get("slug", "").strip().lower()
    target_url = data.get("target_url", "").strip()
    description = data.get("description", "").strip()
    posted_location = data.get("posted_location", "").strip()

    if not target_url:
        return JSONResponse({"error": "target_url is required"}, status_code=400)

    # Auto-generate slug if not provided
    if not slug:
        slug = generate_slug()

    # Ensure target_url has a protocol
    if not target_url.startswith(("http://", "https://")):
        target_url = "https://" + target_url

    # Check for duplicate slug
    if db.query(Link).filter(Link.slug == slug).first():
        return JSONResponse({"error": f"Slug '{slug}' already exists"}, status_code=409)

    link = Link(
        slug=slug,
        target_url=target_url,
        description=description,
        posted_location=posted_location,
    )
    db.add(link)
    db.commit()
    db.refresh(link)

    return JSONResponse({
        "id": link.id,
        "slug": link.slug,
        "target_url": link.target_url,
        "tracking_url": f"/t/{link.slug}",
    }, status_code=201)


@app.delete("/api/links/{slug}")
async def delete_link(slug: str, db: Session = Depends(get_db)):
    link = db.query(Link).filter(Link.slug == slug).first()
    if not link:
        return JSONResponse({"error": "Not found"}, status_code=404)
    db.query(Scan).filter(Scan.link_id == link.id).delete()
    db.delete(link)
    db.commit()
    return JSONResponse({"ok": True})


@app.get("/api/links/{slug}/export")
async def export_scans_csv(slug: str, db: Session = Depends(get_db)):
    link = db.query(Link).filter(Link.slug == slug).first()
    if not link:
        return JSONResponse({"error": "Not found"}, status_code=404)

    scans = (
        db.query(Scan)
        .filter(Scan.link_id == link.id)
        .order_by(Scan.scanned_at.desc())
        .all()
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "scanned_at", "ip_city", "ip_region", "ip_country",
        "ip_lat", "ip_lng", "browser_lat", "browser_lng",
        "browser_accuracy", "ip_address", "user_agent",
    ])
    for s in scans:
        writer.writerow([
            s.scanned_at.isoformat() if s.scanned_at else "",
            s.ip_city or "", s.ip_region or "", s.ip_country or "",
            s.ip_lat or "", s.ip_lng or "",
            s.browser_lat or "", s.browser_lng or "", s.browser_accuracy or "",
            s.ip_address or "", s.user_agent or "",
        ])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={slug}_scans.csv"},
    )
