
import json, glob, re
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent

AREA_MAP = {
    "dubai marina":             "Dubai Marina",
    "downtown dubai":           "Downtown Dubai",
    "downtown":                 "Downtown Dubai",
    "palm jumeirah":            "Palm Jumeirah",
    "palm":                     "Palm Jumeirah",
    "business bay":             "Business Bay",
    "jumeirah beach residence": "Jumeirah Beach Residence",
    "jbr":                      "Jumeirah Beach Residence",
    "marsa dubai":              "Dubai Marina",
}

TARGET_AREAS = list(set(AREA_MAP.values()))

# Price limits per area (AED) — based on DLD data
PRICE_LIMITS = {
    "Dubai Marina":             (300_000, 80_000_000),
    "Downtown Dubai":           (400_000, 150_000_000),
    "Palm Jumeirah":            (500_000, 200_000_000),
    "Business Bay":             (250_000, 60_000_000),
    "Jumeirah Beach Residence": (300_000, 80_000_000),
}

PSF_LIMITS = (200, 8_000)   # AED/sqft min/max
SIZE_LIMITS = (100, 30_000)  # sqft min/max

def detect_area(listing):
    """Detect area from title, area field, location field."""
    text = " ".join([
        str(listing.get("title",    "") or ""),
        str(listing.get("area",     "") or ""),
        str(listing.get("location", "") or ""),
        str(listing.get("community","") or ""),
        str(listing.get("neighborhood","") or ""),
    ]).lower()
    for keyword, area in AREA_MAP.items():
        if keyword in text:
            return area
    return None

def clean_listings(listings):
    clean, rejected = [], []

    for l in listings:
        reasons = []

        # 1. Detect and fix area
        area = detect_area(l)
        if not area:
            reasons.append("unknown_area")
        else:
            l["area"] = area

        # 2. Price validation
        price = l.get("price", 0) or 0
        try: price = float(str(price).replace(",",""))
        except: price = 0

        if area and area in PRICE_LIMITS:
            mn, mx = PRICE_LIMITS[area]
            if price < mn:   reasons.append(f"price_too_low:{price:,.0f}")
            if price > mx:   reasons.append(f"price_too_high:{price:,.0f}")
        elif price <= 0:
            reasons.append("zero_price")

        l["price"] = price

        # 3. Price per sqft validation
        psf = l.get("price_per_sqft")
        if psf:
            try: psf = float(str(psf).replace(",",""))
            except: psf = None
        if psf and not (PSF_LIMITS[0] <= psf <= PSF_LIMITS[1]):
            # Recalculate from price + size if possible
            size = l.get("size_sqft")
            if size and float(size) > 0:
                psf = round(price / float(size), 1)
                if not (PSF_LIMITS[0] <= psf <= PSF_LIMITS[1]):
                    reasons.append(f"psf_outlier:{psf}")
                    psf = None
        l["price_per_sqft"] = psf

        # 4. Size validation
        size = l.get("size_sqft")
        if size:
            try: size = float(str(size).replace(",",""))
            except: size = None
            if size and not (SIZE_LIMITS[0] <= size <= SIZE_LIMITS[1]):
                reasons.append(f"size_outlier:{size}")
                size = None
        l["size_sqft"] = size

        # 5. Recalculate psf if both price and size are valid
        if price and size and size > 0 and not l["price_per_sqft"]:
            l["price_per_sqft"] = round(price / size, 1)

        # 6. URL check
        url = l.get("url","")
        if not url or not str(url).startswith("http"):
            reasons.append("missing_url")

        if reasons:
            l["rejected"] = True
            l["rejection_reasons"] = reasons
            rejected.append(l)
        else:
            l["rejected"] = False
            clean.append(l)

    return clean, rejected

if __name__ == "__main__":
    # Load all raw listing files
    raw_files = sorted(glob.glob(str(ROOT / "data/raw/*.json")))
    all_listings = []

    for f in raw_files:
        try:
            data = json.load(open(f))
            if isinstance(data, list):
                all_listings.extend(data)
            elif isinstance(data, dict):
                for key in ["records","listings","data"]:
                    if key in data and isinstance(data[key], list):
                        all_listings.extend(data[key])
                        break
        except Exception as e:
            print(f"  Skip {f}: {e}")

    print(f"Raw listings loaded: {len(all_listings)}")

    # Deduplicate by URL first
    seen, deduped = set(), []
    for l in all_listings:
        url = l.get("url","")
        if url and url in seen:
            continue
        if url: seen.add(url)
        deduped.append(l)
    print(f"After dedup: {len(deduped)}")

    # Clean
    clean, rejected = clean_listings(deduped)
    print(f"Clean: {len(clean)} | Rejected: {len(rejected)}")

    # Area breakdown
    from collections import Counter
    areas = Counter(l["area"] for l in clean)
    for area, count in sorted(areas.items(), key=lambda x: -x[1]):
        prices = [l["price_per_sqft"] for l in clean if l["area"]==area and l.get("price_per_sqft")]
        avg = round(sum(prices)/len(prices)) if prices else 0
        print(f"  {area:<30} {count:>3} listings · AED {avg:,}/sqft")

    # Save
    out_dir = ROOT / "data/processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    week = datetime.now().strftime("%Y-W%W")
    out  = out_dir / f"weekly_{week}.json"
    with open(out, "w") as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(clean)} clean listings → {out}")

    # Save rejection log
    if rejected:
        rej_out = out_dir / f"rejected_{week}.json"
        with open(rej_out, "w") as f:
            json.dump(rejected[:100], f, ensure_ascii=False, indent=2)
        print(f"Rejection log → {rej_out}")
