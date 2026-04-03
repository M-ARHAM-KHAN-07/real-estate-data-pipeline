# 🕷️ Enterprise Web Scraping Portfolio

> Two production scrapers built for a commercial real estate intelligence firm — bypassing PerimeterX bot protection, scraping all 50 US states on Zillow, and routing structured data to CRMs via Zapier webhooks.

---

## Projects

| Project | Target | Challenge | Output |
|---|---|---|---|
| [Commercial Observer Scraper](#1-commercial-observer-scraper) | Paywalled news site | Auth + category false positives | Office lease articles → Zapier |
| [Zillow Active Listings Scraper](#2-zillow-active-listings-scraper) | Zillow.com | PerimeterX CAPTCHA | Luxury listings → Zapier |

---

## Tech Stack

```
Python 3.11+         Selenium WebDriver      undetected-chromedriver
BeautifulSoup4       requests                openpyxl
Chrome DevTools Protocol (CDP)              Xvfb (Ubuntu)
Zapier Webhooks      re (regex)              logging
```

---

## 1. Commercial Observer Scraper

### What it does
Logs into a **paywalled commercial real estate publication**, crawls monthly archive pages, filters articles to the **Office Leases** category only, extracts structured content, and delivers it to a Zapier webhook.

### Pipeline
```
Login (iframe/cookie) → Archive crawl → Article links → Category filter
  → Content extract → Zapier POST → Excel dedup log
```

### Key engineering problem: False positive prevention

The "Office Leases" nav link appears on **every single page** of the site — a naive `soup.find_all("a")` would match 100% of articles.

**Solution:** Before any category check, decompose all `nav`, `header`, `footer`, and `aside` elements from the BeautifulSoup tree. Only then run the category selector chain.

```python
def is_office_lease_article(soup: BeautifulSoup) -> bool:
    # Step 1: strip global site chrome (contains nav links on every page)
    for tag in soup.find_all(["nav", "header", "footer", "aside"]):
        tag.decompose()

    # Step 2: check 12 known article-category CSS selectors
    CATEGORY_SELECTORS = [
        "div.leases-label", "div.channel", "div.article-category",
        "ul.post-categories", "span.category", "div.breadcrumb", ...
    ]
    for selector in CATEGORY_SELECTORS:
        container = soup.select_one(selector)
        if container:
            for a in container.find_all("a", href=True):
                if a["href"].endswith("/leases/office"):
                    return True

    # Step 3: fallback full-page scan (chrome already stripped)
    for a in soup.find_all("a", href=True):
        if a["href"].endswith("/leases/office"):
            return True
    return False
```

### Features
- **Authenticated session** — handles cookie consent banners + iframe-embedded login forms
- **Dynamic month targeting** — defaults to current month, configurable
- **Idempotent re-runs** — Excel log of processed URLs, safely skips already-done articles
- **Retry logic** — per-article timeout retry (2x) + Zapier webhook retry (3x with backoff)
- **Structured logging** — timestamped INFO/WARNING/ERROR throughout

### Data extracted per article
```
url | sub_headline | caption | body_text | category | date_time
```

---

## 2. Zillow Active Listings Scraper

### What it does
Scrapes **active for-sale luxury listings** (min. $7M) across all 50 US states from Zillow. Bypasses PerimeterX bot detection, handles Press & Hold CAPTCHAs programmatically, extracts full property data + agent contacts, and ships records to Zapier.

### Pipeline
```
Xvfb virtual display → Headless Chrome (undetected) → State URL gen
  → CAPTCHA detection → Multi-page crawl → ZPID dedup
  → Listing scrape → Zapier GET → Excel log
```

### Key engineering problem: PerimeterX CAPTCHA bypass

Zillow uses **PerimeterX** with a "Press & Hold" CAPTCHA that requires sustaining a mouse button press for 10–15 seconds. Standard selenium clicks fail. Third-party CAPTCHA services (2Captcha, etc.) don't support this interaction type.

**Solution:** Chrome DevTools Protocol (CDP) `Input.dispatchMouseEvent` with a realistic mouse trajectory.

```
Step 1: Detect CAPTCHA
  → Scan page source for "press & hold" / "before we continue"
  → Check DOM for #px-captcha, role="button" elements containing "press"
  → Recurse into iframes

Step 2: Human-like mouse approach (cubic ease-in-out smoothstep)
  → 8–15 step trajectory from random start position to button center
  → ±2px jitter per step, 10–40ms inter-step delay

Step 3: CDP hold
  → Input.dispatchMouseEvent: mousePressed at button centroid
  → Hold 12–16 seconds with randomised duration + real-time countdown log
  → Input.dispatchMouseEvent: mouseReleased

Step 4: Verify + fallback chain
  → Poll page every 1s for CAPTCHA clearance (max 15s)
  → 5 automatic attempts before escalating
  → 5-minute manual solve window (automated watch loop)
  → URL saved to captcha_blocked.json for retry run
```

```python
# Smoothstep cubic easing toward button (anti-pattern detection)
for step in range(steps):
    t = (step + 1) / steps
    t = t * t * (3 - 2 * t)  # smoothstep — not linear!
    mx = start_x + (cx - start_x) * t + random.uniform(-2, 2)
    my = start_y + (cy - start_y) * t + random.uniform(-2, 2)
    driver.execute_cdp_cmd('Input.dispatchMouseEvent', {
        'type': 'mouseMoved', 'x': mx, 'y': my, 'pointerType': 'mouse'
    })
    time.sleep(random.uniform(0.01, 0.04))

# CDP hold for randomised 12–16 seconds
hold_duration = random.uniform(12, 16)
driver.execute_cdp_cmd('Input.dispatchMouseEvent', {
    'type': 'mousePressed', 'x': cx, 'y': cy,
    'button': 'left', 'clickCount': 1
})
while time.time() - start_time < hold_duration:
    time.sleep(0.5)
driver.execute_cdp_cmd('Input.dispatchMouseEvent', {'type': 'mouseReleased', ...})
```

### Anti-detection techniques

| Technique | Implementation |
|---|---|
| **Browser fingerprint** | `undetected-chromedriver` patches the Chrome binary — removes `navigator.webdriver`, automation attributes, CDP indicators |
| **Virtual display** | `Xvfb :99` — Chrome runs in a real X11 session, not headless mode. Harder to fingerprint than `--headless` |
| **Randomised delays** | All waits: `random.uniform(min, max)`. Pages: 2–4s, listings: 3–6s, states: 10–20s |
| **User-agent rotation** | Pool of 4 real Chrome UA strings (macOS/Win/Linux), selected per session |
| **Mouse behaviour** | Smoothstep cubic easing, not instant movement or linear interpolation |
| **Auto browser restart** | If state fails, driver is killed and a fresh instance launched before retry |

### Data extracted per listing

```
source_platform | listing_id (ZPID) | listing_url
address_full | city | state | zip
price | beds | baths | sqft | price_per_sqft | lot_size
property_type | status | listed_date | year_built
agent_name | agent_email | agent_phone | agent_company
mls_id | days_on_zillow | views | saves
description | scraped_at
```

### Deduplication: ZPID-based, not URL-based

Zillow URLs vary between runs (query params, trailing slashes, case). URL-string matching causes duplicate scraping.

**Solution:** Extract ZPID via regex, track in a set, persist to Excel:
```python
def extract_zpid(url):
    m = re.search(r'/(\d+)_zpid', str(url))
    return m.group(1) if m else None

# Check before scraping:
if extract_zpid(url) in self.scraped_urls:
    continue  # already done
```

### Scale
- 50 US states, each with up to 30 search-results pages
- Browser restarts between states on failure — no manual intervention needed
- Stops early per state when 2 consecutive pages yield no new listings

---

## Project structure

```
commercial-observer-scraper/
├── co_scraper.py          # Main scraper — login, crawl, filter, extract, send
└── scraped_urls.xlsx      # Persistent dedup log (auto-created)

zillow-scraper/
├── zillow_scraper.py      # Main scraper — all 50 states, CAPTCHA solver
├── zillow_data_active/
│   └── captcha_blocked_urls.json   # URLs that hit persistent CAPTCHA
└── recent_listings_scrapped_urls.xlsx   # ZPID dedup log
```

---

## Running

### Commercial Observer

```bash
pip install selenium beautifulsoup4 requests openpyxl
# Set EMAIL, PASSWORD, WEBHOOK_URL in config block at top of file
python co_scraper.py
```

### Zillow

```bash
pip install undetected-chromedriver selenium requests openpyxl
# Ubuntu — install Xvfb
sudo apt install xvfb
# Set ZAPIER_WEBHOOK_URL and URL_TRACKING_FILE in config block
python zillow_scraper.py
```

---

## Notes

- Both scrapers were built and deployed for a commercial real estate intelligence company.
- Credentials and webhook URLs have been removed from this repository.
- The Zillow scraper targets the `$7M+` price tier as configured for this client's use case.
