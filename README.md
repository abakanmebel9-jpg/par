# par — Cabinet Furniture & Kitchen Design News Parser

Hourly news aggregator that fetches **cabinet-furniture / kitchen / furniture-design**
news from curated RSS sources and publishes them as a single JSON file:

- **[`data/furniture-news.json`](data/furniture-news.json)** — novelties in
  cabinet furniture, kitchen production, kitchen design, and furniture design
  (cabinets, wardrobes, kitchens, millwork, built-ins, sofas, tables, chairs,
  hardware, materials).

Output feeds the **[@abakan_mebel](https://t.me/abakan_mebel)** Telegram channel
(Abakan Furniture — Russian furniture & kitchens).

A GitHub Actions workflow runs `fetch_news.py` **every hour** (at `HH:05`) and
commits the refreshed JSON file back to the repo.

Built on the architecture of
[`sochiautoparts/nws`](https://github.com/sochiautoparts/nws) (an automotive
news parser), adapted for the cabinet-furniture / kitchen-design domain.

---

## How it works

```
RSS sources (35 hand-tested feeds — curated for @abakan_mebel)
        │
        ▼
   fetch_news.py        ← feedparser + requests, parallel fetch
        │
        ├─► normalize items (title, summary, url, image, published)
        ├─► extract lead image (enclosure / media:content / media:thumbnail / <img>)
        ├─► ★ garbage-photo guard — drop items whose only image is a logo/icon/tracker
        ├─► ★ quality guard — drop items with no real content photo
        ├─► ★ relevance classifier — keep only cabinet-furniture/kitchen/furniture
        │     items (English + Russian keyword sets)
        ├─► ★ multi-photo scrape — for 21 gallery-enabled sources, fetch article
        │     page and extract up to 5 additional gallery photos
        ├─► dedup by id = sha256(url + title)
        ├─► recency filter: ≤ 90 days
        │
        ▼
   ┌─────────────────────────┐
   │  furniture-news.json    │
   │  top 800                │
   └─────────────────────────┘
```

### Output size (typical run)

| File | Items | Sources | Multi-photo items |
|---|---:|---:|---:|
| `furniture-news.json` | ~140 | ~25 | ~40 |

Each multi-photo item carries up to 6 distinct image URLs (lead first).

---

## Sources (35 hand-tested feeds — 2026-06, curated for @abakan_mebel)

All sources return RSS feeds with quality photos embedded (`media:content`,
enclosures, or `<img>` in summary). Sources were tested 2026-06 for: working
HTTP endpoint, valid feed, embedded photos, recent relevant content.

> **Curation note for @abakan_mebel:** Foreign forestry / wood-industry /
wooden-furniture-INDUSTRY sources (Woodworking Network, Popular Woodworking,
Woodshop News, RTA Cabinet Store, Industry Today) were **REMOVED** — they
published lumber-harvest / sawmill / forest-management news instead of
furniture & kitchen DESIGN. Forestry phrases are also in `BLOCKLIST`.

### Russian sources (4) — for the @abakan_mebel Russian audience

- **АМДПР** — `amedoro.com/ru/news/novosti-otrasli.feed?type=rss` — Association
  of Furniture & Woodworking Industry Enterprises of Russia (Mr.Doors, Felix,
  TBM, Basis, WOODEX MEBEL SUMMIT — Russian furniture-industry news)
- **Мебель-expo** — `meb-expo.ru/ru/rss/` — official "Мебель" trade-show portal
  (Russian furniture fair, industry digest)
- **Archi.ru** — `archi.ru/rss.xml` — leading Russian architecture & interior
  magazine
- **Rmnt.ru** — `rmnt.ru/rss/news.xml` — Russian home, repair & interior portal

### Design portals — broad (16, international furniture/kitchen DESIGN)

- **Dezeen** — main + Interiors (`dezeen.com/feed/`, `/interiors/feed/`) —
  leading global design & architecture magazine
- **Design Milk** — main + Interiors + Architecture + Technology + Art
  (`design-milk.com/feed/` + 4 category feeds) — contemporary design
  destination with strong furniture coverage
- **Design Boom** — `designboom.com/feed/` — design, architecture, art
  magazine with product/furniture focus
- **Yanko Design** — `yankodesign.com/feed/` — industrial design & product
  concept magazine (furniture, lighting, accessories)
- **Trendir** — `trendir.com/feed/` — modern home & furniture trends
- **Homedit** — `homedit.com/feed/` — home design ideas with deep kitchen and
  furniture coverage
- **Decoist** — `decoist.com/feed/` — kitchen / countertop / furniture design
  (strong kitchen signal: "Outdated Countertop Material", kitchen trends)
- **Wallpaper** — `wallpaper.com/rss.xml` — international design magazine,
  strong furniture & interiors coverage
- **Minimalissimo** — `minimalissimo.com/feed` — minimalist product / furniture
  / lighting design
- **ArchDaily** — `archdaily.com/feed` — architecture + interiors with rich
  project galleries
- **Apartment Therapy** — `apartmenttherapy.com/main.rss` — home & furniture
  design ideas (kitchen, furniture, small-space solutions)

### Design portals — topic-specific, high relevance (6)

- **Design Milk tag kitchen** — `design-milk.com/tag/kitchen/feed/` — every
  Design Milk post tagged "kitchen"
- **Design Milk tag furniture** — every post tagged "furniture"
- **Design Milk tag cabinets** — every post tagged "cabinets"
- **Dezeen tag kitchens** — `dezeen.com/tag/kitchens/feed/` — every Dezeen
  post tagged "kitchens" (modular kitchen products, kitchen design projects)
- **Dezeen tag cabinets** — every post tagged "cabinets" (larder cupboards,
  pantry cabinets, storage cabinets)
- **Dezeen tag furniture** — every post tagged "furniture" (new furniture
  collections, designer collaborations)

### Home / lifestyle magazines (8)

- **Real Homes** — `realhomes.com/rss` — UK home magazine with extensive
  kitchen content
- **Homes & Gardens** — `homesandgardens.com/rss` — UK home & design magazine
- **Livingetc** — `livingetc.com/rss` — modern living & interiors magazine
- **Sunset** — `sunset.com/rss` — Western-US home & lifestyle magazine with
  outdoor kitchen/furniture coverage
- **Elle Decor** — `elledecor.com/rss/all.xml` — design magazine
- **House Beautiful** — `housebeautiful.com/rss/all.xml` — home magazine with
  kitchen tours & design tips
- **Veranda** — `veranda.com/rss/all.xml` — high-end interiors magazine
- **Ideal Home** — `idealhome.co.uk/api/rss` — UK home & kitchen magazine

### Outdoor furniture (1)

- **Gardenista** — `gardenista.com/feed/` — sister site of Remodelista,
  outdoor furniture, garden rooms, sheds & outbuildings

Sources marked **gallery-enabled** have `scrape_gallery: true` —
the parser fetches the article page for the top 3 most recent items and
extracts up to 5 additional gallery photos (so each item ends up with up to
6 images total).

---

## Relevance classifier

Each item's title + summary is checked against:

### Strong keywords (English)

`kitchen` · `cabinet` · `cabinetry` · `wardrobe` · `closet` · `cupboard` ·
`larder` · `pantry` · `millwork` · `case furniture` · `casegoods` ·
`furniture` · `furnishings` · `bookcase` · `bookshelf` · `shelving` ·
`dresser` · `sideboard` · `credenza` · `hutch` · `buffet` · `sofa` · `couch` ·
`loveseat` · `sectional` · `dining table` · `coffee table` · `console table` ·
`chair` · `armchair` · `stool` · `ottoman` · `vanity` · `bathroom vanity` ·
`built-in` · `fitted furniture` · `modular furniture` · `MDF` · `plywood` ·
`veneer` · `laminate` · `particleboard` · `melamine` · `hardwood` ·
`softwood` · `timber` · `lumber` · `hinge` · `drawer slide` · `soft-close` ·
`countertop` · `worktop` · `backsplash` · `island` · `kitchen island` ·
`cabinetmaker` · `woodworker` · `woodworking` · `cabinet hardware` ·
`interior design` · `interiors`

### Strong keywords (Russian — prefix match for morphology)

`кухн-` · `корпусн-` · `шкаф-` · `гардероб-` · `стеллаж-` · `полк-` ·
`мебел-` · `стол` · `стул-` · `кресло-` · `диван-` · `тумб-` · `комод-` ·
`витрин-` · `фасад-` · `ЛДСП` · `МДФ` · `дсп` · `шпон` · `фурнитур-` ·
`петл-` · `направляющие` · `столешниц-` · `фрезеровк-` · `распил` ·
`дизайнер мебели` · `дизайн мебели` · `дизайн интерьер` · `интерьер` ·
`кухонный гарнитур` · `мебельный щит` · `встроенн-` · `модульн-`

### Loose patterns (need ≥2 distinct matches)

Each of the strong-keyword categories above is also encoded as a loose
regex pattern; if no single strong keyword matches, the classifier requires
≥2 distinct loose-pattern matches (e.g. "MDF" + "countertop" → relevant
even without explicit "cabinet" keyword).

An item is kept iff: ≥1 strong keyword match OR ≥2 distinct loose matches.

---

## Garbage-photo guard

Every extracted image URL is filtered through `is_garbage_image()` which rejects:

| Category | Examples |
|---|---|
| Logos / icons | `/logo`, `/icons/`, `/favicon`, `/sprite`, `foo-logo.png`, `_logo` |
| Trackers / ad pixels | `doubleclick`, `google-analytics`, `facebook.com/tr`, `/pixel`, `/beacon` |
| Avatars / author bylines | `/avatar`, `/authors/`, `gravatar`, `/profile-pic`, `-avatar`, `-author` |
| Placeholders / spacers | `placeholder`, `transparent`, `16x9-tr.png`, `default-image`, `no-image`, `/blank.` |
| Tiny dimensions | `1x1`, `?w=1&h=1`, `-90x90.jpg`, `-32x32.png`, any `w`/`h` ≤ 32 |
| Social media buttons | `/social/`, `twitter.com`, `instagram.com`, `youtube.com`, `facebook.com`, `tiktok.com` |
| Theme / site chrome | `/wp-content/themes/`, `/wp-content/plugins/`, `/wp-includes/`, `/assets/images/`, `/assets/dist/`, `/dist/images/`, `/img/icons/`, `/img/social/`, `/img/logo` |
| Shopping / affiliate | `amazon.com`, `shopify`, `/shop/`, `/store/` |
| GIFs (almost always animated icons in feeds) | `.gif` |
| Emoji | `emoji`, `/emoticons/` |

If an item's only image is garbage, **the item is dropped entirely** —
guaranteeing the JSON file only contains real content photos.

Additionally, items with **no image at all** are dropped (`REQUIRE_IMAGE = True`)
— this enforces the "quality photos" requirement end-to-end.

---

## Multi-photo scraping

For sources flagged `scrape_gallery: true`, the parser:

1. Takes the top 3 most recent items from the RSS feed
2. Fetches each article's HTML page
3. Extracts `<img>` URLs (including `srcset`, `data-src`, `data-lazy-src`)
4. Groups them by base URL (stripping query strings & WordPress size suffixes
   like `-1024x576.jpg`), and picks the **largest** variant per group
5. Filters out garbage URLs (same filter as above)
6. Caps at 6 total images per item (lead image first, then 5 extras)

This means top-tier sources (Design Milk, Dezeen, Homes & Gardens, House
Beautiful, Veranda, Livingetc, Ideal Home, Real Homes, Trendir, Homedit,
Design Boom, Decoist, Wallpaper, ArchDaily, Apartment Therapy, Rmnt.ru,
DM tag kitchen/furniture/cabinets, Dezeen tag kitchens, Elle Decor)
provide rich multi-photo news items.

---

## Output JSON schema

```jsonc
{
  "kind": "furniture",
  "topic": "cabinet furniture, kitchens, furniture design",
  "generated_at": "2026-06-18T15:26:20+00:00",
  "generated_at_human": "2026-06-18 15:26 UTC",
  "total_items": 143,
  "sources_used": ["Dezeen", "Design Milk", "Homes & Gardens", ...],
  "sources_count": 25,
  "multi_photo_items": 41,            // count of items with >1 image
  "items": [
    {
      "id": "a3f8c1d9...",             // sha256(url+title)[:16]
      "title": "Hampton Lumber announces 2026 Lumber Wrap Design winners",
      "summary": "Short excerpt (≤600 chars)...",
      "url": "https://www.woodworkingnetwork.com/...",
      "image": "https://wwn-files-live.s3.../Darrington-design-2026-1024.png",  // lead image (backwards-compat)
      "images": [                      // array of image URLs (lead first)
        "https://wwn-files-live.s3.../Darrington-design-2026-1024.png",
        "https://wwn-files-live.s3.../Willamina-design-2026-1024.png",
        "https://wwn-files-live.s3.../Cowlitz-design-2026-1024.png",
        ...                           // up to 6 entries
      ],
      "source": "Woodworking Network",
      "source_url": "https://www.woodworkingnetwork.com",
      "published": "2026-06-15T10:30:00+00:00"
    }
  ]
}
```

**Field notes:**
- `image` (string) — the lead image; kept for backwards compatibility with
  consumers that expect a single image.
- `images` (array of strings) — list of all quality image URLs for this item,
  lead image first. Always has ≥1 entry (imageless items are dropped). Capped
  at 6 entries. Use this field for multi-photo UIs.

---

## Usage

### Local run

```bash
pip install -r requirements.txt
python fetch_news.py
# → writes data/furniture-news.json
```

### GitHub Actions

The workflow at `.github/workflows/fetch-news.yml`:

- Runs automatically every hour at `HH:05 UTC`
- Can be triggered manually from the **Actions** tab ("Fetch Furniture News" →
  "Run workflow")
- Runs on every push that touches `fetch_news.py` / `requirements.txt` / the
  workflow itself
- Commits the refreshed JSON file back to `main` with message
  `chore(news): hourly refresh @ <timestamp>`
- Uses the built-in `GITHUB_TOKEN` — no extra secrets required

---

## Config knobs

Defined at the top of `fetch_news.py`:

| Constant | Default | Meaning |
|---|---|---|
| `HTTP_TIMEOUT` | `20` | Per-RSS-request timeout (seconds) |
| `HTML_TIMEOUT` | `12` | Per-article-page timeout for gallery scraping |
| `MAX_ITEMS_PER_FEED` | `30` | Cap per source so one noisy feed can't dominate |
| `MAX_AGE_DAYS` | `90` | Items older than this are dropped (90d matches the reference nws BMW setting for niche topics) |
| `MAX_GALLERY_SCRAPE_PER_SOURCE` | `3` | How many article pages to fetch per gallery source |
| `MAX_IMAGES_PER_ITEM` | `6` | Cap on the `images` array (incl. lead image) |
| `OUTPUT_CAP` | `800` | Max items in furniture-news.json |
| `REQUIRE_IMAGE` | `True` | Drop items with no real content photo (quality requirement) |

---

## Adding a source

1. Test the feed URL with `curl -A "Mozilla/5.0 ..." <url> | head -100`
2. Verify it returns HTTP 200 and includes images in entries (enclosure /
   `media:content` / `media:thumbnail` / `<img>` in summary)
3. Add a row to `SOURCES` in `fetch_news.py`
4. If the source has multi-photo galleries on article pages, add
   `"scrape_gallery": True` to the source dict
5. Run `python fetch_news.py` locally to verify

If a source starts 404'ing, the parser logs a warning and continues with the
rest — no manual intervention needed.

---

## Architecture credit

This parser is structurally based on
[`sochiautoparts/nws`](https://github.com/sochiautoparts/nws) — an automotive
news aggregator (BMW + general auto) that runs hourly and publishes two JSON
files. The core building blocks were reused verbatim or near-verbatim:

- `feedparser` + `requests` parallel fetching via `ThreadPoolExecutor`
- Lead-image extraction from `enclosures` / `media_content` /
  `media_thumbnail` / `entry.links` / `<img>` in summary/content
- `is_garbage_image()` URL-pattern filter (same patterns as reference)
- `_ImgCollector` HTML parser + `extract_gallery_from_html` for multi-photo
  scraping (largest-variant-per-group selection, WordPress size-suffix
  stripping)
- `item_id = sha256(url + title)[:16]` dedup
- `is_recent()` recency filter with `datetime.fromisoformat`
- `sort_newest_first()` with empty-date fallback
- GitHub Actions hourly cron + 3-attempt push retry with `git pull --rebase
  -X theirs`

The adaptations for the cabinet-furniture / kitchen-design domain:

- Single JSON output (`data/furniture-news.json`) instead of two
- `REQUIRE_IMAGE = True` quality guard (imageless items dropped)
- `is_furniture_relevant()` classifier (English + Russian keyword sets,
  strong-match + loose-pattern logic) instead of `is_bmw_relevant()`
- Domain-specific blocklist
- 90-day recency window (vs. reference's 30/90 day split for two files)
