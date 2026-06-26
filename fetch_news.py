#!/usr/bin/env python3
"""
Cabinet furniture & kitchen design news parser — fetches RSS feeds from
curated kitchen-design / furniture-design / interior sources, keeps only
items relevant to cabinet furniture or kitchen/furniture design, and writes
ONE JSON file: data/furniture-news.json.

Runs hourly via GitHub Actions. Output feeds the @abakan_mebel Telegram
channel (Abakan Furniture — Russian furniture & kitchens).

Sources were hand-tested for:
  - Working RSS endpoint (HTTP 200 with valid feed)
  - Quality photos embedded in feed (media:content / enclosures / <img>)
  - Recent, relevant content
  - Multi-photo support: top-tier sources have `scrape_gallery=True` so the
    parser fetches the article page and extracts up to N additional photos
    (satisfying the "preferably with multiple photos" requirement).

All extracted images are filtered through is_garbage_image() to guarantee
no logos, icons, trackers, or placeholders ever land in the JSON output.

Source list (35 hand-tested feeds — 2026-06, curated for @abakan_mebel):
  - Russian sources (furniture industry / interior / trade show): 4
  - Design portals (broad, international): 16
  - Design portals (topic-specific kitchens/cabinets/furniture): 6
  - Home/lifestyle magazines: 8
  - Gardenista (sister site of Remodelista, outdoor furniture): 1

NOTE: Foreign forestry / wood-industry / wooden-furniture-INDUSTRY sources
(Woodworking Network, Popular Woodworking, Woodshop News, RTA Cabinet Store,
Industry Today) were REMOVED — they published lumber-harvest / sawmill /
forest-management news instead of furniture & kitchen DESIGN. Forestry
phrases are also in BLOCKLIST as a safety net.

Topic coverage:
  - Cabinets / cabinetry / millwork
  - Kitchens (production + design)
  - Wardrobes / cupboards / shelving
  - Furniture (case furniture, sofas, chairs, tables)
  - Built-ins / fitted furniture
  - Hardware (fittings, hinges, drawer slides)
  - Materials (wood, MDF, veneer, laminates)
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse, parse_qs

import feedparser
import requests

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("furniture-news")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
HTTP_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
HTML_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html, application/xhtml+xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",  # helps bypass 403 bot-blocks on some sites
}

HTTP_TIMEOUT = 20
HTML_TIMEOUT = 12
MAX_ITEMS_PER_FEED = 30            # cap so one noisy feed can't dominate
MAX_AGE_DAYS = 90                  # only keep items newer than this — 90d matches the
                                  # reference nws BMW setting, since cabinet-furniture
                                  # news is a niche topic with lower publish frequency
MAX_GALLERY_SCRAPE_PER_SOURCE = 3  # how many article pages to fetch per gallery source
MAX_IMAGES_PER_ITEM = 6            # cap on the `images` array (incl. lead image)
OUTPUT_CAP = 800                   # max items in furniture-news.json
REQUIRE_IMAGE = True               # drop items with no real content photo (quality
                                  # requirement: "качественные источники с качественными
                                  # фотографиями")
MIN_TITLE_LEN = 20                 # drop items with title shorter than this (catches
                                  # terse product-name-only titles like "Sacha Chair")
MIN_SUMMARY_LEN = 100              # drop items with summary shorter than this (catches
                                  # clickbait listicles with no real excerpt)

# ─────────────────────────────────────────────────────────────────────────────
# Curated source list — hand-tested 2026-06
# ─────────────────────────────────────────────────────────────────────────────
SOURCES: list[dict[str, Any]] = [
    # ═══════════════════════════════════════════════════════════════════════════
    # CURATED FOR @abakan_mebel — furniture & kitchen DESIGN focus
    #
    # Foreign forestry / wood-industry / wooden-furniture-industry sources were
    # REMOVED per channel requirements (Woodworking Network, Popular Woodworking,
    # Woodshop News, RTA Cabinet Store, Industry Today — they published lumber-
    # harvest / sawmill / forest-management news instead of furniture DESIGN).
    #
    # Added Russian-language sources (АМДПР, meb-expo, Archi.ru) and additional
    # international furniture/kitchen DESIGN portals (Decoist, Wallpaper,
    # Minimalissimo, ArchDaily, Apartment Therapy). All new feeds were
    # hand-tested 2026-06 for: HTTP 200, valid feed, embedded photos, recent
    # relevant content.
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Russian sources (4) — for the @abakan_mebel Russian audience ──────────
    # АМДПР = Association of Furniture & Woodworking Industry Enterprises of
    # Russia. Russian furniture-industry news (Mr.Doors, Felix, TBM, Basis).
    {"name": "АМДПР",                    "url": "http://amedoro.com/ru/news/novosti-otrasli.feed?type=rss"},
    # meb-expo = official "Мебель" trade-show portal (Russian furniture fair).
    {"name": "Мебель-expo",              "url": "https://www.meb-expo.ru/ru/rss/"},
    # Archi.ru = leading Russian architecture & interior magazine.
    {"name": "Archi.ru",                 "url": "https://archi.ru/rss.xml"},
    # Rmnt.ru = Russian home, repair & interior portal.
    {"name": "Rmnt.ru",                  "url": "https://www.rmnt.ru/rss/news.xml",                 "scrape_gallery": True},

    # ── Design portals — broad (international, furniture/kitchen DESIGN) ─────
    {"name": "Dezeen",                   "url": "https://www.dezeen.com/feed/",                     "scrape_gallery": True},
    {"name": "Dezeen Interiors",         "url": "https://www.dezeen.com/interiors/feed/"},
    {"name": "Design Milk",              "url": "https://design-milk.com/feed/",                    "scrape_gallery": True},
    {"name": "Design Milk Interiors",    "url": "https://design-milk.com/category/interior-design/feed/"},
    {"name": "Design Milk Architecture", "url": "https://design-milk.com/category/architecture/feed/"},
    {"name": "Design Milk Art",          "url": "https://design-milk.com/category/art/feed/"},
    # NOTE: Design Boom (main + design + interiors) REMOVED 2026-06 — all return HTTP 403.
    # NOTE: Design Milk Technology REMOVED 2026-06 — 0 items (tag feed empty).
    {"name": "Yanko Design",             "url": "https://www.yankodesign.com/feed/"},
    {"name": "Trendir",                  "url": "https://www.trendir.com/feed/",                    "scrape_gallery": True},
    {"name": "Homedit",                  "url": "https://www.homedit.com/feed/",                    "scrape_gallery": True},
    # ★ added — kitchen/countertop/furniture design (strong kitchen signal)
    {"name": "Decoist",                  "url": "https://www.decoist.com/feed/",                    "scrape_gallery": True},
    # ★ added — international design magazine, strong furniture/interiors
    {"name": "Wallpaper",                "url": "https://www.wallpaper.com/rss.xml",                "scrape_gallery": True},
    # ★ added — minimalist product/furniture/lighting design
    # NOTE: Minimalissimo REMOVED 2026-06 — feed produces terse product-name-only
    # titles ("Sacha Chair", "Azukimé Chair", "Chunk Stool") that fail the new
    # MIN_TITLE_LEN filter. Content is real but too sparse for the channel.
    # {"name": "Minimalissimo", "url": "https://minimalissimo.com/feed"},
    # ★ added — architecture + interiors, rich project galleries
    {"name": "ArchDaily",                "url": "https://www.archdaily.com/feed",                   "scrape_gallery": True},
    # ★ added — home & furniture design ideas
    {"name": "Apartment Therapy",        "url": "https://www.apartmenttherapy.com/main.rss",       "scrape_gallery": True},

    # ── Design portals — topic-specific (high relevance) ─────────────────────
    {"name": "DM tag kitchen",           "url": "https://design-milk.com/tag/kitchen/feed/",        "scrape_gallery": True},
    {"name": "DM tag furniture",         "url": "https://design-milk.com/tag/furniture/feed/",      "scrape_gallery": True},
    {"name": "DM tag cabinets",          "url": "https://design-milk.com/tag/cabinets/feed/",       "scrape_gallery": True},
    # NOTE: DM tag lighting REMOVED 2026-06 — 0 items (tag feed empty).
    # NOTE: DM tag office REMOVED 2026-06 — 0 items (tag feed empty).
    {"name": "Dezeen tag kitchens",      "url": "https://www.dezeen.com/tag/kitchens/feed/",        "scrape_gallery": True},
    {"name": "Dezeen tag cabinets",      "url": "https://www.dezeen.com/tag/cabinets/feed/"},
    {"name": "Dezeen tag furniture",     "url": "https://www.dezeen.com/tag/furniture/feed/"},

    # ── Home / lifestyle magazines (kitchen tours, furniture, interiors) ─────
    # NOTE: Homes & Gardens, House Beautiful, Ideal Home, The Kitchn, Bob Vila were
    # REMOVED 2026-06 (affiliate/shopping-deal noise: Amazon picks, Prime Day,
    # "% off", Martha Stewart gadget pushes). Even with the new BLOCKLIST patterns
    # filtering individual deal posts, these sources' editorial mix was too
    # shopping-heavy to keep — they polluted the channel with non-design content.
    # Kept: Real Homes, Livingetc, Sunset, Elle Decor, Veranda (more editorial).
    {"name": "Real Homes",               "url": "https://www.realhomes.com/rss",                    "scrape_gallery": True},
    # NOTE: Livingetc REMOVED 2026-06 — 29% bad (affiliate/clickbait). Replaced by
    # higher-quality kitchen-design sources (DM tag kitchen, Trendir furniture).
    # {"name": "Livingetc", "url": "https://www.livingetc.com/rss", "scrape_gallery": True},
    {"name": "Sunset",                   "url": "https://www.sunset.com/rss",                       "scrape_gallery": True},
    {"name": "Elle Decor",               "url": "https://www.elledecor.com/rss/all.xml",            "scrape_gallery": True},
    {"name": "Veranda",                  "url": "https://www.veranda.com/rss/all.xml",              "scrape_gallery": True},

    # ── Gardenista (sister site of Remodelista, outdoor furniture) ───────────
    {"name": "Gardenista",               "url": "https://www.gardenista.com/feed/"},

    # ═══════════════════════════════════════════════════════════════════════════
    # EXPANDED SOURCES (2026-06) — hand-tested HTTP 200 + valid feed + photos.
    # Added to broaden coverage of the abakanmebel.online product categories:
    # kitchens, sliding wardrobes, living/bedroom sets, bathroom furniture,
    # walk-in closets, office furniture. See worklog for test results.
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Extra Russian sources — interior / architecture projects (RU audience) ──
    # Archi.ru projects feed returns ~30 interior/architecture items with inline
    # images (vs the 4-item main feed). Same publisher, deeper content.
    {"name": "Archi.ru Projects",        "url": "https://archi.ru/projects/rss.xml",                "scrape_gallery": True},
    {"name": "Archi.ru News",            "url": "https://archi.ru/news/rss.xml"},

    # ── Remodelista — sister site of Gardenista, home/kitchen/bath DESIGN ─────
    {"name": "Remodelista",              "url": "https://www.remodelista.com/feed/",                "scrape_gallery": True},

    # ── International home / kitchen / interior magazines ──────────────────────
    # NOTE: Country Living REMOVED 2026-06 — 80% bad (Prime Day affiliate +
    # summary-short listicles). The feed is dominated by Amazon-shopping content
    # during sale events, not furniture/interior DESIGN.
    # {"name": "Country Living", "url": "https://www.countryliving.com/rss/all.xml"},
    # NOTE: The Kitchn REMOVED 2026-06 — feed was 100% affiliate/recipe noise
    # (Prime Day food deals, gadget lists). Even with BLOCKLIST patterns, every
    # recent item was shopping/recipe, not kitchen DESIGN.
    # Yellowtrace — AU design blog, strong furniture/interior photography.
    {"name": "Yellowtrace",              "url": "https://www.yellowtrace.com.au/feed/",             "scrape_gallery": True},

    # ── Design Boom — REMOVED 2026-06 (HTTP 403 on all feeds) ──
    # Was: Design Boom design + Design Boom interiors — both 403.

    # ── Dezeen tag feeds — direct coverage of site product categories ──────────
    # Bathrooms → «Мебель для ванной»; Lighting → kitchen/wardrobe/bath; Storage →
    # cabinets/shelving/wardrobes; Bedrooms → «Гарнитуры»; Offices → «Офисная
    # мебель»; Wardrobes → «Шкафы-купе / Гардеробные». All return media:content.
    {"name": "Dezeen tag bathrooms",     "url": "https://www.dezeen.com/tag/bathrooms/feed/",       "scrape_gallery": True},
    {"name": "Dezeen tag lighting",      "url": "https://www.dezeen.com/tag/lighting/feed/",        "scrape_gallery": True},
    {"name": "Dezeen tag storage",       "url": "https://www.dezeen.com/tag/storage/feed/",         "scrape_gallery": True},
    {"name": "Dezeen tag bedrooms",      "url": "https://www.dezeen.com/tag/bedrooms/feed/",        "scrape_gallery": True},
    {"name": "Dezeen tag offices",       "url": "https://www.dezeen.com/tag/offices/feed/",         "scrape_gallery": True},
    {"name": "Dezeen tag wardrobes",     "url": "https://www.dezeen.com/tag/wardrobes/feed/",       "scrape_gallery": True},

    # ── Design Milk tag feeds — direct coverage of site product categories ─────
    # bathroom → «Мебель для ванной»; bedroom/living room → «Гарнитуры»; office →
    # «Офисная мебель»; wardrobe → «Шкафы-купе / Гардеробные»; lighting & storage
    # → kitchen/wardrobe/bath fittings and cabinetry.
    {"name": "DM tag bathroom",          "url": "https://design-milk.com/tag/bathroom/feed/",       "scrape_gallery": True},
    {"name": "DM tag bedroom",           "url": "https://design-milk.com/tag/bedroom/feed/",        "scrape_gallery": True},
    {"name": "DM tag living room",       "url": "https://design-milk.com/tag/living-room/feed/",   "scrape_gallery": True},
    # NOTE: DM tag office REMOVED 2026-06 — 0 items (tag feed empty).
    {"name": "DM tag wardrobe",          "url": "https://design-milk.com/tag/wardrobe/feed/",       "scrape_gallery": True},
    # NOTE: DM tag lighting REMOVED 2026-06 — 0 items (tag feed empty).
    {"name": "DM tag storage",           "url": "https://design-milk.com/tag/storage/feed/",        "scrape_gallery": True},

    # ═══════════════════════════════════════════════════════════════════════════
    # EXPANDED SOURCES (2026-06 batch) — hand-tested HTTP 200 + valid feed + photos.
    # Added to broaden coverage of cabinet-furniture / interior-project photography.
    # Each was verified: feed returns 200, items have embedded images, recent titles
    # match cabinet/furniture/kitchen/interior DESIGN keywords, sample images load
    # with quality dimensions (most ≥1000px). See /home/z/my-project/worklog.md.
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Premium interior / home tours (strong project photography) ──────────────
    # NOTE: Architectural Digest REMOVED 2026-06 — 67% bad (Prime Day affiliate,
    # "Best Coffee Tables" shopping listicles). Despite high-res photos, the
    # editorial content is dominated by shopping/deals, not design projects.
    # {"name": "Architectural Digest", "url": "https://www.architecturaldigest.com/feed/rss", "scrape_gallery": True},
    # NOTE: House Beautiful UK REMOVED 2026-06 — 56% bad (clickbait + summary-short).
    # {"name": "House Beautiful UK", "url": "https://www.housebeautiful.com/uk/rss/all.xml"},
    # Hunker — interiors/furniture design. Sample photos: 1600x899.
    {"name": "Hunker",                   "url": "https://www.hunker.com/feed",                       "scrape_gallery": True},

    # ── Interior / architecture project portfolios (cabinet furniture in situ) ──
    # Home Adore — interior project showcases. Sample photos: 1050x700+.
    {"name": "Home Adore",               "url": "http://homeadore.com/feed",                        "scrape_gallery": True},
    # NOTE: Archilovers Projects + Divisare REMOVED 2026-06 — pure architecture
    # project feeds (house names, no furniture keywords); produced 0 furniture-
    # relevant items after the classifier. Kept Home Adore which has interior
    # project showcases that do pass the furniture/interior relevance filter.

    # ── Cabinet / furniture design specialists ──────────────────────────────────
    # Case Furniture UK — furniture designer's project case studies (restaurants,
    # colleges, installations). 8/8 furniture relevance, photos 1280x560+.
    {"name": "Case Furniture UK",        "url": "https://www.casefurniture.com/blogs/news.atom"},
    # NOTE: RTA Cabinet Store REMOVED 2026-06 — 50% bad (affiliate marketing
    # content like "The Wall You've Been Ignoring Deserves Better" is a sales
    # pitch, not a design article). Cabinet topic is well-covered by Popular
    # Woodworking + Woodshop News + Wood Whisperer instead.
    # {"name": "RTA Cabinet Store", "url": "https://www.rtacabinetstore.com/blog/feed/"},
    # Core77 — industrial/furniture design magazine, 7/8 furniture relevance.
    # Photos 400x400 (square crops, but content-rich).
    {"name": "Core77",                   "url": "https://www.core77.com/rss.xml"},
    # NKBA — National Kitchen & Bath Association. Industry but kitchen/bath focused.
    {"name": "NKBA",                     "url": "https://www.nkba.org/feed/"},
    # Id Lights — interior/lighting design blog. 4/8 furniture relevance.
    {"name": "Id Lights",                "url": "https://idlights.com/feed/"},

    # ── Russian sources (critical for @abakan_mebel Russian audience) ───────────
    # NOTE: Novate.ru REMOVED 2026-06 — feed images are tiny ~150px preview
    # thumbnails (novate.ru/preview/<id>s1.jpg) with no larger version available,
    # and the feed is mostly off-topic (cars, science, lifehacks) — only ~3/40
    # items pass the furniture relevance filter. Replaced by Vse-o-remonte which
    # has full-size content photos and 8/8 furniture relevance.
    # Vse-o-remonte.ru — Russian renovation/interior site. 8/8 furniture relevance!
    # Kitchen/bathroom/countertop installation articles with photos in <content>.
    {"name": "Vse-o-remonte",            "url": "https://vse-o-remonte.ru/feed",                    "scrape_gallery": True},

    # ═══════════════════════════════════════════════════════════════════════════
    # EXPANDED SOURCES (2026-06 batch 2) — 32 more hand-verified feeds
    # Each verified: HTTP 200, valid feed, items have embedded images, recent
    # titles match cabinet/furniture/kitchen/interior DESIGN keywords, sample
    # images load with quality dimensions. See /home/z/my-project/worklog.md
    # and feed_candidates_report.json / feed_candidates_report_batch2.json.
    # ═══════════════════════════════════════════════════════════════════════════

    # ── More UK home / interior magazines (kitchen tours, furniture, interiors) ──
    # NOTE: House & Garden UK REMOVED 2026-06 — all RSS paths return 404.
    # NOTE: Country Living UK REMOVED 2026-06 — 100% bad (garden-furniture-sales +
    # summary-short). The feed is lifestyle/garden content, not furniture DESIGN.
    # NOTE: Good Housekeeping UK REMOVED 2026-06 — 100% bad (summary-short
    # clickbait, lifestyle content, no furniture/interior focus).
    # {"name": "Country Living UK", "url": "https://www.countryliving.com/uk/rss/all.xml"},
    # {"name": "Good Housekeeping UK", "url": "https://www.goodhousekeeping.com/uk/rss/all.xml"},

    # ── Australian interior magazines (strong kitchen/furniture project photos) ──
    # ★ Interiors Addict AU scored 8/8 furniture-relevance in testing — direct
    # kitchen/furniture/cabinet coverage (e.g. "Farmhouse sinks now available as
    # part of kaboodle kitchen's range").
    {"name": "Interiors Addict AU",      "url": "https://theinteriorsaddict.com/feed/",               "scrape_gallery": True},
    {"name": "Home Beautiful AU",        "url": "https://www.homebeautiful.com.au/rss",               "scrape_gallery": True},
    {"name": "Homes To Love AU",         "url": "https://www.homestolove.com.au/feed",                "scrape_gallery": True},
    {"name": "Better Homes & Gardens AU","url": "https://www.bhg.com.au/rss"},
    # NOTE: Belle Magazine AU REMOVED 2026-06 — all RSS paths return 404.
    # NOTE: The Design Files AU REMOVED 2026-06 — HTTP 403 (bot-blocked).
    # NOTE: Inside Out AU + Real Living AU REMOVED 2026-06 — DNS resolution fails
    # from sandbox AND GitHub Actions runner (Are Media properties are geo-blocked
    # to AU/NZ IPs). Produced 0 items in every test run.
    # {"name": "Inside Out AU", "url": "https://www.insideout.com.au/rss"},
    # {"name": "Real Living AU", "url": "https://www.realliving.com.au/rss"},

    # ── Kitchen & bath trade / period homes (cabinet furniture in situ) ──────────
    {"name": "Period Homes",             "url": "https://www.period-homes.com/feed/"},

    # ── Cabinet / furniture-making publications (project-rich, real workshops) ──
    # Hand-tested 2026-06: each returns furniture/cabinet-making articles with
    # project photography (not industry/factory news — filtered by blocklist).
    {"name": "Popular Woodworking",      "url": "https://www.popularwoodworking.com/feed/"},
    {"name": "Woodshop News",            "url": "https://www.woodshopnews.com/rss"},
    {"name": "Wood Whisperer",           "url": "https://thewoodwhisperer.com/feed/"},
    {"name": "Lost Art Press",           "url": "https://blog.lostartpress.com/feed/"},
    {"name": "Renaissance Woodworker",   "url": "https://renaissancewoodworker.com/feed/"},

    # ── DIY furniture / cabinet blogs (project photos, before/after, plans) ─────
    {"name": "Shanty 2 Chic",            "url": "https://www.shanty-2-chic.com/feed/",                "scrape_gallery": True},
    {"name": "Build-Basic",              "url": "https://build-basic.com/feed/",                      "scrape_gallery": True},
    {"name": "Plaster & Disaster",       "url": "https://plasteranddisaster.com/feed/"},
    {"name": "DIY Candy",                "url": "https://diycandy.com/feed/"},
    {"name": "The Lettered Cottage",     "url": "https://theletteredcottage.net/feed/"},
    {"name": "Unhappy Hipsters",         "url": "https://unhappyhipsters.com/feed/",                  "scrape_gallery": True},

    # ── Home / lifestyle (interior/furniture project photos) ─────────────────────
    {"name": "Bob Vila",                 "url": "https://www.bobvila.com/feed/"},
    {"name": "Family Handyman",          "url": "https://www.familyhandyman.com/feed/"},
    {"name": "PopSugar Home",            "url": "https://www.popsugar.com/home/rss"},

    # ── Furniture-design publications ───────────────────────────────────────────
    # ★ Yanko Design furniture category — 8/8 furniture relevance, 1280x960 photos.
    {"name": "Yanko Design furniture",   "url": "https://www.yankodesign.com/category/furniture/feed/", "scrape_gallery": True},
    {"name": "Azure magazine CA",        "url": "https://www.azuremagazine.com/feed"},
    {"name": "Stylepark DE",             "url": "https://www.stylepark.com/feed"},

    # ── Design Boom tag feeds (work when the main /feed/ 403s) ───────────────────
    # The main Design Boom feed + /design/ + /category/interiors/ all return HTTP
    # 403 (bot-blocked). The architecture tag feed still returns 200 with 500x400
    # photos and produces furniture/interior-relevant items (e.g. hanok interiors,
    # preschool reconfigurations). Art + technology tags were tested but produced
    # 0 furniture-relevant items after the classifier, so they were not added.
    {"name": "Design Boom architecture", "url": "https://www.designboom.com/architecture/feed/"},

    # ═══════════════════════════════════════════════════════════════════════════
    # EXPANDED SOURCES (2026-06 batch 3) — added after quality audit found that
    # many general home magazines (Homes & Gardens, Ideal Home, Country Living,
    # House Beautiful, The Kitchn, Architectural Digest) produce 50-100%
    # affiliate/shopping content. These category feeds are more focused on
    # furniture DESIGN with real project photography.
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Trendir furniture category — 8/8 furniture relevance! Cabinet/wardrobe/
    # nightstand/console/dresser design ideas with photos. Direct cabinet-furniture
    # focus — exactly what @abakan_mebel needs.
    {"name": "Trendir furniture",         "url": "https://www.trendir.com/category/furniture/feed/",  "scrape_gallery": True},

    # ── Popular Woodworking projects category — cabinet/furniture build projects
    # with step-by-step photos ("6-Drawer Storage Cabinet", "Flip-Top Tool Cart").
    # More focused than the main Popular Woodworking feed (which had 40% affiliate).
    {"name": "Popular Woodworking projects", "url": "https://www.popularwoodworking.com/category/projects/feed/"},
]

# ─────────────────────────────────────────────────────────────────────────────
# Relevance classifier
#
# An item is "furniture-relevant" if its title/summary contains any of these
# strong keywords (whole-word match, case-insensitive). The set was hand-tuned
# to capture: cabinet furniture (корпусная мебель), kitchens (кухни),
# cabinets (шкафы), wardrobes (гардеробы), furniture design (дизайн мебели),
# materials (MDF/wood/laminate), fittings/hardware.
# ─────────────────────────────────────────────────────────────────────────────
STRONG_KEYWORDS_EN: list[str] = [
    "kitchen", "kitchens", "kitchenette",
    "cabinet", "cabinets", "cabinetry", "cabinetmaking", "cabinet-making",
    "wardrobe", "wardrobes", "closet", "closets",
    "cupboard", "cupboards", "larder", "pantry",
    "millwork", "case furniture", "casegoods", "case goods",
    "furniture", "furnishings",
    "bookcase", "bookshelf", "shelving", "shelves",
    "dresser", "sideboard", "credenza", "hutch", "buffet",
    "sofa", "couch", "loveseat", "sectional",
    "dining table", "coffee table", "console table",
    "chair", "chairs", "armchair", "stool", "ottoman",
    "vanity", "bathroom vanity",
    "built-in", "builtin", "fitted furniture", "modular furniture",
    "MDF", "plywood", "veneer", "laminate", "particleboard", "melamine",
    "hardwood", "softwood", "timber", "lumber",
    "hinge", "hinges", "drawer slide", "drawer slides", "soft-close",
    "countertop", "countertops", "worktop", "worktops",
    "backsplash",
    "island", "kitchen island",
    "cabinetmaker", "cabinet maker", "woodworker", "woodworking",
    "cabinet hardware", "cabinet knob", "cabinet pull",
    "interior design", "interiors",
    # ── 2026-06 expansion — coverage for abakanmebel.online categories ──
    # Bathroom furniture («Мебель для ванной»)
    "bathroom", "bathrooms",  # site sells bathroom furniture; Dezeen/DM tag feeds
    "bathroom cabinet", "bathroom cabinets", "bathroom furniture",
    "bath vanity", "washstand", "washbasin cabinet",
    "medicine cabinet", "mirror cabinet",
    # Bedroom / living-room sets («Гарнитуры»)
    "bedroom", "bedrooms", "bed frame", "headboard", "nightstand", "nightstands",
    "bedside table", "wardrobe closet",
    "living room", "media console", "tv unit", "entertainment center",
    # Office furniture («Офисная мебель»)
    "office furniture", "desk", "desks", "workstation", "workstations",
    "filing cabinet", "bookcase desk", "executive desk",
    # Lighting & storage (kitchen/wardrobe/bath fittings & cabinetry)
    "pendant light", "pendant lights", "cabinet lighting", "under-cabinet",
    "storage cabinet", "storage cabinets", "storage system",
    "linen closet", "wardrobe system", "closet system",
    # Walk-in closets («Гардеробные»)
    "walk-in closet", "walk in closet", "dressing room",
    # Sliding-door wardrobes («Шкафы-купе»)
    "sliding wardrobe", "sliding-door wardrobe", "sliding closet",
]

STRONG_KEYWORDS_RU: list[str] = [
    "кухн",          # кухня, кухни, кухонный, кухонь
    "кухо",          # кухни/кухонь/кухонный — "кухн" misses "кухонь" (gen.pl.)
    "корпусн",       # корпусная, корпусного
    "шкаф",          # шкаф, шкафы, шкафчик
    "шкаф-купе", "шкафы-купе",  # «Шкафы-купе» (sliding wardrobes)
    "гардероб",      # гардероб, гардеробная → «Гардеробные»
    "стеллаж",       # стеллаж
    "полк",          # полка, полки
    "мебел",         # мебель, мебельный
    "стол ", "стол,", "стол.", "столы",
    "стул",          # стул, стулья
    "кресло", "кресла",
    "диван",         # диван
    "тумб",          # тумба, тумбы
    "комод",         # комод
    "витрин",        # витрина
    "фасад",         # фасад (кухонный)
    "ЛДСП", "МДФ", "дсп", "шпон",
    "фурнитур",      # фурнитура
    "петл",          # петля
    "направляющие",  # drawer slides
    "столешниц",     # столешница
    "фрезеровк",     # фрезеровка (CNC)
    "распил",
    "дизайнер мебели",
    "дизайн мебели",
    "дизайн интерьер",
    "интерьер",
    "кухонный гарнитур",
    "мебельный щит",
    "встроенн",      # встроенная мебель
    "модульн",       # модульная мебель
    # ── 2026-06 expansion — coverage for abakanmebel.online categories ──
    # «Мебель для ванной» (bathroom furniture)
    "мебель для ванной", "мебель в ванную",
    "тумба под раковин", "тумба с раковин",
    "зеркало с полк",  # mirror cabinet
    # «Гарнитуры» (bedroom / living-room sets)
    "спальн",        # спальня, спальный гарнитур
    "гостин",        # гостиная, мебель для гостиной
    "прихож",        # прихожая (hall furniture)
    "кровать",       # кровать
    "изголовь",      # headboard
    "тумба прикроватн",  # nightstand
    # «Офисная мебель» (office furniture)
    "офисная мебель", "мебель для офиса",
    "рабочее место", "компьютерный стол",
    # «Гардеробные» (walk-in closets)
    "гардеробная комнат", "система хранен",
    # General storage / lighting
    "системы хранения", "хранение вещей",
    "подсветка", "освещение кухн",  # kitchen/wardrobe lighting
]

# Loose patterns — used as secondary signal (need ≥2 distinct matches)
LOOSE_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bkitchen(?:s|ette)?\b", re.I),
    re.compile(r"\bcabinet(?:s|ry|maker|making)?\b", re.I),
    re.compile(r"\b(?:wardrobe|closet|cupboard|larder|pantry)s?\b", re.I),
    re.compile(r"\bfurniture\b", re.I),
    re.compile(r"\b(?:sofa|couch|loveseat|sectional|armchair|ottoman)\b", re.I),
    re.compile(r"\b(?:bookcase|bookshelf|shelving|sideboard|credenza|hutch|buffet)\b", re.I),
    re.compile(r"\b(?:dining|coffee|console)\s+table\b", re.I),
    re.compile(r"\b(?:MDF|plywood|veneer|laminate|particleboard|melamine|hardwood|softwood|timber|lumber)\b", re.I),
    re.compile(r"\b(?:millwork|casegoods|case\s+goods|case\s+furniture)\b", re.I),
    re.compile(r"\b(?:countertop|worktop|backsplash)\b", re.I),
    re.compile(r"\b(?:interior\s+design|interiors)\b", re.I),
    re.compile(r"\b(?:built-?in|fitted|modular)\s+furniture\b", re.I),
    re.compile(r"\b(?:woodworking|woodworker|cabinetmaker)\b", re.I),
    re.compile(r"\b(?:hinge|drawer\s+slide|soft-close|cabinet\s+hardware)\b", re.I),
    # ── 2026-06 expansion — bathroom / bedroom / office / lighting / storage ──
    re.compile(r"\b(?:bathroom|bath)\s+(?:vanity|cabinet|furniture|cabinets)\b", re.I),
    re.compile(r"\b(?:medicine|mirror)\s+cabinet\b", re.I),
    re.compile(r"\b(?:bedroom|bedrooms|nightstand|headboard|bed\s+frame)\b", re.I),
    re.compile(r"\b(?:living\s+room|media\s+console|entertainment\s+center)\b", re.I),
    re.compile(r"\b(?:office\s+furniture|workstation|filing\s+cabinet|executive\s+desk)\b", re.I),
    re.compile(r"\b(?:walk-?in\s+closet|dressing\s+room|wardrobe\s+system|closet\s+system)\b", re.I),
    re.compile(r"\b(?:pendant\s+light|cabinet\s+lighting|under-?cabinet)\b", re.I),
    re.compile(r"\b(?:storage\s+cabinet|linen\s+closet|storage\s+system)\b", re.I),
    # Russian
    re.compile(r"кухн", re.I),
    re.compile(r"корпусн", re.I),
    re.compile(r"шкаф", re.I),
    re.compile(r"мебел", re.I),
    re.compile(r"гардероб", re.I),
    re.compile(r"столешниц", re.I),
    re.compile(r"фасад", re.I),
    re.compile(r"фурнитур", re.I),
    re.compile(r"дизайн\s+интерьер", re.I),
    # ── Russian expansion ──
    re.compile(r"шкаф-?купе", re.I),
    re.compile(r"спальн|гостин|прихож", re.I),
    re.compile(r"офисная\s+мебель|мебель\s+для\s+офиса|компьютерный\s+стол", re.I),
    re.compile(r"мебель\s+для\s+ванной|тумба\s+(?:под\s+)?раковин", re.I),
    re.compile(r"систем[ыа]\s+хранен|гардеробная\s+комнат", re.I),
]


def is_furniture_relevant(title: str, summary: str) -> bool:
    """Return True if item is relevant to cabinet furniture / kitchen design.

    Strong match (any single keyword) → True.
    Otherwise, ≥2 distinct loose-pattern matches → True.
    """
    text = f"{title} {summary}"
    text_lower = text.lower()

    # Strong English keywords (whole-word, case-insensitive)
    for kw in STRONG_KEYWORDS_EN:
        if re.search(r"\b" + re.escape(kw) + r"\b", text, re.IGNORECASE):
            return True

    # Strong Russian keywords (prefix match, case-insensitive — handles
    # morphological endings like кухня/кухни/кухонь/кухонный)
    for kw in STRONG_KEYWORDS_RU:
        if kw.lower() in text_lower:
            return True

    # Loose patterns — need ≥2 distinct matches
    distinct: set[str] = set()
    for pat in LOOSE_PATTERNS:
        for m in pat.finditer(text):
            distinct.add(pat.pattern.lower())
    return len(distinct) >= 2


# Blocklist — non-furniture or noise we never want
BLOCKLIST: list[str] = [
    # Adult / spam
    "porn", "casino", "viagra", "cialis", "escort", "xxx",
    # Off-topic vehicles (when they slip through broad feeds)
    "Formula 1", "race car", "racecars",
    # Off-topic politics-only
    "election 2026", "senate race",

    # ── Forestry / lumber-industry / wooden-furniture-INDUSTRY noise ────────
    # The @abakan_mebel channel wants furniture & kitchen DESIGN, not forestry
    # or wood-extraction industry news. These phrases are blocked even if the
    # article also mentions furniture-material keywords (lumber/timber/hardwood)
    # which would otherwise pass the relevance classifier.
    # English — lumber/timber industry, logging, sawmills, forest management:
    "lumber harvest", "lumber company", "lumber mill", "lumber wrap",
    "timber harvest", "timber sale", "timber industry", "timber company",
    "logging company", "logging operation", "logging road",
    "sawmill", "saw mill", "forest service", "forest management",
    "national forest", "forest products", "forestry industry",
    "wood pellet", "pulp mill", "pulpwood", "biomass plant",
    "fire mitigation", "wildfire", "forest fire", "tree planting program",
    "USDA puts", "Weyerhaeuser", "Hampton Lumber", "PotlatchDeltic",
    "lumber prices rise", "lumber prices fall", "lumber futures",
    # English — wooden-furniture manufacturing trade (factory/CNC/finishing-line
    # industry press, NOT design): keep design articles, drop factory-news.
    "furniture factory fire", "factory closure", "layoff", "union vote",
    "AWI Chicago", "IWF Atlanta", "AWFS",  # trade-association event chatter
    # ── 2026-06 expansion — broader forestry / wood-industry noise ──
    # Sawmill / timber-harvest machinery & industry press:
    "sawmill industry", "sawmill capacity", "sawmill expansion",
    "timber frame", "timber frame home", "timber harvest permit",
    "timber supply", "timber quota", "timber export", "timber import",
    "lumber yard", "lumber distributor", "lumber tariff", "lumber duty",
    "wood pellet plant", "biomass fuel", "biomass energy",
    "pulp and paper", "paper mill", "cardboard mill",
    "logging equipment", "harvester machine", "forwarder machine",
    "reforestation program", "carbon credits forest",
    # Wooden-furniture manufacturing industry press (factory/supply chain, not design):
    "furniture manufacturer earnings", "furniture industry shipments",
    "furniture market report", "furniture trade deficit",
    "case goods imports", "upholstery fabric supplier",
    "woodworking machinery", "edge bander", "panel saw", "CNC router",  # machinery not design
    # Russian — лесная промышленность / деревообработка как отрасль:
    "лесная промышленность", "лесной отрасли", "лесной промышленности",
    "лесозаготов", "лесопил", "лесхоз", "вырубка лес", "сплав леса",
    "древесные пеллеты", "целлюлозный комбинат", "бумажная фабрика",
    "лесной пожар", "тушение лес",
    # ── Russian expansion — лесопромышленность / деревообработка (отрасль) ──
    "лесопромышлен", "деревообрабатывающая промышленность",
    "деревообрабатывающая отрасль", "лесопереработ",
    "лесной экспорт", "лесной квот", "лесной аукцион",
    "пиломатериал", "пилорама", "лесопилка",
    "круглый лес", "деловая древесина", "баланс березовый",
    "древесная пеллета", "древесная мука", "древесная щепа",
    "ЦБК",  # целлюлозно-бумажный комбинат
    "древесно-стружечная плита производство",  # ДСП production (industry)
    "мебельная фабрика производство объём",  # furniture factory output stats
    "лесной сертификат", "FSC сертификат",  # forest certification
    # ── Recipe / food noise (slips in from The Kitchn and similar food-heavy feeds) ──
    # The Kitchn is kitchen-focused but ~half recipes; drop pure-recipe titles so
    # only kitchen DESIGN / cabinet / renovation content remains.
    "recipe", "recipes", "ingredient", "ingredients",
    "baking", "bake", "baked", "roast", "roasted", "grill", "grilled",
    "salad dressing", "soup recipe", "pasta recipe", "chicken recipe",
    "dessert", "cocktail", "smoothie", "marinade", "sauce recipe",
    "calories", "nutrition", "dietitian",

    # ── Affiliate / shopping-deal noise (2026-06 quality overhaul) ──────────
    # The @abakan_mebel channel wants furniture & kitchen DESIGN, not affiliate
    # marketing posts (Amazon picks, Prime Day deals, "% off" shopping lists).
    # These slipped through from Homes & Gardens, House Beautiful, The Kitchn,
    # Architectural Digest, Elle Decor, Ideal Home, Country Living, Bob Vila.
    # Even when the source is editorial-quality, individual deal posts are NOT
    # design content — block them so the JSON only carries real articles.
    "prime day", "prime deal", "prime day deal",
    "black friday", "cyber monday", "way day",
    "% off", "percent off", "discount code", "promo code", "coupon code",
    "amazon prime", "amazon deal", "amazon sale", "amazon pick",
    "best price", "lowest price", "price drop", "price cut",
    "clearance sale", "flash sale", "limited-time deal", "limited time offer",
    "free shipping", "buy now", "shop now", "add to cart",
    "was $", "now $", "under $", "save up to", "save $",
    "best prime day", "prime day furniture", "prime day kitchen",
    "prime day deals under", "deals under $",
    # celebrity-gossip homes (not design-focused)
    "haaland", "sienna miller", "stallone", "kardashian", "jenner",
    "jennifer garner", "martha stewart says", "jennifer lopez",
    "kate middleton", "princess", "royal family",
    # tool-deal posts (tool brands pushing Prime Day on tools, not furniture)
    "dewalt deal", "makita deal", "milwaukee deal", "dremel deal",
    "greenworks deal", "lawn mower deal", "trimmer deal",
]


# ─────────────────────────────────────────────────────────────────────────────
# Garbage image detection
# ─────────────────────────────────────────────────────────────────────────────
GARBAGE_URL_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in [
        # Logos, icons, favicons, sprites
        r"/logo[s]?\b",
        r"/icons?/",
        r"/favicon",
        r"/sprite[s]?\b",
        r"\blogo[s]?\b",
        r"\bfavicon\b",
        r"-logo[-_]?",
        r"_logo\b",

        # Trackers & ad pixels
        r"/pixel[s]?\b",
        r"/tracker",
        r"/beacon",
        r"doubleclick",
        r"google-analytics",
        r"facebook\.com/tr",
        r"googletagmanager",
        r"scorecardresearch",
        r"/ads?/",
        r"\bad[-_]?server",
        r"advertising",
        r"/sponsored/",
        r"/sponsor/",

        # Avatars, profile pics, author bylines
        r"/avatar",
        r"/authors?/",
        r"/profile[-_]?pic",
        r"/byline",
        r"gravatar",
        r"wp-content/uploads/.*\bavatar\b",
        r"-avatar\b",
        r"-author\b",

        # Placeholders, blanks, transparent spacers
        r"/blank\.",
        r"placeholder",
        r"\btransparent\b",
        r"\b16x9-tr\b",
        r"\bdefault[-_]?image\b",
        r"\bno[-_]?image\b",
        r"\bmissing[-_]?image\b",

        # 1x1 / tiny dimension hints
        r"\b1x1\b",
        r"width[=:]1\b",
        r"height[=:]1\b",

        # Social media buttons & share icons
        r"/social/",
        r"/share[-_]?icon",
        r"twitter\.com/",
        r"instagram\.com/",
        r"youtube\.com/",
        r"tiktok\.com/",
        r"facebook\.com/",
        r"linkedin\.com/",
        r"pinterest\.com/",
        r"reddit\.com/",
        r"whatsapp\.com/",
        r"telegram\.org/",
        r"/newsletter/",
        r"/subscribe/",
        r"/sign[-_]?up/",
        r"/comment[s]?/",

        # Theme & site chrome
        r"/wp-content/themes/",
        r"/wp-content/plugins/",
        r"/wp-includes/",
        r"/wp-content/themes/[^/]+/images/",
        r"/themes?/[^/]+/images/",
        r"/templates?/[^/]+/images/",
        r"/assets/images/",
        r"/assets/img/",
        r"/assets/dist/",
        r"/static/images/",
        r"/static/dist/",
        r"/dist/images/",
        r"/img/icons?/",
        r"/img/social/",
        r"/img/logo",

        # Emoji
        r"emoji",
        r"/emoticons?/",

        # Shopping / affiliate
        r"amazon\.com/",
        r"shopify",
        r"/shop/",
        r"/store/",

        # GIFs (almost never content photos in feeds)
        r"\.gif($|\?)",
    ]
]


def is_garbage_image(url: str) -> bool:
    """Return True if the URL looks like a non-content image
    (logo/icon/tracker/avatar/placeholder/social/theme/etc.).
    """
    if not url:
        return True
    if url.startswith("data:"):
        return True
    # Tiny dimension hints in query (?w=1&h=1, ?resize=1x1, etc.)
    q = parse_qs(urlparse(url).query)
    for k in ("w", "width", "h", "height"):
        if k in q and q[k]:
            try:
                if int(q[k][0]) <= 32:
                    return True
            except ValueError:
                pass
    # Small WordPress size suffix like "-90x90.jpg", "-32x32.png"
    if re.search(r"-(\d{1,2})x(\d{1,2})\.(?:jpg|jpeg|png|webp|gif)(?:\?|$)", url, re.I):
        return True
    # WordPress author/profile pics
    if re.search(r"wp-content/uploads/.*(?:avatar|profile|author)", url, re.I):
        return True
    for pat in GARBAGE_URL_PATTERNS:
        if pat.search(url):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────
def fetch_url(url: str, want_html: bool = False) -> tuple[int | None, bytes | None, str | None]:
    headers = HTML_HEADERS if want_html else HTTP_HEADERS
    try:
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT if not want_html else HTML_TIMEOUT)
        return r.status_code, r.content, None
    except Exception as e:
        return None, None, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Image URL upgrade — resolve relative URLs and upgrade thumbnails to full-size
#
# Many RSS feeds embed a small thumbnail as the lead image (e.g. Dezeen's
# 411x411 square crop, ArchDaily's 528x352 medium_jpg, Archi.ru's 150x112
# preview). The full-size version is usually available by:
#   1. Stripping the WordPress "-WxH" size suffix (Dezeen: sq-411x411.jpg → sq.jpg
#      gives 2364x2364; works for any WP media library image).
#   2. Upgrading ArchDaily path segment "medium_jpg" → "large_jpg"
#      (528x352 → 2000x1333).
#   3. Upgrading Archi.ru path "/i/150/" → "/i/1262/" (150x112 → 1200x900).
# Also handles protocol-relative URLs ("//host/path" → "https://host/path")
# used by Novate.ru, and relative URLs ("/path" → resolved against feed base)
# used by Мебель-expo.
# ─────────────────────────────────────────────────────────────────────────────
def upgrade_image_url(url: str, base_url: str = "") -> str:
    """Resolve relative URLs and upgrade known thumbnail patterns to full-size."""
    if not url:
        return url
    # Protocol-relative URL (//host/path) — used by Novate.ru
    if url.startswith("//"):
        url = "https:" + url
    # Relative URL (/path or path) — resolve against feed base URL
    if base_url and not url.startswith(("http://", "https://")):
        url = urljoin(base_url, url)
    if not url.startswith(("http://", "https://")):
        return url

    # ── Archi.ru: /i/<size>/ → /i/1262/ (thumbnail → full-size) ──
    # i.archi.ru serves multiple sizes under /i/<size>/ path: /i/150/, /i/150_150/,
    # /i/752_792/, /i/752_501/, etc. 1262 is the largest standard size.
    if "i.archi.ru/i/" in url:
        url = re.sub(r"/i/\d+[_\d]*/", "/i/1262/", url)

    # ── ArchDaily: any size variant → large_jpg in path ──
    # images.adsttc.com/media/images/<hash>/<size>/<file>.jpg
    # Sizes: thumb_jpg (~125px), medium_jpg (~528px), large_jpg (~2000px).
    # Gallery scraping picks up thumb_jpg and medium_jpg; upgrade both to large_jpg.
    if "adsttc.com/media/images/" in url:
        url = re.sub(r"/(?:thumb|medium|small|tiny)_jpg/", "/large_jpg/", url)

    # ── Hearst CDN (hips.hearstapps.com): strip crop/resize query params ──
    # Hearst serves the SAME image at any crop via ?crop=...&resize=... query.
    # Stripping these gives the full-size original (e.g. 4350x2870 instead of 360x360).
    # Affected: Veranda, Elle Decor, House Beautiful, Good Housekeeping, Country Living.
    if "hearstapps.com" in url:
        p = urlparse(url)
        q = parse_qs(p.query)
        # Remove sizing params; keep only non-sizing params (if any)
        sizing_keys = {"crop", "resize", "w", "h", "width", "height"}
        kept = {k: v for k, v in q.items() if k.lower() not in sizing_keys}
        new_query = "&".join(f"{k}={v[0]}" for k, v in kept.items())
        url = f"{p.scheme}://{p.netloc}{p.path}" + (f"?{new_query}" if new_query else "")

    # ── WordPress size suffix strip: -WxH.jpg → .jpg ──
    # Dezeen sq-411x411.jpg → sq.jpg (411→2364); works for any WP image.
    # But ONLY strip if the suffix looks like a real WP crop marker
    # (2-4 digit W and H), to avoid mangling other URLs.
    m = re.search(r"-\d{2,4}x\d{2,4}(?=\.\w+$)", url)
    if m:
        url = url[:m.start()] + url[m.end():]

    # ── Future CDN (cdn.mos.cms.futurecdn.net / images.fie.futurecdn.net) ──────
    # Future Publishing CDN serves the same image at any size via a
    # "-<width>-<quality>" suffix before the extension:
    #   foo-1280-80.jpg (1280x720) → foo.jpg (1600x900 full-size)
    #   foo-450-80.jpg.webp → foo.jpg.webp
    # Affected: Homes & Gardens, Ideal Home, Livingetc, Wallpaper, Real Homes.
    if "futurecdn.net" in url:
        url = re.sub(r"-\d+-\d+(?=\.\w+(?:\.\w+)?$)", "", url)

    return url


def extract_image(entry: Any, base_url: str = "") -> str | None:
    """Try every standard RSS image location. Returns None if no image found.

    The extracted URL is passed through upgrade_image_url() to resolve relative
    URLs and upgrade known thumbnail patterns to full-size versions.
    """
    candidates: list[str] = []

    for enc in getattr(entry, "enclosures", []) or []:
        href = enc.get("href", "")
        if href:
            t = enc.get("type", "").lower()
            if t.startswith("image") or any(href.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
                candidates.append(href)
    for m in getattr(entry, "media_content", []) or []:
        url = m.get("url", "")
        if url:
            candidates.append(url)
    for m in getattr(entry, "media_thumbnail", []) or []:
        url = m.get("url", "")
        if url:
            candidates.append(url)
    for ln in getattr(entry, "links", []) or []:
        if ln.get("rel") == "enclosure" and ln.get("type", "").lower().startswith("image"):
            href = ln.get("href", "")
            if href:
                candidates.append(href)
    for field in ("summary", "description", "content"):
        val = getattr(entry, field, None)
        if not val:
            continue
        if isinstance(val, list) and val:
            val = val[0].get("value", "")
        for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', str(val)):
            candidates.append(m.group(1))

    # Return the first NON-garbage candidate; fall back to first candidate
    # only if all are garbage (caller decides whether to drop the item).
    chosen: str | None = None
    for c in candidates:
        if not is_garbage_image(c):
            chosen = c
            break
    if chosen is None and candidates:
        chosen = candidates[0]
    if chosen is None:
        return None
    # Upgrade: resolve relative URLs + upgrade thumbnails to full-size
    return upgrade_image_url(chosen, base_url)


def strip_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_date(entry: Any) -> str:
    for field in ("published_parsed", "updated_parsed", "created_parsed"):
        t = getattr(entry, field, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                continue
    for field in ("published", "updated", "date"):
        val = getattr(entry, field, "")
        if val:
            try:
                t = feedparser._parse_date(val)
                if t:
                    dt = datetime(*t[:6], tzinfo=timezone.utc)
                    return dt.isoformat()
            except Exception:
                continue
    return ""


def item_id(url: str, title: str) -> str:
    raw = (url or "") + "|" + (title or "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# Article-page gallery scraping
# ─────────────────────────────────────────────────────────────────────────────
class _ImgCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "img":
            return
        d = {k.lower(): (v or "") for k, v in attrs}
        for key in ("src", "data-src", "data-lazy-src", "data-original", "data-cfsrc"):
            v = d.get(key)
            if v:
                self.urls.append(v)
        srcset = d.get("srcset") or d.get("data-srcset")
        if srcset:
            parts = [p.strip().split(" ")[0] for p in srcset.split(",") if p.strip()]
            self.urls.extend(parts)


def _base_image_url(url: str) -> str:
    p = urlparse(url)
    path = re.sub(r"-\d+x\d+(?=\.\w+$)", "", p.path)
    return f"{p.scheme}://{p.netloc}{path}"


def _image_size_hint(url: str) -> int:
    q = parse_qs(urlparse(url).query)
    for k in ("resize", "fit", "w", "width"):
        if k in q and q[k]:
            m = re.search(r"(\d+)", q[k][0])
            if m:
                return int(m.group(1))
    m = re.search(r"-(\d+)x(\d+)\.\w+$", urlparse(url).path)
    if m:
        return int(m.group(1)) * int(m.group(2))
    return 9999


def extract_gallery_from_html(html_text: str, base_url: str, lead_image: str | None) -> list[str]:
    parser = _ImgCollector()
    try:
        parser.feed(html_text)
    except Exception:
        return []

    grouped: dict[str, list[str]] = {}
    for u in parser.urls:
        full = urljoin(base_url, u)
        if not re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", full, re.I):
            continue
        if is_garbage_image(full):
            continue
        b = _base_image_url(full)
        grouped.setdefault(b, []).append(full)

    if lead_image:
        lead_b = _base_image_url(lead_image)
        grouped.pop(lead_b, None)

    chosen: list[str] = []
    for variants in grouped.values():
        best = max(variants, key=_image_size_hint)
        chosen.append(best)

    def rank(u: str) -> tuple[int, int]:
        path = urlparse(u).path.lower()
        premium = 1 if any(s in path for s in ("/uploads/", "/mgl/", "/images/", "/media/", "/hmg-prod/")) else 0
        return (premium, _image_size_hint(u))

    chosen.sort(key=rank, reverse=True)
    # Apply the same thumbnail-upgrade logic to gallery images (Dezeen WP suffix,
    # ArchDaily medium_jpg→large_jpg, Archi.ru i/150→i/1262) so multi-photo items
    # also carry full-size images rather than thumbnails.
    chosen = [upgrade_image_url(u, base_url) for u in chosen[: MAX_IMAGES_PER_ITEM - 1]]
    return chosen


def scrape_article_images(url: str, lead_image: str | None) -> list[str]:
    if not url:
        return []
    status, content, err = fetch_url(url, want_html=True)
    if status != 200 or not content:
        return []
    try:
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:
            text = content.decode("latin-1", errors="replace")
        return extract_gallery_from_html(text, url, lead_image)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Fetching
# ─────────────────────────────────────────────────────────────────────────────
def fetch_one(source: dict[str, Any]) -> list[dict[str, Any]]:
    name = source["name"]
    url = source["url"]
    scrape_gallery = bool(source.get("scrape_gallery", False))
    log.info("Fetching %s (%s)%s", name, url, " [gallery]" if scrape_gallery else "")
    status, content, err = fetch_url(url)
    if status != 200 or content is None:
        log.warning("  ✗ %s: HTTP %s (%s)", name, status, err or "")
        return []
    try:
        feed = feedparser.parse(content)
    except Exception as e:
        log.warning("  ✗ %s: parse error %s", name, e)
        return []
    if feed.bozo and not feed.entries:
        log.warning("  ✗ %s: malformed feed (%s)", name, getattr(feed, "bozo_exception", "?"))
        return []

    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    items: list[dict[str, Any]] = []
    for entry in feed.entries[:MAX_ITEMS_PER_FEED]:
        title = strip_html(getattr(entry, "title", ""))
        if not title:
            continue
        full_text = ""
        c = getattr(entry, "content", None)
        if c:
            if isinstance(c, list) and c:
                full_text = c[0].get("value", "")
            elif isinstance(c, str):
                full_text = c
        summary = strip_html(getattr(entry, "summary", "") or full_text)
        if not summary and full_text:
            summary = strip_html(full_text)
        if len(summary) > 600:
            summary = summary[:597].rsplit(" ", 1)[0] + "…"

        link = getattr(entry, "link", "") or ""
        image = extract_image(entry, base_url)
        published = parse_date(entry)

        combined = f"{title} {summary}".lower()
        if any(bl in combined for bl in BLOCKLIST):
            continue

        # ── Minimum text-length guard (2026-06 quality overhaul) ──────────
        # Drops terse product-name-only titles ("Sacha Chair", "Chunk Stool")
        # and clickbait listicles with no real summary excerpt. These pass the
        # relevance classifier but are too sparse for a furniture-design feed.
        if len(title) < MIN_TITLE_LEN:
            continue
        if len(summary) < MIN_SUMMARY_LEN:
            continue

        # ── Garbage-photo guard ────────────────────────────────────────────
        if image and is_garbage_image(image):
            continue

        # ── Quality guard: require a real content photo ───────────────────
        # User requirement: "качественные источники с качественными
        # фотографиями" (quality sources with quality photos). Items without
        # any image are dropped so the JSON only contains photo-rich news.
        if REQUIRE_IMAGE and not image:
            continue

        # ── Relevance filter — only keep cabinet/kitchen/furniture items ──
        if not is_furniture_relevant(title, summary):
            continue

        items.append({
            "id": item_id(link, title),
            "title": title,
            "summary": summary,
            "url": link,
            "image": image or "",
            "images": [image] if image else [],
            "source": name,
            "source_url": base_url,
            "published": published,
        })

    # ── Gallery scraping (multi-photo) ────────────────────────────────────
    if scrape_gallery and items:
        to_scrape = items[:MAX_GALLERY_SCRAPE_PER_SOURCE]
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(scrape_article_images, it["url"], it["image"]): it for it in to_scrape}
            for fut in as_completed(futures):
                it = futures[fut]
                try:
                    extra = fut.result()
                except Exception:
                    extra = []
                if extra:
                    lead = it["image"]
                    gallery: list[str] = []
                    if lead:
                        gallery.append(lead)
                    for u in extra:
                        if u not in gallery:
                            gallery.append(u)
                    it["images"] = gallery[:MAX_IMAGES_PER_ITEM]

    log.info("  ✓ %s: %d items (multi-photo: %d)",
             name, len(items),
             sum(1 for it in items if len(it.get("images", [])) > 1))
    return items


def fetch_all(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_one, s): s["name"] for s in sources}
        for fut in as_completed(futures):
            try:
                all_items.extend(fut.result())
            except Exception as e:
                log.warning("Source %s crashed: %s", futures[fut], e)
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Filtering, dedup, sort
# ─────────────────────────────────────────────────────────────────────────────
def is_recent(published_iso: str, max_age_days: int = MAX_AGE_DAYS) -> bool:
    if not published_iso:
        return True
    try:
        dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
    except Exception:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    return dt >= cutoff


def dedup(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for it in items:
        existing = by_id.get(it["id"])
        if existing is None:
            by_id[it["id"]] = it
            continue
        if len(it.get("images", [])) > len(existing.get("images", [])):
            by_id[it["id"]] = it
        elif len(it.get("images", [])) == len(existing.get("images", [])) and \
             len(it["summary"]) > len(existing["summary"]):
            by_id[it["id"]] = it
    return list(by_id.values())


def sort_newest_first(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(it: dict[str, Any]) -> tuple[int, str]:
        p = it.get("published", "")
        return (0 if p else 1, p or "")
    return sorted(items, key=key, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────
def build_output(items: list[dict[str, Any]]) -> dict[str, Any]:
    sources_used = sorted({it["source"] for it in items})
    multi_photo = sum(1 for it in items if len(it.get("images", [])) > 1)
    return {
        "kind": "furniture",
        "topic": "cabinet furniture, kitchens, furniture design",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_human": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "total_items": len(items),
        "sources_used": sources_used,
        "sources_count": len(sources_used),
        "multi_photo_items": multi_photo,
        "items": items,
    }


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    log.info("Wrote %s (%d items, %d multi-photo, %d bytes)",
             path, data["total_items"], data["multi_photo_items"],
             path.stat().st_size)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    log.info("=" * 70)
    log.info("Cabinet furniture & kitchen design news parser — starting run")
    log.info("Sources: %d total (%d with gallery scraping)",
             len(SOURCES),
             sum(1 for s in SOURCES if s.get("scrape_gallery")))
    log.info("=" * 70)

    repo_root = Path(__file__).resolve().parent
    data_dir = repo_root / "data"

    raw = fetch_all(SOURCES)
    log.info("Total raw items fetched: %d", len(raw))
    if not raw:
        log.error("No items fetched from any source — aborting")
        return 1

    deduped = dedup(raw)
    log.info("After dedup: %d items", len(deduped))

    # Recency filter
    recent = [it for it in deduped if is_recent(it["published"], MAX_AGE_DAYS)]
    log.info("After recency filter (%d days): %d items", MAX_AGE_DAYS, len(recent))

    def image_first_key(it: dict[str, Any]) -> tuple[int, int, str]:
        n_imgs = len(it.get("images", []))
        has_img = 0 if n_imgs > 0 else 1
        return (has_img, -n_imgs, "")

    items_sorted = sort_newest_first(sorted(recent, key=image_first_key))
    items_sorted = items_sorted[:OUTPUT_CAP]

    def clean(it: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": it["id"],
            "title": it["title"],
            "summary": it["summary"],
            "url": it["url"],
            "image": it["image"],
            "images": it.get("images", []),
            "source": it["source"],
            "source_url": it["source_url"],
            "published": it["published"],
        }

    items_clean = [clean(it) for it in items_sorted]

    write_json(data_dir / "furniture-news.json", build_output(items_clean))

    log.info("=" * 70)
    log.info("Run complete. Total: %d items", len(items_clean))
    log.info("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
