# Brand → Ticker mapping
# Keys are lowercase brand name fragments. Values are stock tickers (None = private).
# Used by score_brands.py and gap_detection.py

BRAND_TICKER_MAP: dict[str, str | None] = {

    # ── Apparel & Footwear ────────────────────────────────────────────────────
    "lululemon":        "LULU",
    "lulu":             "LULU",
    "nike":             "NKE",
    "adidas":           "ADDYY",
    "under armour":     "UAA",
    "gap":              "GPS",
    "old navy":         "GPS",
    "banana republic":  "GPS",
    "ralph lauren":     "RL",
    "pvh":              "PVH",
    "calvin klein":     "PVH",
    "tommy hilfiger":   "PVH",
    "hanesbrands":      "HBI",
    "champion":         "HBI",
    "uniqlo":           "FRCOY",
    "fast retailing":   "FRCOY",
    "zara":             "IDEXY",
    "inditex":          "IDEXY",
    "h&m":              "HMRZF",
    "columbia":         "COLM",
    "vf corporation":   "VFC",
    "timberland":       "VFC",
    "dickies":          None,
    "carhartt":         None,
    "patagonia":        None,
    "arc'teryx":        "ADDYY",  # owned by Amer Sports / Adidas ecosystem
    "north face":       "VFC",
    "supreme":          "VFC",
    "new balance":      None,
    "allbirds":         "BIRD",
    "on running":       "ONON",
    "hoka":             "DECK",
    "ugg":              "DECK",
    "deckers":          "DECK",
    "skechers":         "SKX",
    "steve madden":     "SHOO",
    "crocs":            "CROX",
    "birkenstock":      "BIRK",

    # ── Tech & Electronics ───────────────────────────────────────────────────
    "apple":            "AAPL",
    "iphone":           "AAPL",
    "macbook":          "AAPL",
    "airpods":          "AAPL",
    "samsung":          "SSNLF",
    "sony":             "SONY",
    "logitech":         "LOGI",
    "nvidia":           "NVDA",
    "nvda":             "NVDA",
    "amd":              "AMD",
    "intel":            "INTC",
    "microsoft":        "MSFT",
    "surface":          "MSFT",
    "google":           "GOOGL",
    "pixel":            "GOOGL",
    "meta":             "META",
    "oculus":           "META",
    "amazon":           "AMZN",
    "kindle":           "AMZN",
    "ring":             "AMZN",
    "bose":             None,
    "jabra":            "GN.CO",
    "anker":            None,
    "razer":            "1337.HK",
    "corsair":          "CRSR",
    "steelseries":      None,
    "herman miller":    "MLHR",
    "secretlab":        None,
    "dyson":            None,
    "roomba":           "IRBT",
    "irobot":           "IRBT",
    "sonos":            "SONO",
    "claude":           "AMZN",   # Anthropic — Amazon is lead investor
    "anthropic":        "AMZN",
    "chatgpt":          "MSFT",   # OpenAI — Microsoft is lead investor
    "openai":           "MSFT",

    # ── Food & Beverage ──────────────────────────────────────────────────────
    "chipotle":         "CMG",
    "mcdonald":         "MCD",
    "starbucks":        "SBUX",
    "dunkin":           None,
    "burger king":      "QSR",
    "tim hortons":      "QSR",
    "restaurant brands":"QSR",
    "dominos":          "DPZ",
    "domino's":         "DPZ",
    "papa john":        "PZZA",
    "yum brands":       "YUM",
    "taco bell":        "YUM",
    "kfc":              "YUM",
    "pizza hut":        "YUM",
    "darden":           "DRI",
    "olive garden":     "DRI",
    "shake shack":      "SHAK",
    "dutch bros":       "BROS",
    "monster":          "MNST",
    "celsius":          "CELH",
    "red bull":         None,
    "coca cola":        "KO",
    "coke":             "KO",
    "pepsi":            "PEP",
    "doritos":          "PEP",
    "lays":             "PEP",
    "gatorade":         "PEP",
    "frito":            "PEP",
    "keurig":           "KDP",
    "dr pepper":        "KDP",
    "kraft":            "KHC",
    "heinz":            "KHC",
    "general mills":    "GIS",
    "cheerios":         "GIS",
    "kellogg":          "KLG",
    "campbell":         "CPB",

    # ── Beauty & Skincare ────────────────────────────────────────────────────
    "neutrogena":       "JNJ",
    "aveeno":           "JNJ",
    "cerave":           "LRLCY",   # L'Oréal owns CeraVe
    "la roche posay":   "LRLCY",
    "loreal":           "LRLCY",
    "l'oreal":          "LRLCY",
    "maybelline":       "LRLCY",
    "garnier":          "LRLCY",
    "estee lauder":     "EL",
    "clinique":         "EL",
    "mac cosmetics":    "EL",
    "procter gamble":   "PG",
    "olay":             "PG",
    "old spice":        "PG",
    "gillette":         "PG",
    "pantene":          "PG",
    "head shoulders":   "PG",
    "unilever":         "UL",
    "dove":             "UL",
    "vaseline":         "UL",
    "axe":              "UL",
    "cosrx":            None,
    "the ordinary":     "ELF",   # Deciem — ELF Beauty acquired stake
    "e.l.f":            "ELF",
    "elf beauty":       "ELF",
    "e.l.f beauty":     "ELF",

    # ── Outdoor & Sporting Goods ─────────────────────────────────────────────
    "yeti":             "YETI",
    "hydro flask":      "HBB",
    "helen of troy":    "HELE",
    "oxo":              "HELE",
    "stanley":          None,    # private (PMI)
    "leatherman":       None,
    "gerber":           None,
    "rei":              None,    # co-op
    "bass pro":         None,

    # ── Home & Appliances ────────────────────────────────────────────────────
    "vitamix":          None,
    "kitchenaid":       "WHR",
    "whirlpool":        "WHR",
    "ge appliances":    "GEAPH",
    "lg":               "LGCLF",
    "instant pot":      "CNTH",
    "shark ninja":      "SN",
    "ilife":            None,
    "ikea":             None,

    # ── Finance & Fintech ────────────────────────────────────────────────────
    "ynab":             None,
    "mint":             "INTU",
    "intuit":           "INTU",
    "turbotax":         "INTU",
    "robinhood":        "HOOD",
    "coinbase":         "COIN",
    "paypal":           "PYPL",
    "venmo":            "PYPL",
    "square":           "XYZ",
    "cash app":         "XYZ",
    "block":            "XYZ",
    "affirm":           "AFRM",
    "klarna":           None,

    # ── Telecom & Streaming ──────────────────────────────────────────────────
    "t-mobile":         "TMUS",
    "tmobile":          "TMUS",
    "verizon":          "VZ",
    "visible":          "VZ",
    "at&t":             "T",
    "netflix":          "NFLX",
    "spotify":          "SPOT",
    "disney+":          "DIS",
    "hulu":             "DIS",
    "hbo":              "WBD",
    "max":              "WBD",
    "peacock":          "CMCSA",
    "paramount":        "PARA",
    "duolingo":         "DUOL",

    # ── Fitness & Health ─────────────────────────────────────────────────────
    "peloton":          "PTON",
    "lululemon mirror": "LULU",
    "whoop":            None,
    "oura":             None,
    "fitbit":           "GOOGL",  # Google acquired Fitbit

    # ── Retail & E-commerce ──────────────────────────────────────────────────
    "costco":           "COST",
    "walmart":          "WMT",
    "target":           "TGT",
    "dollar tree":      "DLTR",
    "dollar general":   "DG",
    "tjmaxx":           "TJX",
    "marshalls":        "TJX",
    "home depot":       "HD",
    "lowes":            "LOW",
    "wayfair":          "W",
    "etsy":             "ETSY",
    "shopify":          "SHOP",
}


def normalize(brand: str) -> str:
    return brand.lower().strip()


def get_ticker(brand: str) -> str | None:
    if not brand:
        return None
    b = normalize(brand)
    # exact match first
    if b in BRAND_TICKER_MAP:
        return BRAND_TICKER_MAP[b]
    # substring match
    for key, ticker in BRAND_TICKER_MAP.items():
        if key in b:
            return ticker
    return None
