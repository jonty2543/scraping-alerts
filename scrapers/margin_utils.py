import re


MARGIN_MARKET_HINTS = (
    "winning margin",
    "win margin",
    "margins 12.5",
    "margin 12.5",
    "margin 1-12",
    "margin 1 - 12",
    "1-12 & 13+",
    "1 - 12 & 13+",
)


def canonical_margin_selection(
    name,
    participant=None,
    lower_limit=None,
    upper_limit=None,
):
    """Return a bookmaker-independent ``Team 1-12`` or ``Team 13+`` label."""
    raw_name = str(name or "").strip()
    if isinstance(participant, dict):
        participant = participant.get("name") or participant.get("englishName") or participant.get("title")
    team = str(participant or "").strip()
    text = re.sub(r"\s+", " ", raw_name).strip()
    text_l = text.lower()

    try:
        lower = int(float(lower_limit)) if lower_limit is not None else None
    except (TypeError, ValueError):
        lower = None
    try:
        upper = int(float(upper_limit)) if upper_limit is not None else None
    except (TypeError, ValueError):
        upper = None

    band = None
    if lower == 1 and upper == 12:
        band = "1-12"
    elif lower == 13 and upper is None:
        band = "13+"
    elif re.search(r"\b1\s*(?:[-–]|to)\s*12\b", text_l):
        band = "1-12"
    elif re.search(r"\b13\s*\+", text_l) or re.search(r"\b13\s+or\s+more\b", text_l):
        band = "13+"

    if band is None:
        return None

    if not team:
        team = re.sub(r"\bto\s+win\s+by\b.*$", "", text, flags=re.IGNORECASE).strip()
        team = re.sub(r"\bwin\s+by\b.*$", "", team, flags=re.IGNORECASE).strip()
        team = re.sub(
            r"\bby\s+(?:1\s*(?:[-–]|to)\s*12|13\s*(?:\+|or\s+more)).*$",
            "",
            team,
            flags=re.IGNORECASE,
        ).strip()
        team = re.sub(
            r"\s+(?:1\s*(?:[-–]|to)\s*12|13\s*(?:\+|or\s+more))(?:\s+points?)?\s*$",
            "",
            team,
            flags=re.IGNORECASE,
        ).strip()

    if not team or team.lower() in {"draw", "tie", "any other result"}:
        return None
    return f"{team} {band}"


def is_target_margin_market(market_name, selections):
    """Identify the four-outcome 1-12/13+ market without accepting other bands."""
    normalized = [selection for selection in selections if selection]
    if len(normalized) != 4:
        return False

    team_bands = {}
    for selection in normalized:
        team, separator, band = selection.rpartition(" ")
        if not separator or band not in {"1-12", "13+"} or not team:
            return False
        team_bands.setdefault(team.lower(), set()).add(band)
    if len(team_bands) != 2 or any(bands != {"1-12", "13+"} for bands in team_bands.values()):
        return False

    market_l = re.sub(r"\s+", " ", str(market_name or "").lower()).strip()
    return any(hint in market_l for hint in MARGIN_MARKET_HINTS) or bool(team_bands)
