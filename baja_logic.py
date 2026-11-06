"""Shared Baja-Bash logic — pure stdlib, no app dependencies.

Single source of truth for the bearing math, apparent-wind/wave math,
sea-state labelling, traffic-light verdict, and the Baja station chain.
Vendored into the aipp-weather-ingest repo so the GitHub Action can
produce the same per-day labels as the Render app without importing
weather_report.py (which pulls in Flask/Open-Meteo/etc.).

If logic changes here, copy this file into aipp-weather-ingest too —
both repos must match for the ERA5 backfill to stay consistent with
live forecasts.
"""
import math

WAVE_SPEED_FACTOR = 3.03


def get_bearing(lat1, lon1, lat2, lon2):
    phi1, phi2   = math.radians(lat1), math.radians(lat2)
    delta_lambda = math.radians(lon2 - lon1)
    y = math.sin(delta_lambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - \
        math.sin(phi1) * math.cos(phi2) * math.cos(delta_lambda)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def get_apparent_data(true_w, true_dir, boat_speed, boat_heading):
    w_rad = math.radians((270 - true_dir)    % 360)
    b_rad = math.radians((90  - boat_heading) % 360)
    rel_x = (true_w * math.cos(w_rad)) - (boat_speed * math.cos(b_rad))
    rel_y = (true_w * math.sin(w_rad)) - (boat_speed * math.sin(b_rad))
    aws = math.sqrt(rel_x ** 2 + rel_y ** 2)
    awd = (270 - math.degrees(math.atan2(rel_y, rel_x))) % 360
    awa = abs((awd - boat_heading + 180) % 360 - 180)
    if   awa <  11:  pos = "HEAD TO WIND"
    elif awa <  30:  pos = "Pinching"
    elif awa <  45:  pos = "Close Hauled"
    elif awa <  60:  pos = "Close Reach"
    elif awa < 110:  pos = "Beam Reach"
    elif awa < 150:  pos = "Broad Reach"
    else:            pos = "Running"
    return aws, awa, pos


def get_apparent_period(wave_p, wave_dir, boat_speed, boat_heading):
    if wave_p <= 0:
        return 0
    wave_speed_kts = WAVE_SPEED_FACTOR * wave_p
    angle_rad      = math.radians(abs((wave_dir - boat_heading + 180) % 360 - 180))
    vel_component  = boat_speed * math.cos(angle_rad)
    return max(0.1, wave_p / (1 + (vel_component / wave_speed_kts)))


# Steepness: wave period (s) / height (ft). User's sailing rule (2026-09-27):
# 10 ft @ 8 s = death, 10 ft @ 10 s and 4 ft @ 4 s = dangerous, 10 ft @ 30 s
# = no big deal. Cuts between those anchors are extrapolated, not calibrated.
STEEP_MIN_FT      = 4.0          # below this, the old square check still applies
STEEP_CUTS        = ((1.1, 3), (1.5, 2), (2.0, 1))   # ratio < cut -> rank
STEEP_HARD_RATIO  = 0.9          # true ratio at/below: DANGEROUS any heading
FOLLOW_EASE_STEPS = 2            # running with seas eases the true tier this far

# Cabo Falso cape-effect boost. Applied only when raw sustained wind is at or
# above CAPE_MIN_RAW_KT -- light-wind roundings (May 8, Nov 4 07:00, Nov 5
# 03:00 captain reports) were easy at the raw model wind. Cutoff is TENTATIVE.
CAPE_FACTOR      = 1.33
CAPE_MIN_RAW_KT  = 12.0


def cape_boost(wind, gust):
    """(wind, gust, applied) with the cape factor applied above the cutoff."""
    if wind is not None and wind >= CAPE_MIN_RAW_KT:
        return wind * CAPE_FACTOR, gust * CAPE_FACTOR, True
    return wind, gust, False
# Wind against swell: wind blowing into the swell stacks chop on it. Cuts are
# a judgment call (2026-09-27, Turtle Bay 7.4 ft S swell under 16G21 NNW).
OPPOSE_MIN_KT  = 15.0
OPPOSE_MIN_DEG = 135.0           # wind-from vs swell-from separation
OPPOSE_MIN_FT  = 4.0
_RANK_LABEL = {0: "GOOD", 1: "FAIR", 2: "POOR", 3: "DANGEROUS"}
_LABEL_RANK = {v: k for k, v in _RANK_LABEL.items()}


def steep_rank(h_ft, period_s):
    """0 none / 1 FAIR / 2 POOR / 3 DANGEROUS from period(s)/height(ft)."""
    if h_ft < STEEP_MIN_FT or period_s <= 0:
        return 0
    r = period_s / h_ft
    if r <= STEEP_CUTS[0][0]:
        return 3
    for cut, rank in STEEP_CUTS[1:]:
        if r < cut:
            return rank
    return 0


def _cap_label(label, rank):
    """Worse of the scored label and a steepness rank."""
    if rank <= 0 or label not in _LABEL_RANK:
        return label
    return _RANK_LABEL[max(_LABEL_RANK[label], rank)]


def get_sea_label(wave_h, true_p, app_p, wave_dir, boat_heading, true_wind, awa, true_gust=0,
                  wind_dir=None):
    flags = []
    score = 10
    downwind = awa >= 110

    if wave_h < 0.15 and true_wind < 4:
        return "EXCELLENT (Glassy)", []

    if true_wind < 8 and true_gust < 15:
        flags.append("CALM")

    if   true_wind > 30: score -= 8; flags.append("🌊 BLOWN OUT")
    elif true_wind >= 20:
        score -= 1 if downwind else 6
        flags.append("⚠️ CHOPPY")
    elif true_wind > 15: score -= 2

    rel_angle        = abs((wave_dir - boat_heading + 180) % 360 - 180)
    waves_from_ahead = rel_angle < 90

    # Seas 4 ft+ are judged by steepness (period/height), not the old fixed
    # 6 s cutoff. Heading enters through the apparent (encounter) period:
    # head seas shorten it, following seas stretch it -- but running can
    # only ease the sea's own true tier by FOLLOW_EASE_STEPS.
    h_ft       = wave_h * 3.28084
    true_rank  = steep_rank(h_ft, true_p)
    steep      = max(steep_rank(h_ft, app_p), true_rank - FOLLOW_EASE_STEPS)
    if h_ft >= STEEP_MIN_FT and true_p > 0 and true_p / h_ft <= STEEP_HARD_RATIO:
        steep = 3
    if true_rank >= 2:
        flags.append("⚠️ SQUARE")
    if steep >= 2 and rel_angle <= 60:
        flags.append("⚠️ POUNDING")
    if true_rank >= 2 and rel_angle >= 120:
        flags.append("⚠️ SURFING")

    square = h_ft < STEEP_MIN_FT and true_p < 6.0 and wave_h > 0.75
    if square:
        flags.append("⚠️ SQUARE")
        if waves_from_ahead:
            sq_pen = 2 if ("⚠️ CHOPPY" in flags and downwind) else 4
            score -= sq_pen

    if wave_h > 0.5 and 70 < rel_angle < 110:
        flags.append("Beam Swell")
        score -= 3

    if wave_h > 1.8 and true_rank > 0: score -= 2   # long gentle swell: no height penalty
    if wave_h > 0.3 and app_p < 4: score -= 2

    if "🌊 BLOWN OUT" in flags or (square and awa < 75 and waves_from_ahead) or steep >= 3:
        return "DANGEROUS", flags

    if   score >= 8: label = "GOOD"
    elif score >= 6: label = "FAIR"
    elif score >= 4: label = "POOR"
    elif awa >= 75:  label = "POOR"
    else:            label = "DANGEROUS"
    label = _cap_label(label, steep)

    if (wind_dir is not None and true_wind >= OPPOSE_MIN_KT and h_ft >= OPPOSE_MIN_FT
            and abs((wind_dir - wave_dir + 180) % 360 - 180) >= OPPOSE_MIN_DEG):
        flags.append("⚠️ WIND vs SWELL")
        if label in _LABEL_RANK:
            label = _RANK_LABEL[min(3, _LABEL_RANK[label] + 1)]
    return label, flags


def _traffic_light(worst):
    aws      = worst['app_w']
    aws_gust = worst['app_w_gust']
    awa      = worst['awa']
    label    = worst['label']
    forward  = awa < 110

    if label in ("DANGEROUS", "POOR"):  return "🔴 WAIT  "
    if aws_gust > 30:                   return "🔴 WAIT  "

    wind_flags = {"🌊 BLOWN OUT", "⚠️ CHOPPY", "⚠️ SQUARE"}
    beam_only  = (
        not any(f in worst['flags'] for f in wind_flags)
        and set(worst['flags']) - {"CALM", "⚠️ CAPE EFFECT"} == {"Beam Swell"}
    )

    if forward:
        if aws > 20:                    return "🔴 WAIT  "
        is_calm = "CALM" in worst['flags']
        if aws >= 16 and not is_calm:   return "🟡 CAUTION"
        gust_floor = 22 if is_calm else 15
        if awa < 30 and aws_gust >= gust_floor and worst['g'] >= 12: return "🟡 CAUTION"
        if (worst['g'] - worst['w']) > 12: return "🟡 CAUTION"
        if label == "FAIR" and not beam_only: return "🟡 CAUTION"
        if is_calm: worst['motoring'] = True
        return "🟢 GO     "
    else:
        if aws >= 30:                   return "🔴 WAIT  "
        if aws >= 25:                   return "🟡 CAUTION"
        if "⚠️ CAPE EFFECT" in worst['flags'] or "⚠️ CHOPPY" in worst['flags']:
            return "🟡 CAUTION"
        if worst['g'] >= 25:            return "🟡 CAUTION"
        if label == "FAIR" and not beam_only: return "🟡 CAUTION"
        return "🟢 GO     "


_BAJA_STATIONS = [
    ("Cabo Falso",  22.75, -110.5,  True),
    ("Punta Tosca", 23.6,  -110.7, False),
    ("Santa Maria",  24.7,  -112.4, False),
    ("Turtle Bay",   27.8,  -115.3, False),
    ("Ensenada",     31.7,  -116.8, False),
]
