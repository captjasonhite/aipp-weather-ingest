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


def get_sea_label(wave_h, true_p, app_p, wave_dir, boat_heading, true_wind, awa, true_gust=0):
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

    square = true_p < 6.0 and wave_h > 0.75
    if square:
        flags.append("⚠️ SQUARE")
        if waves_from_ahead:
            sq_pen = 2 if ("⚠️ CHOPPY" in flags and downwind) else 4
            score -= sq_pen

    if wave_h > 0.5 and 70 < rel_angle < 110:
        flags.append("Beam Swell")
        score -= 3

    if wave_h > 1.8: score -= 2
    if wave_h > 0.3 and app_p < 4: score -= 2

    if "🌊 BLOWN OUT" in flags or (square and awa < 75 and waves_from_ahead):
        return "DANGEROUS", flags

    if score >= 8: return "GOOD",      flags
    if score >= 6: return "FAIR",      flags
    if score >= 4: return "POOR",      flags
    if awa >= 75:  return "POOR",      flags
    return "DANGEROUS", flags


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
        and set(worst['flags']) - {"CALM"} == {"Beam Swell"}
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
    ("Santa Maria",  24.7,  -112.4, False),
    ("Turtle Bay",   27.8,  -115.3, False),
    ("Ensenada",     31.7,  -116.8, False),
]
