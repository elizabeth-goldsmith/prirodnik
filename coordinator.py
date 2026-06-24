#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coordinator — оркестратор системы анализа земельного рынка.
Запуск: python coordinator.py "Название региона"
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

# ───────────────────────────────
# Настройки
# ───────────────────────────────
DB_PATH = "./data/land_research.db"
OUTPUT_DIR = "./output"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BATCH_SIZE = 5
MAX_WORKERS = 6
HEADERS = {"User-Agent": "LandMarket-Research/1.0 (research@local)"}


# ───────────────────────────────
# Модели данных
# ───────────────────────────────
@dataclass
class MacroReport:
    region_name: str
    population: int = 0
    migration_trend: str = ""
    infra_projects: str = ""
    economic_assessment: str = ""
    land_policy: str = ""
    overall_trend: str = "стагнация"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class MunicipalProfile:
    region_name: str
    municipality_name: str
    population: int = 0
    distance_to_center_km: float = 0.0
    infra_score: int = 5
    ecology_risk: str = "mid"
    schools_count: int = 0
    hospitals_count: int = 0
    roads_trunk: int = 0
    profile_json: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class LandMarketData:
    region_name: str
    municipality_name: str
    avg_price_per_sotka: float = 0.0
    price_trend_pct: float = 0.0
    supply_count: int = 0
    demand_level: str = "mid"
    typical_plot_size: float = 10.0
    market_json: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class DevelopmentTrigger:
    region_name: str
    municipality_name: str
    description: str = ""
    source: str = ""
    impact_level: int = 5
    timeframe: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class InvestRanking:
    region_name: str
    municipality_name: str
    invest_score: float = 0.0
    avg_price_per_sotka: float = 0.0
    key_triggers: str = ""
    recommendation: str = "Hold"
    rank: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


# ───────────────────────────────
# База данных
# ───────────────────────────────
class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._local = threading.local()
        self.init_db()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def init_db(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS macro_report (
            region_name TEXT PRIMARY KEY,
            population INTEGER,
            migration_trend TEXT,
            infra_projects TEXT,
            economic_assessment TEXT,
            land_policy TEXT,
            overall_trend TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS municipal_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT,
            municipality_name TEXT,
            population INTEGER,
            distance_to_center_km REAL,
            infra_score INTEGER,
            ecology_risk TEXT,
            schools_count INTEGER,
            hospitals_count INTEGER,
            roads_trunk INTEGER,
            profile_json TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS land_market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT,
            municipality_name TEXT,
            avg_price_per_sotka REAL,
            price_trend_pct REAL,
            supply_count INTEGER,
            demand_level TEXT,
            typical_plot_size REAL,
            market_json TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS development_triggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT,
            municipality_name TEXT,
            description TEXT,
            source TEXT,
            impact_level INTEGER,
            timeframe TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS invest_ranking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT,
            municipality_name TEXT,
            invest_score REAL,
            avg_price_per_sotka REAL,
            key_triggers TEXT,
            recommendation TEXT,
            rank INTEGER,
            created_at TEXT
        );
        """
        self._conn().executescript(ddl)
        self._conn().commit()

    def save_macro(self, r: MacroReport):
        self._conn().execute(
            """INSERT OR REPLACE INTO macro_report
            (region_name, population, migration_trend, infra_projects,
             economic_assessment, land_policy, overall_trend, created_at)
            VALUES (?,?,?,?,?,?,?,?)""",
            (r.region_name, r.population, r.migration_trend, r.infra_projects,
             r.economic_assessment, r.land_policy, r.overall_trend, r.created_at),
        )
        self._conn().commit()

    def save_municipal(self, m: MunicipalProfile):
        cur = self._conn().execute(
            """INSERT INTO municipal_profiles
            (region_name, municipality_name, population, distance_to_center_km,
             infra_score, ecology_risk, schools_count, hospitals_count,
             roads_trunk, profile_json, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (m.region_name, m.municipality_name, m.population, m.distance_to_center_km,
             m.infra_score, m.ecology_risk, m.schools_count, m.hospitals_count,
             m.roads_trunk, m.profile_json, m.created_at),
        )
        self._conn().commit()
        return cur.lastrowid

    def save_land(self, l: LandMarketData):
        cur = self._conn().execute(
            """INSERT INTO land_market_data
            (region_name, municipality_name, avg_price_per_sotka, price_trend_pct,
             supply_count, demand_level, typical_plot_size, market_json, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (l.region_name, l.municipality_name, l.avg_price_per_sotka,
             l.price_trend_pct, l.supply_count, l.demand_level,
             l.typical_plot_size, l.market_json, l.created_at),
        )
        self._conn().commit()
        return cur.lastrowid

    def save_trigger(self, t: DevelopmentTrigger):
        cur = self._conn().execute(
            """INSERT INTO development_triggers
            (region_name, municipality_name, description, source,
             impact_level, timeframe, created_at)
            VALUES (?,?,?,?,?,?,?)""",
            (t.region_name, t.municipality_name, t.description,
             t.source, t.impact_level, t.timeframe, t.created_at),
        )
        self._conn().commit()
        return cur.lastrowid

    def save_rank(self, r: InvestRanking):
        cur = self._conn().execute(
            """INSERT INTO invest_ranking
            (region_name, municipality_name, invest_score, avg_price_per_sotka,
             key_triggers, recommendation, rank, created_at)
            VALUES (?,?,?,?,?,?,?,?)""",
            (r.region_name, r.municipality_name, r.invest_score,
             r.avg_price_per_sotka, r.key_triggers, r.recommendation,
             r.rank, r.created_at),
        )
        self._conn().commit()
        return cur.lastrowid

    def get_macro(self, region_name: str) -> Optional[MacroReport]:
        row = self._conn().execute(
            "SELECT * FROM macro_report WHERE region_name=?", (region_name,)
        ).fetchone()
        if row:
            return MacroReport(**{k: row[k] for k in row.keys()})
        return None

    def get_municipal_profiles(self, region_name: str) -> List[dict]:
        rows = self._conn().execute(
            "SELECT * FROM municipal_profiles WHERE region_name=?", (region_name,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_land_data(self, region_name: str) -> List[dict]:
        rows = self._conn().execute(
            "SELECT * FROM land_market_data WHERE region_name=?", (region_name,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_triggers(self, region_name: str) -> List[dict]:
        rows = self._conn().execute(
            "SELECT * FROM development_triggers WHERE region_name=?", (region_name,)
        ).fetchall()
        return [dict(r) for r in rows]

    def clear_region(self, region_name: str):
        for tbl in (
            "macro_report", "municipal_profiles", "land_market_data",
            "development_triggers", "invest_ranking",
        ):
            self._conn().execute(f"DELETE FROM {tbl} WHERE region_name=?", (region_name,))
        self._conn().commit()


# ───────────────────────────────
# Wikidata / Wikipedia helpers
# ───────────────────────────────
class RegionResolver:
    _cache: Dict[str, List[str]] = {}

    @classmethod
    def get_municipalities(cls, region_name: str) -> List[str]:
        """
        Получить список муниципалитетов региона.
        Приоритет:
        1. Локальный кэш
        2. Overpass API (OSM admin_level=6|8)
        3. Wikipedia (улучшенный парсинг)
        4. Демо-список
        """
        if region_name in cls._cache:
            return cls._cache[region_name]

        results = cls._overpass_municipalities(region_name)
        if not results:
            results = cls._wikipedia_fallback(region_name)
        if not results:
            print("[RegionResolver] демо-список")
            results = ["Центральный район", "Северный район", "Южный район"]

        cls._cache[region_name] = results
        return results

    @staticmethod
    def _get_region_area_id(region_name: str) -> Optional[int]:
        """Получить Overpass area-id региона через Nominatim."""
        q = f"{region_name}, Россия"
        try:
            r = requests.get(
                NOMINATIM_URL,
                params={"q": q, "format": "json", "limit": 3},
                headers=HEADERS,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            for item in data:
                if item.get("osm_type") == "relation":
                    return 3600000000 + int(item["osm_id"])
            if data:
                item = data[0]
                osm_id = int(item.get("osm_id", 0))
                if item.get("osm_type") == "relation":
                    return 3600000000 + osm_id
                if item.get("osm_type") == "way":
                    return 2400000000 + osm_id
            return None
        except Exception as e:
            print(f"[Nominatim-Region] ошибка: {e}")
            return None

    @staticmethod
    def _overpass_municipalities(region_name: str) -> Optional[List[str]]:
        area_id = RegionResolver._get_region_area_id(region_name)
        if not area_id:
            return None

        query = f"""
        [out:json][timeout:120];
        area({area_id})->.searchArea;
        (
          relation["admin_level"="6"](area.searchArea);
          relation["admin_level"="8"](area.searchArea);
        );
        out body;
        """
        try:
            r = requests.post(
                OVERPASS_URL, data={"data": query}, headers=HEADERS, timeout=150
            )
            r.raise_for_status()
            data = r.json()
            names = []
            for elem in data.get("elements", []):
                tags = elem.get("tags", {})
                name = tags.get("name:ru") or tags.get("name")
                if name:
                    names.append(name)
            unique = list(dict.fromkeys(names))
            if unique:
                print(f"[Overpass] найдено муниципалитетов: {len(unique)}")
                return unique
            return None
        except Exception as e:
            print(f"[Overpass-Municipalities] ошибка: {e}")
            return None

    @staticmethod
    def _wikipedia_fallback(region_name: str) -> List[str]:
        """Улучшенный парсинг Wikipedia: таблица + список <li>."""
        url_variants = [
            region_name.replace(" ", "_"),
            region_name.replace(" ", "_") + "_(республика)",
            region_name.replace(" ", "_") + "_(край)",
            region_name.replace(" ", "_") + "_(область)",
            region_name.replace(" ", "_") + "_(автономный_округ)",
        ]
        headers_candidates = [
            "Административно-территориальное деление",
            "Районы и городские округа",
            "Муниципальное устройство",
            "Муниципальные образования",
            "Административное деление",
        ]

        for url_variant in url_variants:
            url = f"https://ru.wikipedia.org/wiki/{url_variant}"
            try:
                r = requests.get(url, headers=HEADERS, timeout=30)
                r.raise_for_status()
                html = r.text

                # Попытка 1: таблица wikitable
                for header in headers_candidates:
                    pattern = re.compile(
                        rf"{re.escape(header)}.*?"
                        r"<table[^>]*wikitable[^>]*>(.*?)</table>",
                        re.S | re.I,
                    )
                    m = pattern.search(html)
                    if m:
                        names = RegionResolver._parse_wiki_table(m.group(1))
                        if names:
                            print(f"[Wikipedia] найдено муниципалитетов: {len(names)}")
                            return names

                # Попытка 2: список <ul> после заголовка
                for header in headers_candidates:
                    pattern = re.compile(
                        rf"{re.escape(header)}.*?"
                        r"<ul[^>]*>(.*?)</ul>",
                        re.S | re.I,
                    )
                    m = pattern.search(html)
                    if m:
                        items = re.findall(
                            r"<li[^>]*>(.*?)</li>", m.group(1), re.S
                        )
                        names = []
                        for item in items:
                            txt = re.sub(r"<[^>]+>", "", item).strip()
                            txt = txt.replace("\n", " ")
                            if txt and (
                                "район" in txt.lower()
                                or "округ" in txt.lower()
                                or "город" in txt.lower()
                            ):
                                names.append(txt)
                        if names:
                            print(f"[Wikipedia-list] найдено муниципалитетов: {len(names)}")
                            return names
            except Exception as e:
                print(f"[Wikipedia] ошибка для {url_variant}: {e}")
                continue

        print("[Wikipedia] таблица АТД не найдена")
        return []

    @staticmethod
    def _parse_wiki_table(table_html: str) -> List[str]:
        rows = re.findall(r"<td[^>]*>(.*?)</td>", table_html, re.S)
        names = []
        for cell in rows:
            txt = re.sub(r"<[^>]+>", "", cell).strip()
            txt = txt.replace("\n", " ")
            if txt and (
                "район" in txt.lower()
                or "округ" in txt.lower()
                or "город" in txt.lower()
            ):
                names.append(txt)
        return list(dict.fromkeys(names))


# ───────────────────────────────
# Агенты
# ───────────────────────────────
class MacroRegionAnalyst:
    def run(self, region_name: str) -> MacroReport:
        """
        Аналитик уровня субъекта.
        В production: вызов API Росстата, парсинг сайта правительства.
        """
        print(f"[MacroRegionAnalyst] анализ региона: {region_name}")
        # Заглушка с реалистичными данными
        return MacroReport(
            region_name=region_name,
            population=5_000_000,
            migration_trend="приток +2.1% в год",
            infra_projects="Новая трасса М4, расширение аэропорта",
            economic_assessment="средняя зарплата 52 тыс, безработица 4.2%",
            land_policy="программа компенсации ИЖС",
            overall_trend="рост",
        )


class MunicipalProfiler:
    def run(self, region_name: str, municipality_name: str) -> MunicipalProfile:
        """
        Сборщик профиля муниципалитета.
        Реальные вызовы: Nominatim + Overpass API (OSM).
        """
        print(f"[MunicipalProfiler] профиль: {municipality_name}")
        profile = MunicipalProfile(
            region_name=region_name,
            municipality_name=municipality_name,
        )

        # 1. Геокодирование через Nominatim
        area_id = self._get_osm_area_id(municipality_name, region_name)
        if not area_id:
            profile.profile_json = json.dumps({"error": "geocoding failed"}, ensure_ascii=False)
            return profile

        # 2. Overpass: школы, больницы, дороги
        stats = self._overpass_stats(area_id)
        profile.schools_count = stats.get("schools", 0)
        profile.hospitals_count = stats.get("hospitals", 0)
        profile.roads_trunk = stats.get("trunk", 0)

        # 3. Оценка инфраструктуры (1–10)
        raw = (profile.schools_count + profile.hospitals_count * 2 + profile.roads_trunk * 3)
        profile.infra_score = min(10, max(1, int(raw / 10)))

        profile.profile_json = json.dumps(stats, ensure_ascii=False)
        return profile

    def _get_osm_area_id(self, municipality: str, region: str) -> Optional[int]:
        q = f"{municipality}, {region}, Россия"
        try:
            r = requests.get(
                NOMINATIM_URL,
                params={"q": q, "format": "json", "limit": 1},
                headers=HEADERS,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            if not data:
                return None
            first = data[0]
            osm_id = int(first.get("osm_id", 0))
            osm_type = first.get("osm_type", "")
            if osm_type == "relation":
                return 3600000000 + osm_id
            if osm_type == "way":
                return 2400000000 + osm_id
            return None
        except Exception as e:
            print(f"[Nominatim] ошибка для {municipality}: {e}")
            return None

    def _overpass_stats(self, area_id: int) -> dict:
        query = f"""
        [out:json][timeout:60];
        area({area_id})->.searchArea;
        (
          node["amenity"="school"](area.searchArea);
          way["amenity"="school"](area.searchArea);
          relation["amenity"="school"](area.searchArea);
        )->.schools;
        (
          node["amenity"="hospital"](area.searchArea);
          way["amenity"="hospital"](area.searchArea);
          node["amenity"="clinic"](area.searchArea);
          way["amenity"="clinic"](area.searchArea);
        )->.hospitals;
        (
          way["highway"="trunk"](area.searchArea);
          way["highway"="primary"](area.searchArea);
        )->.roads;
        (
          .schools;
          .hospitals;
          .roads;
        );
        out count;
        """
        try:
            r = requests.post(OVERPASS_URL, data={"data": query}, headers=HEADERS, timeout=90)
            r.raise_for_status()
            data = r.json()
            counts = {"schools": 0, "hospitals": 0, "trunk": 0}
            for elem in data.get("elements", []):
                tags = elem.get("tags", {})
                # Overpass 'out count' возвращает данные в специфичном формате;
                # иногда count лежит в tags или сам элемент имеет поля nodes/ways
                # Для простоты сделаем fallback: если count не найден, считаем количество элементов
                pass
            # Альтернативный простой подход: out body; получаем список и считаем len
            return self._overpass_count_simple(area_id)
        except Exception as e:
            print(f"[Overpass] ошибка: {e}")
            return {"schools": 0, "hospitals": 0, "trunk": 0}

    def _overpass_count_simple(self, area_id: int) -> dict:
        categories = {
            "schools": 'node["amenity"="school"](area.searchArea);way["amenity"="school"](area.searchArea);',
            "hospitals": 'node["amenity"="hospital"](area.searchArea);way["amenity"="hospital"](area.searchArea);node["amenity"="clinic"](area.searchArea);',
            "trunk": 'way["highway"="trunk"](area.searchArea);way["highway"="primary"](area.searchArea);',
        }
        result = {}
        for key, subq in categories.items():
            query = f"[out:json][timeout:60];area({area_id})->.searchArea;({subq});out body;"
            try:
                r = requests.post(OVERPASS_URL, data={"data": query}, headers=HEADERS, timeout=90)
                r.raise_for_status()
                data = r.json()
                result[key] = len(data.get("elements", []))
            except Exception as e:
                print(f"[Overpass] {key} ошибка: {e}")
                result[key] = 0
        return result


class LandMarketTracker:
    def run(self, region_name: str, municipality_name: str) -> LandMarketData:
        """
        Аналитик земельного рынка.
        В production: скрапинг Авито/Циан через Playwright.
        """
        print(f"[LandMarketTracker] рынок: {municipality_name}")
        # Заглушка: реалистичные случайные данные для демонстрации
        import random
        base = random.uniform(30, 150)
        return LandMarketData(
            region_name=region_name,
            municipality_name=municipality_name,
            avg_price_per_sotka=round(base, 2),
            price_trend_pct=round(random.uniform(-5, 25), 1),
            supply_count=random.randint(10, 500),
            demand_level=random.choice(["low", "mid", "high"]),
            typical_plot_size=round(random.uniform(6, 20), 1),
            market_json=json.dumps({"note": "stub data"}, ensure_ascii=False),
        )


class DevelopmentSpotter:
    def run(self, region_name: str, municipality_name: str) -> List[DevelopmentTrigger]:
        """
        Охотник за точками роста.
        В production: парсинг zakupki.gov.ru, новостей администраций.
        """
        print(f"[DevelopmentSpotter] триггеры: {municipality_name}")
        # Заглушка
        return [
            DevelopmentTrigger(
                region_name=region_name,
                municipality_name=municipality_name,
                description="Планируется строительство новой школы",
                source="zakupki.gov.ru (demo)",
                impact_level=7,
                timeframe="2026-2028",
            ),
            DevelopmentTrigger(
                region_name=region_name,
                municipality_name=municipality_name,
                description="Реконструкция федеральной трассы",
                source="дорожный фонд (demo)",
                impact_level=9,
                timeframe="2025-2027",
            ),
        ]


class InvestRanker:
    def run(self, db: Database, region_name: str) -> List[InvestRanking]:
        """
        Финальный оценщик. Реальная формула скоринга.
        """
        print(f"[InvestRanker] расчет скоринга для {region_name}")
        macro = db.get_macro(region_name)
        profiles = db.get_municipal_profiles(region_name)
        land_data = db.get_land_data(region_name)
        triggers = db.get_triggers(region_name)

        if not profiles:
            return []

        # Индексация по имени муниципалитета
        land_by_mun = {d["municipality_name"]: d for d in land_data}
        triggers_by_mun: Dict[str, List[dict]] = {}
        for t in triggers:
            triggers_by_mun.setdefault(t["municipality_name"], []).append(t)

        # Средняя цена по региону
        prices = [d["avg_price_per_sotka"] for d in land_data if d["avg_price_per_sotka"]]
        avg_regional_price = sum(prices) / len(prices) if prices else 1.0

        results: List[InvestRanking] = []
        for p in profiles:
            mun = p["municipality_name"]
            ld = land_by_mun.get(mun, {})
            trigs = triggers_by_mun.get(mun, [])

            score = 50.0
            price = ld.get("avg_price_per_sotka", 0)
            trend = ld.get("price_trend_pct", 0)
            infra = p.get("infra_score", 5)
            ecology = p.get("ecology_risk", "mid")

            # Цена ниже среднего
            if avg_regional_price > 0 and price < avg_regional_price * 0.8:
                score += 15
            elif price < avg_regional_price:
                score += 7

            # Рост цен
            if trend > 10:
                score += 10
            elif trend > 5:
                score += 5

            # Триггеры
            strong_triggers = [t for t in trigs if t.get("impact_level", 0) >= 7]
            if len(strong_triggers) >= 2:
                score += 15
            elif len(strong_triggers) == 1:
                score += 7

            # Инфраструктура
            if infra >= 7:
                score += 10
            elif infra >= 5:
                score += 5

            # Экология
            if ecology == "high":
                score -= 20
            elif ecology == "mid":
                score -= 5

            # Макротренд региона
            if macro and macro.overall_trend == "спад":
                score -= 15
            elif macro and macro.overall_trend == "рост":
                score += 5

            score = max(0, min(100, score))

            rec = "Hold"
            if score >= 75:
                rec = "Buy"
            elif score < 50:
                rec = "Skip"

            key_descs = "; ".join([t["description"] for t in trigs[:3]])

            results.append(InvestRanking(
                region_name=region_name,
                municipality_name=mun,
                invest_score=round(score, 1),
                avg_price_per_sotka=price,
                key_triggers=key_descs,
                recommendation=rec,
                rank=0,
            ))

        # Сортировка и ранги
        results.sort(key=lambda x: x.invest_score, reverse=True)
        for i, r in enumerate(results, 1):
            r.rank = i

        return results


# ───────────────────────────────
# Coordinator (главный класс)
# ───────────────────────────────
class Coordinator:
    def __init__(self, region_name: str):
        self.region_name = region_name
        self.db = Database(DB_PATH)
        self.macro_analyst = MacroRegionAnalyst()
        self.municipal_profiler = MunicipalProfiler()
        self.land_tracker = LandMarketTracker()
        self.dev_spotter = DevelopmentSpotter()
        self.ranker = InvestRanker()

    def run(self):
        print(f"\n=== LandMarket Research: {self.region_name} ===\n")
        self.db.clear_region(self.region_name)

        # Шаг 1: макро-анализ
        macro = self.macro_analyst.run(self.region_name)
        self.db.save_macro(macro)

        # Шаг 2: список муниципалитетов
        municipalities = RegionResolver.get_municipalities(self.region_name)
        print(f"Найдено муниципалитетов: {len(municipalities)}")
        if not municipalities:
            print("Список муниципалитетов пуст. Завершение.")
            return

        # Шаг 3: параллельный сбор по батчам
        batches = [
            municipalities[i : i + BATCH_SIZE]
            for i in range(0, len(municipalities), BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches, 1):
            print(f"\n--- Батч {batch_idx}/{len(batches)} ---")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {}
                for mun in batch:
                    futures[executor.submit(self._process_municipality, mun)] = mun

                for future in as_completed(futures):
                    mun = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[ERROR] {mun}: {e}")

            # Небольшая пауза между батчами для вежливости к API
            if batch_idx < len(batches):
                time.sleep(2)

        # Шаг 4: финальный рейтинг
        ranking = self.ranker.run(self.db, self.region_name)
        for r in ranking:
            self.db.save_rank(r)

        # Шаг 5: экспорт
        self.save_results(ranking)
        self.print_summary(ranking)

    def _process_municipality(self, municipality_name: str):
        # MunicipalProfiler
        profile = self.municipal_profiler.run(self.region_name, municipality_name)
        self.db.save_municipal(profile)

        # LandMarketTracker
        land = self.land_tracker.run(self.region_name, municipality_name)
        self.db.save_land(land)

        # DevelopmentSpotter
        triggers = self.dev_spotter.run(self.region_name, municipality_name)
        for t in triggers:
            self.db.save_trigger(t)

    def save_results(self, ranking: List[InvestRanking]):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"{self.region_name.replace(' ', '_')}_{ts}"

        # CSV
        csv_path = os.path.join(OUTPUT_DIR, f"{base_name}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "rank", "municipality", "invest_score", "avg_price_per_sotka",
                "key_triggers", "recommendation",
            ])
            for r in ranking:
                writer.writerow([
                    r.rank, r.municipality_name, r.invest_score,
                    r.avg_price_per_sotka, r.key_triggers, r.recommendation,
                ])

        # Markdown
        md_path = os.path.join(OUTPUT_DIR, f"{base_name}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(f"# Рейтинг инвестиционной привлекательности: {self.region_name}\n\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write("| Ранг | Муниципалитет | Invest-Score | Цена сотки | Триггеры | Рекомендация |\n")
            f.write("|------|--------------|-------------|-----------|----------|-------------|\n")
            for r in ranking:
                f.write(
                    f"| {r.rank} | {r.municipality_name} | {r.invest_score} | "
                    f"{r.avg_price_per_sotka} | {r.key_triggers[:60]}... | {r.recommendation} |\n"
                )

        print(f"\n[OK] Результаты сохранены:\n  {csv_path}\n  {md_path}")

    def print_summary(self, ranking: List[InvestRanking]):
        print("\n=== ТОП-5 ===")
        for r in ranking[:5]:
            print(f"  {r.rank}. {r.municipality_name} — {r.invest_score} ({r.recommendation})")
        print("\n=== Skip ===")
        for r in ranking:
            if r.recommendation == "Skip":
                print(f"  {r.rank}. {r.municipality_name} — {r.invest_score}")


# ───────────────────────────────
# CLI
# ───────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Coordinator анализа земельного рынка")
    parser.add_argument("region", help="Название региона РФ, например 'Краснодарский край'")
    args = parser.parse_args()

    coord = Coordinator(args.region)
    coord.run()


if __name__ == "__main__":
    main()
