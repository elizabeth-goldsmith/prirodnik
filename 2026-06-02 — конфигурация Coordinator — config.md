---
name: Конфигурация Coordinator
description: JSON-конфиг агента-оркестратора: пайплайн, зависимости, агенты, форматы данных
type: reference
---

# Конфигурация Coordinator

## Общие параметры

```json
{
  "project_name": "LandMarket-Research",
  "version": "1.0.0",
  "coordinator": {
    "role": "оркестратор",
    "task": "поиск инвестиционно привлекательных муниципалитетов в регионе РФ",
    "max_parallel_agents": 6,
    "batch_size": 5,
    "output_format": "json",
    "data_store": "sqlite",
    "database_path": "./data/land_research.db"
  }
}
```

## Реестр агентов

```json
{
  "agents": [
    {
      "id": "macro-region-analyst",
      "name": "Macro-Region-Analyst",
      "role": "аналитик уровня субъекта",
      "prompt_file": "2026-06-02 — промпты субагентов земельного рынка — prompts.md",
      "inputs": ["region_name"],
      "outputs": ["macro_report"],
      "triggers": ["start"],
      "parallel": false,
      "timeout_sec": 300,
      "retries": 2
    },
    {
      "id": "municipal-profiler",
      "name": "Municipal-Profiler",
      "role": "сборщик профиля муниципалитета",
      "prompt_file": "2026-06-02 — промпты субагентов земельного рынка — prompts.md",
      "inputs": ["municipality_name", "region_name"],
      "outputs": ["municipal_profile"],
      "triggers": ["after_macro_report"],
      "parallel": true,
      "batch_size": 5,
      "timeout_sec": 600,
      "retries": 2
    },
    {
      "id": "land-market-tracker",
      "name": "Land-Market-Tracker",
      "role": "аналитик земельного рынка",
      "prompt_file": "2026-06-02 — промпты субагентов земельного рынка — prompts.md",
      "inputs": ["municipality_name"],
      "outputs": ["land_market_data"],
      "triggers": ["after_macro_report"],
      "parallel": true,
      "batch_size": 5,
      "timeout_sec": 900,
      "retries": 3
    },
    {
      "id": "development-spotter",
      "name": "Development-Spotter",
      "role": "охотник за точками роста",
      "prompt_file": "2026-06-02 — промпты субагентов земельного рынка — prompts.md",
      "inputs": ["municipality_name", "region_name"],
      "outputs": ["development_triggers"],
      "triggers": ["after_macro_report"],
      "parallel": true,
      "batch_size": 5,
      "timeout_sec": 600,
      "retries": 2
    },
    {
      "id": "invest-ranker",
      "name": "Invest-Ranker",
      "role": "финальный оценщик",
      "prompt_file": "2026-06-02 — промпты субагентов земельного рынка — prompts.md",
      "inputs": [
        "macro_report",
        "municipal_profiles[]",
        "land_market_data[]",
        "development_triggers[]"
      ],
      "outputs": ["invest_ranking"],
      "triggers": ["after_all_batches_complete"],
      "parallel": false,
      "timeout_sec": 300,
      "retries": 1
    }
  ]
}
```

## Пайплайн (DAG)

```json
{
  "pipeline": {
    "steps": [
      {
        "step": 1,
        "name": "macro_analysis",
        "agent": "macro-region-analyst",
        "condition": "start",
        "next": ["batch_preparation"]
      },
      {
        "step": 2,
        "name": "batch_preparation",
        "agent": "coordinator",
        "action": "split_municipalities_into_batches",
        "condition": "macro_report_received",
        "next": ["parallel_research"]
      },
      {
        "step": 3,
        "name": "parallel_research",
        "agents": ["municipal-profiler", "land-market-tracker", "development-spotter"],
        "mode": "parallel_per_municipality",
        "condition": "batches_ready",
        "next": ["ranking"]
      },
      {
        "step": 4,
        "name": "ranking",
        "agent": "invest-ranker",
        "condition": "all_batches_complete",
        "next": ["output"]
      },
      {
        "step": 5,
        "name": "output",
        "agent": "coordinator",
        "action": "format_and_save_results",
        "output_files": [
          "./output/invest_ranking_{region}_{date}.csv",
          "./output/invest_ranking_{region}_{date}.md"
        ]
      }
    ]
  }
}
```

## Схема данных (SQLite)

```json
{
  "tables": {
    "macro_report": {
      "region_name": "TEXT PRIMARY KEY",
      "population": "INTEGER",
      "migration_trend": "TEXT",
      "infra_projects": "TEXT",
      "economic_assessment": "TEXT",
      "land_policy": "TEXT",
      "overall_trend": "TEXT",
      "created_at": "TEXT"
    },
    "municipal_profiles": {
      "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
      "region_name": "TEXT",
      "municipality_name": "TEXT",
      "population": "INTEGER",
      "distance_to_center_km": "REAL",
      "infra_score": "INTEGER",
      "ecology_risk": "TEXT",
      "profile_json": "TEXT",
      "created_at": "TEXT"
    },
    "land_market_data": {
      "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
      "region_name": "TEXT",
      "municipality_name": "TEXT",
      "avg_price_per_sotka": "REAL",
      "price_trend_pct": "REAL",
      "supply_count": "INTEGER",
      "demand_level": "TEXT",
      "typical_plot_size": "REAL",
      "market_json": "TEXT",
      "created_at": "TEXT"
    },
    "development_triggers": {
      "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
      "region_name": "TEXT",
      "municipality_name": "TEXT",
      "description": "TEXT",
      "source": "TEXT",
      "impact_level": "INTEGER",
      "timeframe": "TEXT",
      "created_at": "TEXT"
    },
    "invest_ranking": {
      "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
      "region_name": "TEXT",
      "municipality_name": "TEXT",
      "invest_score": "REAL",
      "avg_price_per_sotka": "REAL",
      "key_triggers": "TEXT",
      "recommendation": "TEXT",
      "rank": "INTEGER",
      "created_at": "TEXT"
    }
  }
}
```

## Формула Invest-Score (Invest-Ranker)

```json
{
  "scoring": {
    "base_score": 50,
    "weights": {
      "price_below_regional_avg": 15,
      "price_trend_above_10pct": 10,
      "strong_triggers_count_ge_2": 15,
      "infra_score_above_7": 10,
      "ecology_risk_high": -20,
      "macro_region_decline": -15
    },
    "recommendation_thresholds": {
      "Buy": "invest_score >= 75",
      "Hold": "invest_score >= 50 AND invest_score < 75",
      "Skip": "invest_score < 50"
    }
  }
}
```

## Логика Coordinator (псевдокод)

```
function run(region_name):
  1. Запросить у пользователя: region_name
  2. Получить список муниципалитетов региона (ФИАС / Википедия / Browser)
  3. Запустить Macro-Region-Analyst(region_name)
  4. Разбить муниципалитеты на батчи по batch_size
  5. Для каждого батча параллельно запустить:
       Municipal-Profiler(municipality)
       Land-Market-Tracker(municipality)
       Development-Spotter(municipality)
  6. Сохранить все результаты в SQLite
  7. После завершения всех батчей запустить Invest-Ranker(все_данные)
  8. Сохранить invest_ranking в CSV и Markdown
  9. Вернуть пользователю путь к файлам и краткую сводку (Топ-5)
```
