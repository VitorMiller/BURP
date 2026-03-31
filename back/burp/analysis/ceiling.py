from __future__ import annotations

from typing import Any, Iterable

from burp.settings import get_settings


TETO_REFERENCE_SOURCE = "Subsídio dos ministros do STF"
TETO_REFERENCE_NOTE = (
    "Usa os valores anuais configurados no backend. Por padrão, 2023-2025 seguem a Lei 14.520/2023; "
    "anos posteriores reutilizam o último valor conhecido até nova configuração em "
    "BURP_TETO_CONSTITUCIONAL_BY_YEAR."
)


def resolve_constitutional_ceiling(year: int) -> dict[str, Any]:
    settings = get_settings()
    table = settings.teto_constitucional_by_year
    available_years = sorted(table)
    if not available_years:
        raise RuntimeError("teto_constitucional_by_year_not_configured")

    if year in table:
        reference_year = year
        fallback = False
    else:
        prior_years = [candidate for candidate in available_years if candidate <= year]
        reference_year = prior_years[-1] if prior_years else available_years[0]
        fallback = reference_year != year

    value = round(float(table[reference_year]), 2)
    return {
        "year": year,
        "reference_year": reference_year,
        "value": value,
        "fallback_applied": fallback,
        "source": TETO_REFERENCE_SOURCE,
        "source_note": TETO_REFERENCE_NOTE,
    }


def build_ceiling_reference(years: Iterable[int]) -> dict[str, Any]:
    values_by_year: dict[str, float] = {}
    fallbacks: list[dict[str, Any]] = []
    for year in sorted(set(years)):
        info = resolve_constitutional_ceiling(year)
        values_by_year[str(year)] = info["value"]
        if info["fallback_applied"]:
            fallbacks.append(
                {
                    "year": year,
                    "reference_year": info["reference_year"],
                    "value": info["value"],
                }
            )
    return {
        "source": TETO_REFERENCE_SOURCE,
        "source_note": TETO_REFERENCE_NOTE,
        "values_by_year": values_by_year,
        "fallbacks": fallbacks,
    }
