from __future__ import annotations
from datetime import date
import calendar
import pandas as pd

from typing import List, Callable, Dict

from src.scripts.data_warehouse.access import (
    query_facts,
    getSites,
    getCamps,
)
from src.utils.logging import LOGGER


# -----------------------------------------------------------------------------
# PUBLIC ENTRY POINT
# -----------------------------------------------------------------------------
def build_markdown(session, year: int, month: int, categories: List[str]) -> str:
    """
    Assemble a Markdown report for any combination of categories.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
    year, month : int
        Calendar year / month to summarise.
    categories : list[str]
        e.g. ["Retail", "Marketing"].  Case-insensitive.
    """
    categories = [c.lower() for c in categories]               # normalise
    month_name = date(year, month, 1).strftime("%B %Y")

    section_builders: Dict[str, Callable[..., str]] = {
    "retail": _retail_section,
    "customer_survey": _survey_section,     
    "marketing": _marketing_section,
    }

    markdown_parts = [f"# MCCS Data Analytics – {month_name}\n"]

    for cat in categories:
        builder = section_builders.get(cat)
        if builder is None:
            LOGGER.warning(f"No section builder for category: {cat!r}; skipping.")
            continue
        markdown_parts.append(builder(session, year, month))

    report = "\n".join(markdown_parts).rstrip() + "\n"
    LOGGER.info("Markdown built successfully")
    return report


# -----------------------------------------------------------------------------
# RETAIL SECTION (all 11 insights)
# -----------------------------------------------------------------------------
def _retail_section(session, year: int, month: int) -> str:
    """Return the **Retail** section (11 insights)."""
    # ─── DATE HELPERS ───────────────────────────────────────────────────────
    month_start = date(year, month, 1)
    month_end   = date(year, month, calendar.monthrange(year, month)[1])

    prev_year, prev_month = (year, month - 1) if month > 1 else (year - 1, 12)
    prev_start = date(prev_year, prev_month, 1)

    # ─── SITE CACHES ───────────────────────────────────────────────────────
    sites           = getSites(session)
    mart_site_ids   = [s.site_id for s in sites if getattr(s, "store_format", "").upper() == "MARINE MART"]
    main_site_ids   = [s.site_id for s in sites if getattr(s, "store_format", "").upper() == "MAIN STORE"]

    id_to_name = {
        str(s.site_id): (getattr(s, "site_name", None) or getattr(s, "name", str(s.site_id)))
        .replace("MARINE MART", "")
        .replace("MAIN STORE", "")
        .strip()
        for s in sites
    }

    # ---------------- 1. Total revenue & MoM -----------------
    revenue_df     = query_facts(session, 1, group_name="all", period_level=2, exact_date=month_start)
    total_revenue  = revenue_df["value"].sum() if not revenue_df.empty else 0.0

    prev_rev_df    = query_facts(session, 1, group_name="all", period_level=2, exact_date=prev_start)
    prev_revenue   = prev_rev_df["value"].sum() if not prev_rev_df.empty else 0.0
    pct_rev_change = (100 * (total_revenue - prev_revenue) / prev_revenue) if prev_revenue else 0.0

    # ---------------- 2. Top-5 Marine Marts ------------------
    units_df = query_facts(session, 2, group_names=mart_site_ids, period_level=2, exact_date=month_start)
    md_top5_mart = _top5_list(units_df, id_to_name)

    # ---------------- 3. Top-5 Main Stores -------------------
    main_units_df = query_facts(session, 2, group_names=main_site_ids, period_level=2, exact_date=month_start)
    md_top5_main = _top5_list(main_units_df, id_to_name)

    # ---------------- 4. Day with highest AOV ----------------
    aov_df = query_facts(session, 4, group_name="all", period_level=1,
                         date_from=month_start, date_to=month_end)
    if not aov_df.empty:
        best_row  = aov_df.loc[aov_df["value"].idxmax()]
        best_day  = best_row["date"].strftime("%B %d, %Y")
        best_aov  = best_row["value"]
    else:
        best_day, best_aov = "-", 0.0

    # ---------------- 5. Camp with fewest returned items -----
    camps                 = getCamps(session)
    camp_names            = [c.name for c in camps]
    returned_items_df     = query_facts(session, 5, group_names=camp_names,
                                        period_level=2, exact_date=month_start)
    if not returned_items_df.empty:
        min_row_items      = returned_items_df.loc[returned_items_df["value"].idxmin()]
        least_return_camp  = min_row_items["group_name"]
        least_return_items = int(min_row_items["value"])
    else:
        least_return_camp, least_return_items = "-", 0

    # ---------------- 6. Units sold & MoM --------------------
    units_month_df = query_facts(session, 2, group_name="all", period_level=2, exact_date=month_start)
    units_total    = units_month_df["value"].sum() if not units_month_df.empty else 0
    units_prev_df  = query_facts(session, 2, group_name="all", period_level=2, exact_date=prev_start)
    units_prev     = units_prev_df["value"].sum() if not units_prev_df.empty else 0
    pct_units_change = (100 * (units_total - units_prev) / units_prev) if units_prev else 0.0

    # ---------------- 7. Transactions & MoM ------------------
    txn_month_df = query_facts(session, 3, group_name="all", period_level=2, exact_date=month_start)
    txn_total    = txn_month_df["value"].sum() if not txn_month_df.empty else 0
    txn_prev_df  = query_facts(session, 3, group_name="all", period_level=2, exact_date=prev_start)
    txn_prev     = txn_prev_df["value"].sum() if not txn_prev_df.empty else 0
    pct_txn_change = (100 * (txn_total - txn_prev) / txn_prev) if txn_prev else 0.0

    # ---------------- 8. Return-rate -------------------------
    returned_all_df     = query_facts(
        session,
        5,                       # metric_id for returned items
        group_name="all",        # <─ key change
        period_level=2,
        exact_date=month_start,
    )
    returned_items_total = returned_all_df["value"].sum() if not returned_all_df.empty else 0
    return_rate          = (100 * returned_items_total / units_total) if units_total else 0.0

    # ---------------- 9. Busiest day by transactions ---------
    txn_daily_df = query_facts(session, 3, group_name="all", period_level=1,
                               date_from=month_start, date_to=month_end)
    if not txn_daily_df.empty:
        busiest_row   = txn_daily_df.loc[txn_daily_df["value"].idxmax()]
        busiest_day   = busiest_row["date"].strftime("%B %d, %Y")
        busiest_count = int(busiest_row["value"])
    else:
        busiest_day, busiest_count = "-", 0

    # ---------------- 10. Return-transactions & MoM ----------
    rtxn_month_df = query_facts(session, 6, group_name="all", period_level=2, exact_date=month_start)
    rtxn_total    = rtxn_month_df["value"].sum() if not rtxn_month_df.empty else 0
    rtxn_prev_df  = query_facts(session, 6, group_name="all", period_level=2, exact_date=prev_start)
    rtxn_prev     = rtxn_prev_df["value"].sum() if not rtxn_prev_df.empty else 0
    pct_rtxn_change = (100 * (rtxn_total - rtxn_prev) / rtxn_prev) if rtxn_prev else 0.0

    # ---------------- 11. Weekday busyness -------------------
    busiest_wk_mart, busiest_wk_mart_units = _weekday_peak(session, mart_site_ids, month_start, month_end)
    busiest_wk_main, busiest_wk_main_units = _weekday_peak(session, main_site_ids, month_start, month_end)

    # ─── Compose markdown for Retail ──────────────────────────────────────
    def word(change):        # pretty +/- wording
        return "increase" if change > 0 else "decrease" if change < 0 else "change"

    retail_md = (
        "## Retail\n\n"
        f"1. Total revenue **${total_revenue:,.2f}** "
        f"({pct_rev_change:+.1f}% {word(pct_rev_change)} vs last month).\n\n"
        f"2. Top 5 Marine Marts:\n{md_top5_mart}\n\n"
        f"3. Top 5 Main Stores:\n{md_top5_main}\n\n"
        f"4. Highest AOV on **{best_day}** at **${best_aov:,.2f}**.\n\n"
        f"5. **{least_return_camp}** had the fewest returns "
        f"(**{least_return_items:,} items**).\n\n"
        f"6. Units sold: **{int(units_total):,}** "
        f"({pct_units_change:+.1f}% {word(pct_units_change)}).\n\n"
        f"7. Transactions: **{int(txn_total):,}** "
        f"({pct_txn_change:+.1f}% {word(pct_txn_change)}).\n\n"
        f"8. Return-rate: **{return_rate:.1f}%** of units sold.\n\n"
        f"9. Busiest day: **{busiest_day}** "
        f"({busiest_count:,} transactions).\n\n"
        f"10. Return transactions: **{int(rtxn_total):,}** "
        f"({pct_rtxn_change:+.1f}% {word(pct_rtxn_change)}).\n\n"
        f"11. Weekday peaks – Marine Marts on **{busiest_wk_mart}**s "
        f"({busiest_wk_mart_units:,}), Main Stores on **{busiest_wk_main}**s "
        f"({busiest_wk_main_units:,}).\n"
    )
    return retail_md

# ─────────────────────────────────────────────────────────────────────────────
# ❷  CUSTOMER SURVEY SECTION
#     Follows the same structure/wording conventions as _retail_section()
# ─────────────────────────────────────────────────────────────────────────────
def _survey_section(session, year: int, *_):      # month arg ignored
    """
    Nine annual insights for Customer-Survey metrics.
    All queries use period_level = 4 and exact_date = Jan-01-{year}.
    """
    anchor_date = date(2025, 1, 1)                #  ❄️  fixed date
    sites       = getSites(session)

    mart_ids = [s.site_id for s in sites
                if getattr(s, "store_format", "").upper() == "MARINE MART"]
    main_ids = [s.site_id for s in sites
                if getattr(s, "store_format", "").upper() == "MAIN STORE"]

    id2name = {str(s.site_id): (getattr(s, "site_name", None) or getattr(s, "name", str(s.site_id)))
                             .replace("MARINE MART", "")
                             .replace("MAIN STORE", "")
                             .strip()
               for s in sites}

    # 1. Positive feedback (metric 7) & YoY change vs previous year
    pf_now  = _mean_metric(session, 7, "all", anchor_date)

    # 2. Avg satisfaction (metric 8) & YoY
    sat_now  = _mean_metric(session, 8, "all", anchor_date)

    # 3 & 4. Positive-feedback top-5 lists
    pf_mart = query_facts(session = session, metric_id=7, group_names=mart_ids, period_level=4, exact_date=anchor_date)
    pf_main = query_facts(session = session, metric_id=7, group_names=mart_ids, period_level=4,  exact_date=anchor_date)
    md_pf_mart = _top5_list(pf_mart, id2name, fmt=lambda v: f"{v*100:.1f}%")
    md_pf_main = _top5_list(pf_main, id2name, fmt=lambda v: f"{v*100:.1f}%")

    # 5-7. Atmosphere, Price, Service (metrics 20-22) – Main Stores only
    atm  = query_facts(session = session, metric_id=20, group_names=main_ids, period_level=4, exact_date=anchor_date)
    LOGGER.debug(f"ATM: {atm}")
    price = query_facts(session = session, metric_id=21, group_names=main_ids, period_level=4, exact_date=anchor_date)
    LOGGER.debug(f"Price: {price}")
    serv  = query_facts(session = session, metric_id=22, group_names=main_ids, period_level=4, exact_date=anchor_date)
    LOGGER.debug(f"Serv: {serv}")
    md_atm   = _top5_list(atm,   id2name, fmt=lambda v: f"{v:.2f}")
    md_price = _top5_list(price, id2name, fmt=lambda v: f"{v:.2f}")
    md_serv  = _top5_list(serv,  id2name, fmt=lambda v: f"{v:.2f}")

    # 8. Best single site for positive feedback (any format)
    all_sites_pf = query_facts(session=session, metric_id=7,
                               group_names=[s.site_id for s in sites], period_level=4,
                               exact_date=anchor_date)
    if not all_sites_pf.empty:
        best_row   = all_sites_pf.loc[all_sites_pf["value"].idxmax()]
        best_site  = id2name.get(str(best_row["group_name"]), best_row["group_name"])
        best_score = best_row["value"]*100
    else:
        best_site, best_score = "-", 0.0

    # 9. Best single Main Store overall satisfaction (metric 8)
    main_sat = query_facts(session, 8, main_ids, 4, exact_date=anchor_date)
    if not main_sat.empty:
        sat_row   = main_sat.loc[main_sat["value"].idxmax()]
        sat_site  = id2name.get(str(sat_row["group_name"]), sat_row["group_name"])
        sat_score = sat_row["value"]
    else:
        sat_site, sat_score = "-", 0.0

    # ── markdown ───────────────────────────────────────────────────────────
    w = lambda c: "increase" if c > 0 else "decrease" if c < 0 else "change"
    md = (
        "## Customer Survey (e.g. January 2025)\n\n"
        f"1. Positive feedback **{pf_now*100:.1f}%**\n\n"
        f"2. Average satisfaction **{sat_now:.2f}/5**\n\n"
        f"3. Marine Marts – positive feedback leaders:\n{md_pf_mart}\n\n"
        f"4. Main Stores – positive feedback leaders:\n{md_pf_main}\n\n"
        f"5. Main Stores – atmosphere score leaders:\n{md_atm}\n\n"
        f"6. Main Stores – price satisfaction leaders:\n{md_price}\n\n"
        f"7. Main Stores – service satisfaction leaders:\n{md_serv}\n\n"
        f"8. Best site overall for positive feedback: "
        f"**{best_site}** ({best_score:.1f}%).\n\n"
    )
    return md


def _marketing_section(session, year: int, month: int) -> str:
    """
    Nine Adobe-style insights for social + email metrics.
    Period-level = 2 (monthly).
    """
    m_start  = date(year, month, 1)
    p_year, p_month = (year, month - 1) if month > 1 else (year - 1, 12)
    p_start  = date(p_year, p_month, 1)

    # helper for one-liner metric pulls
    def _val(metric_id, when):
        df = query_facts(session, metric_id, group_name="all",
                         period_level=2, exact_date=when)
        agg = df["value"].mean() if metric_id in (17, 18, 19) else df["value"].sum()
        return agg if not df.empty else 0.0

    # ---- headline KPIs ---------------------------------------------------
    total_eng_now   = _val(9,  m_start)
    total_eng_prev  = _val(9,  p_start)
    eng_change_pct  = 100*(total_eng_now-total_eng_prev)/total_eng_prev if total_eng_prev else 0.0

    foll_now        = _val(10, m_start)
    foll_prev       = _val(10, p_start)
    foll_change_pct = 100*(foll_now-foll_prev)/foll_prev if foll_prev else 0.0

    reach_now       = _val(16, m_start)
    reach_prev      = _val(16, p_start)
    reach_delta_pct = 100*(reach_now-reach_prev)/reach_prev if reach_prev else 0.0

    # ---- content cadence & quality --------------------------------------
    posts_now   = _val(11, m_start)
    likes_now   = _val(12, m_start)
    comm_now    = _val(13, m_start)
    share_now   = _val(14, m_start)
    clicks_now  = _val(15, m_start)

    # ---- engagement-rate momentum ---------------------------------------
    er_delta_pp = _val(17, m_start) * 100        # already a % change value

    # ---- email deliverability & opens ------------------------------------
    deliv_rt = _val(18, m_start) * 100
    open_rt  = _val(19, m_start) * 100

    # ---- wording helper --------------------------------------------------
    word = lambda c: "↑" if c > 0 else "↓" if c < 0 else "→"


    # pretty strings – show “awaiting data” when the metric is still zero
    deliv_str = f"{deliv_rt:.2f} %" if deliv_rt else "...awaiting data"
    open_str  = f"{open_rt:.2f} %"  if open_rt  else "...awaiting data"

    # ---- compose markdown ------------------------------------------------
    marketing_md = (
        "## Marketing\n\n"
        f"1. **Total engagement** {int(total_eng_now):,} "
        f"({word(eng_change_pct)} {eng_change_pct:+.1f}% vs prev-month).\n\n"
        f"2. **New followers** {int(foll_now):,} "
        f"({word(foll_change_pct)} {foll_change_pct:+.1f}% MoM).\n\n"
        f"3. **Reach** {int(reach_now):,} unique users "
        f"({reach_delta_pct:+.1f}% MoM).\n\n"
        f"4. **Content volume** {int(posts_now):,} posts published.\n\n"
        f"5. **Reaction mix** – {int(likes_now):,} likes, "
        f"{int(comm_now):,} comments, {int(share_now):,} shares.\n\n"
        f"6. **Estimated clicks** {int(clicks_now):,} – a direct traffic proxy.\n\n"
        f"7. **Engagement-rate momentum** {er_delta_pp:+.2f} pp vs prior month.\n\n"
        f"8. **Email delivery rate** {deliv_str}.\n\n"
        f"9. **Email open rate** {open_str}.\n"
    )
    return marketing_md


# helper: annual mean value
def _mean_metric(session, metric_id, group, anchor_date):
    df = query_facts(session = session,
                     metric_id = metric_id,
                     group_name=group,
                     period_level=4,
                     exact_date=anchor_date)
    return df["value"].mean() if not df.empty else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Helper tweaks for survey values
# ─────────────────────────────────────────────────────────────────────────────
def _top5_list(df, id_to_name, fmt=lambda v: f"{v:.2f}") -> str:
    """Generic bullet list; fmt is a formatter for the numeric value."""
    if df.empty:
        return "   * _No data available_"
    df["site_name"] = (
        df["group_name"].astype(str)
        .map(id_to_name)
        .fillna(df["group_name"].astype(str))
    )
    top5 = df.sort_values("value", ascending=False).head(5)
    return "\n".join(f"   * {r.site_name} — {fmt(r.value)}"
                     for r in top5.itertuples(index=False))


def _weekday_peak(session, site_ids, month_start, month_end):
    """Return tuple (weekday_name, units) with highest units sold for a site list."""
    if not site_ids:
        return "-", 0
    daily_df = query_facts(session, 2, group_names=site_ids, period_level=1,
                           date_from=month_start, date_to=month_end)
    if daily_df.empty:
        return "-", 0
    daily_df["weekday"] = pd.to_datetime(daily_df["date"]).dt.day_name()
    week = daily_df.groupby("weekday")["value"].sum()
    return week.idxmax(), int(week.max())



# if __name__ == "__main__":
#     # Example usage
#     with SessionLocal() as session:
#         md = build_markdown(session=session, year = 2024, month=5)
#         print(md)
    
