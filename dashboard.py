import streamlit as st
import sqlite3
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

DB_PATH = "marketpulse.db"

st.set_page_config(
    page_title="MarketPulse",
    page_icon="📡",
    layout="wide",
)

# ── helpers ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_signals_df() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT s.id, s.signal_type, s.confidence, s.intensity,
               s.brand_or_product, s.ticker_hint, s.trigger_phrase,
               s.market_implication, s.classified_at,
               p.text, p.platform, p.source, p.timestamp
        FROM signals s
        JOIN raw_posts p ON s.post_id = p.id
        WHERE s.signal_detected = 1
        ORDER BY s.classified_at DESC
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_json(path: str, default):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


SIGNAL_COLORS = {
    "OBSESSION":        "#ef4444",
    "SOCIAL_PROOF":     "#f97316",
    "SWITCHING":        "#3b82f6",
    "DISCOVERY":        "#22c55e",
    "SPEND_CONFESSION": "#a855f7",
}

STATUS_COLORS = {
    "BUY_WATCH": "#ef4444",
    "MONITOR":   "#f59e0b",
    "NEUTRAL":   "#6b7280",
}

ACTION_COLORS = {
    "WATCH":            "#22c55e",
    "RESEARCH_FURTHER": "#f59e0b",
    "PASS":             "#6b7280",
}

# ── load data ─────────────────────────────────────────────────────────────────

signals_df   = load_signals_df()
brand_scores = load_json("brand_scores.json", [])
gap_scores   = load_json("gap_scores.json", [])
stock_data   = load_json("stock_data.json", {})
briefs       = load_json("investigation_briefs.json", [])
validations  = load_json("validation_results.json", [])

# ── sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("📡 MarketPulse")
st.sidebar.caption("Consumer conversations → financial signals")

# quick stats
total_posts   = signals_df["id"].count() if not signals_df.empty else 0
breakouts     = sum(1 for b in brand_scores if b.get("breakout"))
buy_watch     = sum(1 for g in gap_scores if g["status"] == "BUY_WATCH")

st.sidebar.metric("Signals detected", len(signals_df))
st.sidebar.metric("Breakouts", breakouts)
st.sidebar.metric("BUY_WATCH alerts", buy_watch)
st.sidebar.divider()

page = st.sidebar.radio(
    "View",
    ["Trending Brands", "Signal Feed", "Gap Panel", "Intelligence Briefs", "Stock Correlation"],
)

# ── page: trending brands ─────────────────────────────────────────────────────

if page == "Trending Brands":
    st.title("🔥 Trending Brands")
    st.caption(f"Ranked by consumer momentum score · {len(signals_df)} total signals")

    if not brand_scores:
        st.warning("Run `score_brands.py` to generate scores.")
        st.stop()

    # deduplicate by first word, keep highest score
    seen, top_brands = set(), []
    for b in brand_scores:
        key = b["brand"].lower().split()[0]
        if key not in seen and b["signal_count"] > 0:
            seen.add(key)
            top_brands.append(b)
        if len(top_brands) == 20:
            break

    # breakout badges
    if breakouts:
        st.error(f"⚡ {breakouts} BREAKOUT signal{'s' if breakouts > 1 else ''} detected")
        for b in brand_scores:
            if b.get("breakout"):
                st.markdown(
                    f"&nbsp;&nbsp;**{b['brand']}** — score jumped to **{b['score']:.0f}** "
                    f"· {b.get('dominant_signal_type')} · `{b['ticker'] or 'private'}`"
                )
        st.divider()

    # metric cards
    cols = st.columns(4)
    for i, b in enumerate(top_brands[:8]):
        ticker_label = f" · {b['ticker']}" if b.get("ticker") else ""
        with cols[i % 4]:
            st.metric(
                label=f"{b['brand'][:20]}{ticker_label}",
                value=f"{b['score']:.0f} / 100",
                delta=f"{b['signal_count']} signals",
            )

    st.divider()

    # bar chart — colour breakouts differently
    chart_data = top_brands[:15]
    bar_colors = ["#f59e0b" if b.get("breakout") else "#6366f1" for b in chart_data]

    fig = go.Figure(go.Bar(
        x=[b["score"] for b in chart_data],
        y=[b["brand"][:28] for b in chart_data],
        orientation="h",
        marker_color=bar_colors,
        text=[f"{b['score']:.0f}" for b in chart_data],
        textposition="outside",
    ))
    fig.update_layout(
        height=520,
        xaxis_title="Momentum Score",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=0, r=50, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # signal type pie
    st.subheader("Signal type breakdown")
    c1, c2 = st.columns(2)
    type_counts = signals_df["signal_type"].value_counts()
    fig2 = go.Figure(go.Pie(
        labels=type_counts.index,
        values=type_counts.values,
        marker_colors=[SIGNAL_COLORS.get(t, "#888") for t in type_counts.index],
        hole=0.45,
    ))
    fig2.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=10),
                       paper_bgcolor="rgba(0,0,0,0)")
    c1.plotly_chart(fig2, use_container_width=True)

    # platform breakdown
    plat_counts = signals_df["platform"].value_counts()
    fig3 = go.Figure(go.Bar(
        x=plat_counts.index,
        y=plat_counts.values,
        marker_color=["#6366f1", "#f59e0b"],
        text=plat_counts.values,
        textposition="outside",
    ))
    fig3.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=30),
                       plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                       yaxis_title="Signals")
    c2.plotly_chart(fig3, use_container_width=True)


# ── page: signal feed ─────────────────────────────────────────────────────────

elif page == "Signal Feed":
    st.title("📋 Signal Feed")

    col1, col2, col3 = st.columns(3)
    signal_filter = col1.multiselect(
        "Signal type", options=list(SIGNAL_COLORS.keys()), default=list(SIGNAL_COLORS.keys())
    )
    conf_min = col2.slider("Min confidence", 0.0, 1.0, 0.65, step=0.05)
    platform_filter = col3.multiselect(
        "Platform", options=signals_df["platform"].unique().tolist(),
        default=signals_df["platform"].unique().tolist(),
    )

    filtered = signals_df[
        signals_df["signal_type"].isin(signal_filter) &
        (signals_df["confidence"] >= conf_min) &
        signals_df["platform"].isin(platform_filter)
    ]
    st.caption(f"Showing {len(filtered)} of {len(signals_df)} signals")

    for _, row in filtered.head(60).iterrows():
        color = SIGNAL_COLORS.get(row["signal_type"], "#888")
        with st.container():
            c1, c2 = st.columns([1, 5])
            with c1:
                st.markdown(
                    f"<div style='background:{color};color:white;padding:4px 8px;"
                    f"border-radius:4px;font-size:11px;text-align:center;font-weight:600'>"
                    f"{row['signal_type']}</div>",
                    unsafe_allow_html=True,
                )
                st.caption(f"conf: {row['confidence']:.2f}")
                st.caption(f"{row['platform']} · {row.get('intensity') or '—'}")
            with c2:
                brand  = row["brand_or_product"] or "Unknown"
                ticker = f" `{row['ticker_hint']}`" if row.get("ticker_hint") else ""
                st.markdown(f"**{brand}**{ticker}")
                if row["trigger_phrase"]:
                    st.markdown(f"> *\"{row['trigger_phrase']}\"*")
                if row["market_implication"]:
                    st.caption(f"📈 {row['market_implication']}")
            st.divider()


# ── page: gap panel ───────────────────────────────────────────────────────────

elif page == "Gap Panel":
    st.title("🎯 Gap Panel")
    st.caption("Brands where consumer excitement outpaces institutional awareness")

    if not gap_scores:
        st.warning("Run `gap_detection.py` to generate gap scores.")
        st.stop()

    # validation lookup
    val_map = {v["ticker"]: v for v in validations}

    for g in gap_scores:
        status = g["status"]
        color  = STATUS_COLORS.get(status, "#888")
        val    = val_map.get(g["ticker"], {})

        with st.container():
            c1, c2, c3, c4 = st.columns([3, 2, 2, 3])
            c1.markdown(f"**{g['brand']}**  \n`{g['ticker']}`")
            c2.metric("Consumer Score", f"{g['consumer_score']:.0f}")
            c3.metric("Institutional",  f"{g['institutional_score']:.0f}")
            c4.markdown(
                f"<div style='background:{color};color:white;padding:6px 12px;"
                f"border-radius:6px;text-align:center;font-weight:bold;margin-bottom:6px'>"
                f"Gap {g['gap_score']:+.0f} · {status}</div>",
                unsafe_allow_html=True,
            )

            # show validation verdict if available
            if val:
                action = val.get("recommended_action", "")
                ac     = ACTION_COLORS.get(action, "#888")
                vscore = val.get("validation_score", "—")
                reason = val.get("reason", "")
                c4.markdown(
                    f"<div style='background:{ac};color:white;padding:4px 10px;"
                    f"border-radius:4px;font-size:12px;text-align:center'>"
                    f"{action} · val {vscore}</div>",
                    unsafe_allow_html=True,
                )
                if reason:
                    st.caption(f"↳ {reason}")

            st.divider()


# ── page: intelligence briefs ─────────────────────────────────────────────────

elif page == "Intelligence Briefs":
    st.title("🧠 Intelligence Briefs")
    st.caption("AI-generated one-page briefs for brands above momentum threshold")

    if not briefs:
        st.warning("Run `agent_investigate.py` to generate briefs.")
        st.stop()

    val_map = {v["ticker"]: v for v in validations}

    for b in briefs:
        ticker  = b.get("ticker") or "private"
        score   = b.get("score", 0)
        breakout = b.get("breakout", False)
        val      = val_map.get(b.get("ticker"), {})

        header_color = "#f59e0b" if breakout else "#6366f1"
        with st.expander(
            f"{'⚡ ' if breakout else ''}{b['brand']}  ·  {ticker}  ·  Score {score:.0f}",
            expanded=breakout or score >= 45,
        ):
            # meta row
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Momentum Score", f"{score:.0f} / 100")
            mc2.metric("Reddit Samples", b.get("reddit_sample_count", 0))
            mc3.metric("News Headlines", b.get("headline_count", 0))

            if breakout:
                st.error("⚡ BREAKOUT — score jumped 15+ pts in 48 hours")

            # validation badge
            if val:
                action = val.get("recommended_action", "")
                ac     = ACTION_COLORS.get(action, "#888")
                st.markdown(
                    f"<div style='background:{ac};color:white;display:inline-block;"
                    f"padding:4px 12px;border-radius:4px;font-weight:600;margin-bottom:8px'>"
                    f"Validation: {action} · score {val.get('validation_score', '—')}</div>",
                    unsafe_allow_html=True,
                )
                if val.get("reason"):
                    st.caption(val["reason"])

            st.divider()
            st.markdown(b.get("brief", "No brief available."))


# ── page: stock correlation ───────────────────────────────────────────────────

elif page == "Stock Correlation":
    st.title("📈 Stock Correlation")
    st.caption("90-day price performance for tickers with consumer signals")

    if not stock_data:
        st.warning("Run `fetch_stocks.py` to pull stock data.")
        st.stop()

    selected = st.selectbox("Select ticker", list(stock_data.keys()))
    data     = stock_data[selected]
    weekly   = data.get("weekly_returns", [])

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "90d Return", f"{data['pct_change_90d']:+.1f}%",
        delta_color="normal" if data["pct_change_90d"] >= 0 else "inverse",
    )
    col2.metric("Start Price", f"${data['start_price']}")
    col3.metric("End Price",   f"${data['end_price']}")

    # consumer signals for this ticker
    ticker_brand_map = {b["ticker"]: b["brand"] for b in brand_scores if b.get("ticker")}
    brand_name = ticker_brand_map.get(selected, selected)
    brand_signals = signals_df[
        signals_df["brand_or_product"].str.contains(
            brand_name.split()[0], case=False, na=False
        )
    ] if brand_name else pd.DataFrame()

    if not brand_signals.empty:
        st.info(f"🔔 {len(brand_signals)} consumer signal(s) detected for **{brand_name}**")
        for _, sig in brand_signals.iterrows():
            color = SIGNAL_COLORS.get(sig["signal_type"], "#888")
            st.markdown(
                f"<span style='background:{color};color:white;padding:2px 6px;"
                f"border-radius:3px;font-size:11px'>{sig['signal_type']}</span>"
                f" &nbsp; conf {sig['confidence']:.2f} · {sig['platform']} — "
                f"*\"{sig['trigger_phrase']}\"*",
                unsafe_allow_html=True,
            )
        st.divider()

    if weekly:
        fig = go.Figure(go.Bar(
            x=list(range(len(weekly))),
            y=weekly,
            name="Weekly Return %",
            marker_color=["#22c55e" if v >= 0 else "#ef4444" for v in weekly],
        ))
        fig.update_layout(
            title=f"{selected} — Weekly Returns (last 90 days)",
            xaxis_title="Week",
            yaxis_title="Return %",
            height=350,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # summary table with signal flag and 90d return colour
    st.subheader("All tracked tickers")
    rows = []
    for t, d in stock_data.items():
        has_signal = any(b.get("ticker") == t for b in brand_scores)
        rows.append({
            "Ticker":     t,
            "90d Return": f"{d['pct_change_90d']:+.1f}%",
            "Start":      f"${d['start_price']}",
            "End":        f"${d['end_price']}",
            "Signal":     "✅" if has_signal else "—",
            "BUY_WATCH":  "🔴" if any(g["ticker"] == t and g["status"] == "BUY_WATCH" for g in gap_scores) else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
