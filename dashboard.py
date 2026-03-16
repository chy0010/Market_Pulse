import streamlit as st
import sqlite3
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone

DB_PATH = "marketpulse.db"

st.set_page_config(
    page_title="MarketPulse",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── design tokens ─────────────────────────────────────────────────────────────
BG       = "#FAFAF8"
SURFACE  = "#FFFFFF"
BORDER   = "#E8E6E1"
TEXT     = "#1C1C1A"
MUTED    = "#8C8A84"
ACCENT   = "#3D3D3A"

SIGNAL_COLORS = {
    "OBSESSION":        "#C9796A",   # muted terracotta
    "SOCIAL_PROOF":     "#C4965A",   # warm amber
    "SWITCHING":        "#6A8FBF",   # soft slate blue
    "DISCOVERY":        "#7BA68A",   # sage green
    "SPEND_CONFESSION": "#9C7FB5",   # dusty lavender
}
SIGNAL_BG = {k: v + "18" for k, v in SIGNAL_COLORS.items()}

STATUS_COLORS  = {"BUY_WATCH": "#C9796A", "MONITOR": "#C4965A", "NEUTRAL": "#AEACAA"}
ACTION_COLORS  = {"WATCH": "#7BA68A", "RESEARCH_FURTHER": "#C4965A", "PASS": "#AEACAA"}

CHART_COLORS = ["#3D3D3A", "#6A8FBF", "#C9796A", "#7BA68A", "#C4965A", "#9C7FB5"]

PLOTLY_BASE = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="'Inter', sans-serif", color=MUTED, size=11),
    margin=dict(l=10, r=20, t=24, b=10),
)

# ── global CSS ────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Cormorant:wght@400;500;600&family=Inter:wght@300;400;500;600&display=swap');

  html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
    background: {BG};
    color: {TEXT};
  }}
  .main .block-container {{ background: {BG}; padding-top: 2rem; max-width: 1280px; }}
  #MainMenu, header, footer {{ visibility: hidden; }}

  /* sidebar */
  section[data-testid="stSidebar"] {{
    background: {SURFACE};
    border-right: 1px solid {BORDER};
  }}
  section[data-testid="stSidebar"] * {{ color: {TEXT} !important; }}

  /* metric cards */
  div[data-testid="metric-container"] {{
    background: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: 6px;
    padding: 18px 22px;
  }}
  div[data-testid="metric-container"] label {{
    font-size: 11px !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: {MUTED} !important;
    font-weight: 500;
  }}
  div[data-testid="metric-container"] div[data-testid="metric-value"] {{
    font-size: 28px !important;
    font-weight: 600 !important;
    color: {TEXT} !important;
    font-family: 'Cormorant', serif !important;
  }}

  /* radio buttons */
  div[data-testid="stRadio"] label {{
    font-size: 13px !important;
    letter-spacing: 0.04em;
    padding: 6px 0 !important;
  }}

  /* expanders */
  div[data-testid="stExpander"] {{
    background: {SURFACE};
    border: 1px solid {BORDER} !important;
    border-radius: 6px;
    margin-bottom: 10px;
  }}

  /* select box */
  div[data-testid="stSelectbox"] > div {{
    background: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: 6px;
  }}

  /* divider */
  hr {{ border-color: {BORDER} !important; margin: 24px 0 !important; }}

  /* slider */
  div[data-testid="stSlider"] {{ color: {MUTED}; }}
</style>
""", unsafe_allow_html=True)

# ── data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_signals_df():
    # Streamlit Cloud: no SQLite DB, load from exported JSON
    try:
        with open("signals_data.json") as f:
            return pd.DataFrame(json.load(f))
    except FileNotFoundError:
        pass
    # Local dev fallback: live SQLite DB
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("""
            SELECT s.id, s.signal_type, s.confidence, s.intensity,
                   s.brand_or_product, s.ticker_hint, s.trigger_phrase,
                   s.market_implication, s.classified_at,
                   p.text, p.platform, p.timestamp
            FROM signals s
            JOIN raw_posts p ON s.post_id = p.id
            WHERE s.signal_detected = 1
            ORDER BY s.classified_at DESC
        """, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except FileNotFoundError: return default

signals_df   = load_signals_df()
brand_scores = load_json("brand_scores.json", [])
gap_scores   = load_json("gap_scores.json", [])
stock_data   = load_json("stock_data.json", {})
briefs       = load_json("investigation_briefs.json", [])
validations  = load_json("validation_results.json", [])

breakouts = sum(1 for b in brand_scores if b.get("breakout"))
buy_watch = sum(1 for g in gap_scores if g["status"] == "BUY_WATCH")

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style='padding: 24px 8px 20px'>
      <div style='font-family:Cormorant,serif;font-size:22px;font-weight:600;
                  color:{TEXT};letter-spacing:0.04em;margin-bottom:4px'>
        MarketPulse
      </div>
      <div style='font-size:11px;color:{MUTED};letter-spacing:0.08em;
                  text-transform:uppercase'>
        Consumer Signal Intelligence
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    st.metric("Signals", f"{len(signals_df):,}")
    st.metric("Brands tracked", str(len({b["brand"].split()[0] for b in brand_scores})))
    st.metric("BUY_WATCH alerts", str(buy_watch))
    st.divider()

    page = st.radio("", [
        "Overview", "Trending", "Signal Feed", "Gap Panel", "Briefs", "Stocks"
    ], label_visibility="collapsed")

# ── helpers ───────────────────────────────────────────────────────────────────
def page_title(title: str, sub: str = ""):
    st.markdown(f"""
    <div style='margin-bottom:32px;padding-bottom:20px;border-bottom:1px solid {BORDER}'>
      <h1 style='font-family:Cormorant,serif;font-size:32px;font-weight:500;
                 color:{TEXT};margin:0 0 6px;letter-spacing:0.01em'>{title}</h1>
      {'<p style="font-size:13px;color:' + MUTED + ';margin:0;letter-spacing:0.02em">' + sub + '</p>' if sub else ''}
    </div>
    """, unsafe_allow_html=True)

def pill(text: str, color: str, bg: str = ""):
    bg = bg or color + "14"
    return (
        f"<span style='background:{bg};color:{color};border:1px solid {color}30;"
        f"padding:2px 10px;border-radius:3px;font-size:11px;"
        f"font-weight:500;letter-spacing:0.04em;text-transform:uppercase'>{text}</span>"
    )

def card_open(border_accent: str = BORDER) -> str:
    return (
        f"<div style='background:{SURFACE};border:1px solid {BORDER};"
        f"border-left:2px solid {border_accent};border-radius:6px;"
        f"padding:18px 22px;margin-bottom:10px'>"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if page == "Overview":
    # hero
    st.markdown(f"""
    <div style='background:{SURFACE};border:1px solid {BORDER};border-radius:8px;
                padding:40px 48px;margin-bottom:32px'>
      <div style='font-size:11px;color:{MUTED};text-transform:uppercase;
                  letter-spacing:0.14em;margin-bottom:14px'>
        Market Intelligence
      </div>
      <h1 style='font-family:Cormorant,serif;font-size:42px;font-weight:500;
                 color:{TEXT};margin:0 0 12px;line-height:1.2;letter-spacing:0.01em'>
        People talk.<br>Markets listen.
      </h1>
      <p style='font-size:14px;color:{MUTED};margin:0;max-width:520px;line-height:1.7'>
        Scanning Reddit &amp; YouTube for consumer obsession, brand switching,
        and spend signals — before they appear in earnings reports.
      </p>
    </div>
    """, unsafe_allow_html=True)

    # kpi row
    try:
        conn = sqlite3.connect(DB_PATH)
        total_raw = conn.execute("SELECT COUNT(*) FROM raw_posts").fetchone()[0]
        conn.close()
    except Exception:
        total_raw = 2317  # snapshot count at export time

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Posts ingested",   f"{total_raw:,}")
    c2.metric("Signals detected", f"{len(signals_df):,}")
    c3.metric("Brands tracked",   str(len({b["brand"].split()[0] for b in brand_scores})))
    c4.metric("Breakouts",        str(breakouts))
    c5.metric("BUY_WATCH",        str(buy_watch))

    st.divider()
    col_l, col_r = st.columns([5, 3])

    with col_l:
        st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                    f"letter-spacing:0.1em;margin-bottom:16px'>Top brands by momentum</p>",
                    unsafe_allow_html=True)
        seen, top = set(), []
        for b in brand_scores:
            k = b["brand"].lower().split()[0]
            if k not in seen: seen.add(k); top.append(b)
            if len(top) == 10: break

        fig = go.Figure(go.Bar(
            x=[b["score"] for b in top],
            y=[b["brand"][:32] for b in top],
            orientation="h",
            marker_color=ACCENT,
            opacity=0.85,
            text=[f"  {b['score']:.0f}" for b in top],
            textposition="outside",
            textfont=dict(color=MUTED, size=11),
        ))
        fig.update_layout(
            height=300,
            yaxis=dict(autorange="reversed", tickfont=dict(size=11, color=MUTED),
                       gridcolor=BORDER, linecolor=BORDER),
            xaxis=dict(gridcolor=BORDER, tickfont=dict(color=MUTED), range=[0, 65]),
            **PLOTLY_BASE,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                    f"letter-spacing:0.1em;margin-bottom:16px'>Signal breakdown</p>",
                    unsafe_allow_html=True)
        tc = signals_df["signal_type"].value_counts()
        fig2 = go.Figure(go.Pie(
            labels=tc.index,
            values=tc.values,
            marker_colors=[SIGNAL_COLORS.get(t, "#aaa") for t in tc.index],
            hole=0.62,
            textinfo="percent",
            textfont=dict(size=11, color=SURFACE),
            insidetextorientation="radial",
        ))
        fig2.update_layout(
            height=220,
            legend=dict(font=dict(color=MUTED, size=11), orientation="v", x=1, y=0.5),
            margin=dict(l=0, r=80, t=10, b=10),
            **{k: v for k, v in PLOTLY_BASE.items() if k not in ("margin", "font")},
            font=dict(family="Inter", color=MUTED, size=11),
        )
        st.plotly_chart(fig2, use_container_width=True)

        # platform bar
        st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                    f"letter-spacing:0.1em;margin-top:4px'>Platform</p>",
                    unsafe_allow_html=True)
        for plat, cnt in signals_df["platform"].value_counts().items():
            pct = int(cnt / len(signals_df) * 100)
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:8px'>"
                f"<span style='color:{MUTED};font-size:12px;width:60px'>{plat}</span>"
                f"<div style='flex:1;background:{BORDER};border-radius:2px;height:4px'>"
                f"<div style='width:{pct}%;background:{ACCENT};border-radius:2px;height:4px'></div></div>"
                f"<span style='color:{MUTED};font-size:11px;width:36px;text-align:right'>{pct}%</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # latest signals grid
    st.divider()
    st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                f"letter-spacing:0.1em;margin-bottom:16px'>Latest high-confidence signals</p>",
                unsafe_allow_html=True)
    top_sigs = signals_df[signals_df["confidence"] >= 0.8].head(6)
    cols = st.columns(3)
    for i, (_, row) in enumerate(top_sigs.iterrows()):
        c = SIGNAL_COLORS.get(row["signal_type"], "#aaa")
        with cols[i % 3]:
            st.markdown(f"""
            <div style='background:{SURFACE};border:1px solid {BORDER};
                        border-top:2px solid {c};border-radius:6px;
                        padding:16px 18px;margin-bottom:12px;min-height:110px'>
              <div style='margin-bottom:8px'>{pill(row["signal_type"], c)}</div>
              <div style='color:{TEXT};font-weight:500;font-size:13px;margin-bottom:6px'>
                {str(row["brand_or_product"] or "")[:36]}
              </div>
              <div style='color:{MUTED};font-size:12px;font-style:italic;line-height:1.5'>
                "{str(row["trigger_phrase"] or "")[:65]}…"
              </div>
            </div>
            """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: TRENDING
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Trending":
    page_title("Trending Brands", "Ranked by real-time consumer momentum score")

    if not brand_scores:
        st.warning("Run score_brands.py first.")
        st.stop()

    seen, top = set(), []
    for b in brand_scores:
        k = b["brand"].lower().split()[0]
        if k not in seen: seen.add(k); top.append(b)
        if len(top) == 20: break

    if breakouts:
        st.markdown(f"""
        <div style='background:{SIGNAL_COLORS["OBSESSION"]}10;
                    border:1px solid {SIGNAL_COLORS["OBSESSION"]}30;
                    border-radius:6px;padding:14px 18px;margin-bottom:24px'>
          <span style='color:{SIGNAL_COLORS["OBSESSION"]};font-size:13px;font-weight:500;
                        letter-spacing:0.04em'>
            {breakouts} Breakout{'s' if breakouts > 1 else ''} detected — score jumped 15+ pts in 48 hrs
          </span>
        </div>
        """, unsafe_allow_html=True)

    # score cards row
    cols = st.columns(4)
    for i, b in enumerate(top[:8]):
        c = SIGNAL_COLORS.get(b.get("dominant_signal_type"), ACCENT)
        ticker = f" · {b['ticker']}" if b.get("ticker") else ""
        with cols[i % 4]:
            st.markdown(f"""
            <div style='background:{SURFACE};border:1px solid {BORDER};
                        border-top:2px solid {c};border-radius:6px;
                        padding:18px 20px;margin-bottom:14px;text-align:center'>
              <div style='font-size:10px;color:{MUTED};text-transform:uppercase;
                          letter-spacing:0.1em;margin-bottom:6px'>
                {b.get("dominant_signal_type") or "signal"}
              </div>
              <div style='color:{TEXT};font-weight:500;font-size:13px;margin-bottom:10px'>
                {b["brand"][:22]}{ticker}
              </div>
              <div style='font-family:Cormorant,serif;font-size:38px;font-weight:600;
                          color:{TEXT};line-height:1'>{b["score"]:.0f}</div>
              <div style='font-size:10px;color:{MUTED};margin-top:4px'>
                / 100 · {b["signal_count"]} signals
              </div>
              {'<div style="font-size:10px;color:' + SIGNAL_COLORS["OBSESSION"] + ';margin-top:6px;letter-spacing:0.06em">BREAKOUT</div>' if b.get("breakout") else ''}
            </div>
            """, unsafe_allow_html=True)

    st.divider()

    # full leaderboard
    st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                f"letter-spacing:0.1em;margin-bottom:16px'>Full leaderboard</p>",
                unsafe_allow_html=True)
    bar_colors = [
        SIGNAL_COLORS["OBSESSION"] if b.get("breakout")
        else SIGNAL_COLORS.get(b.get("dominant_signal_type"), ACCENT)
        for b in top[:20]
    ]
    fig = go.Figure(go.Bar(
        x=[b["score"] for b in top[:20]],
        y=[b["brand"][:34] for b in top[:20]],
        orientation="h",
        marker_color=bar_colors,
        opacity=0.75,
        text=[f"  {b['score']:.0f}" for b in top[:20]],
        textposition="outside",
        textfont=dict(color=MUTED, size=11),
    ))
    fig.update_layout(
        height=560,
        yaxis=dict(autorange="reversed", tickfont=dict(size=11, color=MUTED),
                   gridcolor=BORDER, linecolor=BORDER),
        xaxis=dict(gridcolor=BORDER, tickfont=dict(color=MUTED), range=[0, 65]),
        **PLOTLY_BASE,
    )
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: SIGNAL FEED
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Signal Feed":
    page_title("Signal Feed", f"{len(signals_df):,} signals — filter in real time")

    c1, c2, c3 = st.columns(3)
    sig_f  = c1.multiselect("Type", list(SIGNAL_COLORS.keys()), default=list(SIGNAL_COLORS.keys()))
    conf   = c2.slider("Min confidence", 0.0, 1.0, 0.65, step=0.05)
    plat_f = c3.multiselect("Platform", signals_df["platform"].unique().tolist(),
                             default=signals_df["platform"].unique().tolist())

    filt = signals_df[
        signals_df["signal_type"].isin(sig_f) &
        (signals_df["confidence"] >= conf) &
        signals_df["platform"].isin(plat_f)
    ]
    st.markdown(f"<p style='font-size:12px;color:{MUTED};margin-bottom:16px'>"
                f"Showing {len(filt)} signals</p>", unsafe_allow_html=True)

    for _, row in filt.head(60).iterrows():
        c = SIGNAL_COLORS.get(row["signal_type"], "#aaa")
        brand  = str(row["brand_or_product"] or "Unknown")
        ticker = (f"<code style='background:{BORDER};padding:1px 6px;border-radius:2px;"
                  f"font-size:11px;color:{MUTED}'>{row['ticker_hint']}</code>") \
                 if row.get("ticker_hint") else ""
        implication_html = (
            f"<div style='font-size:12px;color:{MUTED}'>{row['market_implication']}</div>"
            if row.get("market_implication") else ""
        )
        trigger_text = str(row["trigger_phrase"] or "")
        st.markdown(
            f"<div style='background:{SURFACE};border:1px solid {BORDER};"
            f"border-left:2px solid {c};border-radius:6px;"
            f"padding:14px 20px;margin-bottom:8px'>"
            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:8px'>"
            f"{pill(row['signal_type'], c)}"
            f"<span style='font-size:11px;color:{MUTED}'>{row['platform']} · conf {row['confidence']:.2f}</span>"
            f"</div>"
            f"<div style='color:{TEXT};font-weight:500;font-size:13px;margin-bottom:4px'>"
            f"{brand[:60]} {ticker}</div>"
            f"<div style='color:{MUTED};font-size:12px;font-style:italic;margin-bottom:4px'>"
            f"&ldquo;{trigger_text}&rdquo;</div>"
            f"{implication_html}"
            f"</div>",
            unsafe_allow_html=True,
        )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: GAP PANEL
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Gap Panel":
    page_title("Gap Panel", "Brands where consumer momentum outpaces institutional awareness")

    if not gap_scores:
        st.warning("Run gap_detection.py first.")
        st.stop()

    val_map = {v["ticker"]: v for v in validations}

    # scatter
    if len(gap_scores) > 1:
        df_gap = pd.DataFrame(gap_scores)
        fig = go.Figure()
        for status, color in STATUS_COLORS.items():
            sub = df_gap[df_gap["status"] == status]
            if sub.empty: continue
            fig.add_trace(go.Scatter(
                x=sub["institutional_score"], y=sub["consumer_score"],
                mode="markers+text",
                name=status,
                text=sub["ticker"],
                textposition="top center",
                textfont=dict(size=10, color=MUTED),
                marker=dict(size=12, color=color, opacity=0.7,
                            line=dict(color=SURFACE, width=1)),
            ))
        mx = max(df_gap["consumer_score"].max(), df_gap["institutional_score"].max()) + 8
        fig.add_trace(go.Scatter(
            x=[0, mx], y=[0, mx], mode="lines",
            line=dict(color=BORDER, dash="dot", width=1),
            showlegend=False, hoverinfo="skip",
        ))
        fig.update_layout(
            height=300,
            xaxis=dict(title="Institutional Awareness", gridcolor=BORDER, tickfont=dict(color=MUTED)),
            yaxis=dict(title="Consumer Momentum",       gridcolor=BORDER, tickfont=dict(color=MUTED)),
            legend=dict(font=dict(color=MUTED, size=11)),
            **PLOTLY_BASE,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(f"<p style='font-size:11px;color:{MUTED};margin-top:-8px;margin-bottom:24px'>"
                    f"Points above the dotted line indicate consumer signal ahead of institutions</p>",
                    unsafe_allow_html=True)

    for g in sorted(gap_scores, key=lambda x: x["gap_score"], reverse=True):
        status = g["status"]
        sc     = STATUS_COLORS.get(status, MUTED)
        val    = val_map.get(g["ticker"], {})
        action = val.get("recommended_action", "")
        ac     = ACTION_COLORS.get(action, MUTED)

        st.markdown(f"""
        <div style='background:{SURFACE};border:1px solid {BORDER};border-radius:6px;
                    padding:20px 24px;margin-bottom:10px'>
          <div style='display:flex;justify-content:space-between;align-items:center;
                      flex-wrap:wrap;gap:10px;margin-bottom:14px'>
            <div>
              <span style='color:{TEXT};font-size:16px;font-weight:500'>{g["brand"]}</span>
              <code style='background:{BORDER};color:{MUTED};padding:1px 8px;
                           border-radius:2px;font-size:11px;margin-left:8px'>{g["ticker"]}</code>
            </div>
            <div style='display:flex;gap:8px'>
              {pill(f"Gap {g['gap_score']:+.0f} · {status}", sc)}
              {pill(f"{action} · {val.get('validation_score', '—')}", ac) if action else ''}
            </div>
          </div>
          <div style='display:flex;gap:40px'>
            <div>
              <div style='font-size:10px;color:{MUTED};text-transform:uppercase;
                          letter-spacing:0.08em;margin-bottom:2px'>Consumer</div>
              <div style='font-family:Cormorant,serif;font-size:28px;color:{TEXT}'>{g["consumer_score"]:.0f}</div>
            </div>
            <div>
              <div style='font-size:10px;color:{MUTED};text-transform:uppercase;
                          letter-spacing:0.08em;margin-bottom:2px'>Institutional</div>
              <div style='font-family:Cormorant,serif;font-size:28px;color:{TEXT}'>{g["institutional_score"]:.0f}</div>
            </div>
            {'<div style="flex:1"><div style="font-size:10px;color:' + MUTED + ';text-transform:uppercase;letter-spacing:0.08em;margin-bottom:2px">Validation note</div><div style="font-size:12px;color:' + MUTED + ';line-height:1.5">' + str(val.get("reason",""))[:100] + '</div></div>' if val.get("reason") else ''}
          </div>
        </div>
        """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: BRIEFS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Briefs":
    page_title("Intelligence Briefs", "AI-generated research briefs for brands above score 40")

    if not briefs:
        st.warning("Run agent_investigate.py first.")
        st.stop()

    val_map = {v["ticker"]: v for v in validations}

    for b in briefs:
        ticker   = b.get("ticker") or "private"
        score    = b.get("score", 0)
        breakout = b.get("breakout", False)
        val      = val_map.get(b.get("ticker"), {})
        action   = val.get("recommended_action", "")
        ac       = ACTION_COLORS.get(action, MUTED)

        with st.expander(
            f"{b['brand']}  ·  {ticker}  ·  {score:.0f} / 100",
            expanded=(breakout or score >= 45),
        ):
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Momentum",      f"{score:.0f} / 100")
            mc2.metric("Reddit samples", b.get("reddit_sample_count", 0))
            mc3.metric("Headlines",      b.get("headline_count", 0))

            if breakout or action:
                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                row_a, row_b = st.columns(2)
                if breakout:
                    row_a.markdown(
                        f"<div style='background:{SIGNAL_COLORS['OBSESSION']}12;"
                        f"border:1px solid {SIGNAL_COLORS['OBSESSION']}30;border-radius:4px;"
                        f"padding:8px 14px;font-size:12px;color:{SIGNAL_COLORS['OBSESSION']}'>"
                        f"Breakout — score +15 pts in 48 hrs</div>",
                        unsafe_allow_html=True,
                    )
                if action:
                    row_b.markdown(
                        f"<div style='background:{ac}12;border:1px solid {ac}30;border-radius:4px;"
                        f"padding:8px 14px;text-align:center;font-size:12px;color:{ac};font-weight:500'>"
                        f"{action} · validation score {val.get('validation_score','—')}</div>",
                        unsafe_allow_html=True,
                    )

            st.divider()
            st.markdown(
                f"<div style='color:{TEXT};font-size:13px;line-height:1.8;'>"
                f"{b.get('brief','').replace(chr(10),'<br>')}</div>",
                unsafe_allow_html=True,
            )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: STOCKS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Stocks":
    page_title("Stock Correlation", "90-day price performance vs consumer signal timing")

    if not stock_data:
        st.warning("Run fetch_stocks.py first.")
        st.stop()

    # all-ticker bar
    st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                f"letter-spacing:0.1em;margin-bottom:16px'>90-day returns — all tickers</p>",
                unsafe_allow_html=True)
    rows_all = sorted(stock_data.items(), key=lambda x: x[1]["pct_change_90d"])
    fig_all = go.Figure(go.Bar(
        x=[t for t, _ in rows_all],
        y=[d["pct_change_90d"] for _, d in rows_all],
        marker_color=[
            SIGNAL_COLORS["DISCOVERY"] if d["pct_change_90d"] >= 0
            else SIGNAL_COLORS["OBSESSION"]
            for _, d in rows_all
        ],
        opacity=0.75,
        text=[f"{d['pct_change_90d']:+.1f}%" for _, d in rows_all],
        textposition="outside",
        textfont=dict(color=MUTED, size=10),
    ))
    fig_all.update_layout(
        height=280,
        xaxis=dict(tickfont=dict(size=11, color=MUTED), gridcolor=BORDER),
        yaxis=dict(title="90d Return %", gridcolor=BORDER, tickfont=dict(color=MUTED),
                   zeroline=True, zerolinecolor=BORDER),
        **PLOTLY_BASE,
    )
    st.plotly_chart(fig_all, use_container_width=True)
    st.divider()

    # deep dive
    st.markdown(f"<p style='font-size:11px;color:{MUTED};text-transform:uppercase;"
                f"letter-spacing:0.1em;margin-bottom:12px'>Deep dive</p>",
                unsafe_allow_html=True)
    selected = st.selectbox("", list(stock_data.keys()), label_visibility="collapsed")
    data     = stock_data[selected]
    weekly   = data.get("weekly_returns", [])

    m1, m2, m3 = st.columns(3)
    m1.metric("90d Return", f"{data['pct_change_90d']:+.1f}%",
              delta_color="normal" if data["pct_change_90d"] >= 0 else "inverse")
    m2.metric("Entry",   f"${data['start_price']}")
    m3.metric("Current", f"${data['end_price']}")

    ticker_brand_map = {b["ticker"]: b["brand"] for b in brand_scores if b.get("ticker")}
    brand_name = ticker_brand_map.get(selected, selected)
    brand_sigs = signals_df[
        signals_df["brand_or_product"].str.contains(brand_name.split()[0], case=False, na=False)
    ] if brand_name else pd.DataFrame()

    disc_color = SIGNAL_COLORS["DISCOVERY"]
    if not brand_sigs.empty:
        st.markdown(
            f"<div style='background:{disc_color}10;"
            f"border:1px solid {disc_color}30;border-radius:6px;"
            f"padding:12px 16px;margin:12px 0;font-size:13px;color:{disc_color}'>"
            f"{len(brand_sigs)} consumer signal(s) detected for {brand_name}</div>",
            unsafe_allow_html=True,
        )
        for _, sig in brand_sigs.iterrows():
            c = SIGNAL_COLORS.get(sig["signal_type"], MUTED)
            trigger = str(sig["trigger_phrase"] or "")
            st.markdown(
                f"&nbsp;&nbsp;{pill(sig['signal_type'], c)} "
                f"<span style='color:{MUTED};font-size:12px'>conf {sig['confidence']:.2f} · {sig['platform']}</span>"
                f" &mdash; <em style='color:{TEXT};font-size:12px'>&ldquo;{trigger}&rdquo;</em>",
                unsafe_allow_html=True,
            )
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    if weekly:
        fig2 = go.Figure(go.Bar(
            x=list(range(1, len(weekly)+1)),
            y=weekly,
            marker_color=[
                SIGNAL_COLORS["DISCOVERY"] if v >= 0 else SIGNAL_COLORS["OBSESSION"]
                for v in weekly
            ],
            opacity=0.7,
            text=[f"{v:+.1f}%" for v in weekly],
            textposition="outside",
            textfont=dict(size=9, color=MUTED),
        ))
        fig2.update_layout(
            title=dict(text=f"{selected} — Weekly Returns", font=dict(color=TEXT, size=13)),
            xaxis=dict(title="Week", gridcolor=BORDER, tickfont=dict(color=MUTED)),
            yaxis=dict(title="Return %", gridcolor=BORDER, tickfont=dict(color=MUTED),
                       zeroline=True, zerolinecolor=BORDER),
            height=280,
            **PLOTLY_BASE,
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    rows_tbl = []
    for t, d in stock_data.items():
        rows_tbl.append({
            "Ticker":    t,
            "90d":       f"{d['pct_change_90d']:+.1f}%",
            "Entry":     f"${d['start_price']}",
            "Current":   f"${d['end_price']}",
            "Signal":    "✓" if any(b.get("ticker") == t for b in brand_scores) else "—",
            "Gap":       next((g["status"] for g in gap_scores if g["ticker"] == t), "—"),
        })
    st.dataframe(pd.DataFrame(rows_tbl), use_container_width=True, hide_index=True)
