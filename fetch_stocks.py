import json
import yfinance as yf
from datetime import datetime, timedelta

TICKERS = ["YETI", "FRCOY", "TMUS", "JNJ", "PEP", "LOGI", "SONY", "KDP",
           "LULU", "NKE", "AAPL", "NVDA", "CMG", "MSFT", "VZ", "QSR"]

END   = datetime.today()
START = END - timedelta(days=90)

def fetch_ticker(ticker: str) -> dict | None:
    try:
        df = yf.download(ticker, start=START.strftime("%Y-%m-%d"),
                         end=END.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if df.empty:
            return None

        close = df["Close"]
        # yfinance v0.2+ returns MultiIndex columns when downloading single ticker
        if hasattr(close, "columns"):
            close = close.iloc[:, 0]
        prices = close.dropna()
        start_price = float(prices.iloc[0])
        end_price   = float(prices.iloc[-1])
        pct_change  = round((end_price - start_price) / start_price * 100, 2)

        # weekly returns for trend detection
        weekly = prices.resample("W").last().pct_change().dropna()
        weekly_returns = [round(float(v) * 100, 2) for v in weekly]

        return {
            "ticker": ticker,
            "start_price": round(start_price, 2),
            "end_price":   round(end_price, 2),
            "pct_change_90d": pct_change,
            "weekly_returns": weekly_returns,
            "data_points": len(prices),
        }
    except Exception as e:
        print(f"  [!] {ticker}: {e}")
        return None


def run():
    print(f"Fetching 90-day price data for {len(TICKERS)} tickers...\n")
    results = {}
    for ticker in TICKERS:
        data = fetch_ticker(ticker)
        if data:
            arrow = "▲" if data["pct_change_90d"] >= 0 else "▼"
            print(f"  {ticker:<8} {arrow} {data['pct_change_90d']:>+6.1f}%   "
                  f"${data['start_price']} → ${data['end_price']}")
            results[ticker] = data
        else:
            print(f"  {ticker:<8} — no data")

    with open("stock_data.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved {len(results)} tickers → stock_data.json")


if __name__ == "__main__":
    run()
