import os, io, asyncio, logging
from typing import List, Dict
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from data_providers import fetch_ohlcv_multi, parse_lookback, supported_timeframes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("compare-bot")

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_SPOT = os.getenv("SPOT_EXCHANGES", "binance")
DEFAULT_FUT = os.getenv("FUTURES_EXCHANGES", "binanceusdm")
TZ = os.getenv("TZ", "Europe/Riga")
if not BOT_TOKEN: raise SystemExit("Не задан BOT_TOKEN в .env")

def parse_kv_params(parts: List[str]) -> Dict[str, str]:
    out = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1); out[k.strip().lower()] = v.strip()
    return out

def render_chart(combined: Dict[str, pd.DataFrame], title: str, tz: str) -> bytes:
    plt.close("all"); fig = plt.figure(figsize=(12,7), dpi=150)
    ax_price = fig.add_axes([0.08, 0.42, 0.9, 0.5]); ax_vol = fig.add_axes([0.08, 0.08, 0.9, 0.28], sharex=ax_price)
    for sym, df in combined.items():
        if df.empty: continue
        idx = df.index
        if "close_spot_idx" in df: ax_price.plot(idx, df["close_spot_idx"], label=f"{sym} spot")
        if "close_fut_idx" in df:  ax_price.plot(idx, df["close_fut_idx"], linestyle="--", label=f"{sym} fut")
        if "volume_total" in df:   ax_vol.bar(idx, df["volume_total"], align="center")
    ax_price.set_ylabel("Price index (start=100)"); ax_price.legend(loc="best", fontsize=9); ax_price.grid(True, alpha=0.3)
    ax_vol.set_ylabel("Volume (spot+fut)"); ax_vol.grid(True, axis="y", alpha=0.3)
    fig.suptitle(title, y=0.98, fontsize=14); ax_vol.tick_params(axis="x", rotation=30)
    buf = io.BytesIO(); fig.savefig(buf, format="png", bbox_inches="tight"); buf.seek(0); return buf.read()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Бот сравнения крипто‑графиков (спот + фьючерсы)\n\n"
        "Команда:\n/compare <tickers> [параметры]\n\n"
        "Примеры:\n• /compare BTC/USDT,ETH/USDT timeframe=1h lookback=7d\n"
        "• /compare SOL/USDT timeframe=4h lookback=30d ex_spot=okx,binance ex_fut=binanceusdm\n\n"
        "Параметры:\n• timeframe=1m..1d\n• lookback=12h|7d|30d|12w|24m\n• ex_spot=binance,okx\n• ex_fut=binanceusdm,bybit\n• title=Заголовок"
    )

async def compare(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    parts = update.message.text.strip().split()
    if len(parts) < 2:
        await update.message.reply_text("Укажите тикеры, пример: /compare BTC/USDT,ETH/USDT timeframe=1h lookback=7d"); return
    tickers = [t.strip().upper() for t in parts[1].split(",") if t.strip()]
    params = parse_kv_params(parts[2:])
    timeframe = params.get("timeframe","1h")
    if timeframe not in supported_timeframes():
        await update.message.reply_text(f"Неподдерживаемый timeframe={timeframe}. Доступны: {', '.join(supported_timeframes())}"); return
    lb_str = params.get("lookback","7d"); lb_ms = parse_lookback(lb_str)
    if lb_ms is None:
        await update.message.reply_text("Некорректный lookback. Примеры: 12h, 7d, 30d, 12w."); return
    ex_spot = [e.strip() for e in params.get("ex_spot", DEFAULT_SPOT).split(",") if e.strip()]
    ex_fut  = [e.strip() for e in params.get("ex_fut",  DEFAULT_FUT ).split(",") if e.strip()]
    title = params.get("title", f"Сравнение: {', '.join(tickers)} | {timeframe} | {lb_str}")
    await update.message.reply_text(f"Собираю данные…\nСпот: {', '.join(ex_spot)}\nФьючерсы: {', '.join(ex_fut)}\nTF: {timeframe}, период: {lb_str}")
    try:
        combined = {}
        import io
        csv_buf = io.StringIO()
        for sym in tickers:
            df = await fetch_ohlcv_multi(sym, timeframe, lb_ms, ex_spot, ex_fut)
            if df.empty:
                await update.message.reply_text(f"Нет данных для {sym}"); continue
            df = df.sort_index()
            if "close_spot" in df and df["close_spot"].notna().any():
                first = df["close_spot"].dropna().iloc[0]; df["close_spot_idx"] = df["close_spot"]/first*100 if first else float('nan')
            if "close_fut" in df and df["close_fut"].notna().any():
                firstf = df["close_fut"].dropna().iloc[0]; df["close_fut_idx"] = df["close_fut"]/firstf*100 if firstf else float('nan')
            df["volume_total"] = df.get("volume_spot",0).fillna(0) + df.get("volume_fut",0).fillna(0)
            combined[sym] = df
            tmp = df.copy(); tmp["symbol"]=sym; tmp.reset_index(names=["timestamp"], inplace=True)
            tmp.to_csv(csv_buf, index=False, header=csv_buf.tell()==0)
        if not combined:
            await update.message.reply_text("Не удалось собрать данные ни для одного тикера."); return
        png = render_chart(combined, title, TZ); import io
        pbuf = io.BytesIO(png); pbuf.name="compare.png"
        cbuf = io.BytesIO(csv_buf.getvalue().encode("utf-8")); cbuf.name="data.csv"
        await update.message.reply_photo(photo=pbuf, caption=title)
        await update.message.reply_document(document=cbuf, caption="Выровненные ряды (CSV)")
    except Exception as e:
        logging.exception("Compare failed"); await update.message.reply_text(f"Ошибка: {e}")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("compare", compare))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
