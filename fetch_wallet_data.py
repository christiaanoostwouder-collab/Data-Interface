import os
import math
import csv
import httpx
import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# fill in the wallet you want trades off
WALLET = "0x0540f430df85c770e0a4fb79d8499d71ebc298eb"

DATA_API = os.getenv("POLY_API_BASE", "https://data-api.polymarket.com")


# turns timestamp into a date
TZ = ZoneInfo("Europe/Amsterdam") if ZoneInfo else None
def parse_date_local(d: str) -> datetime:
    if not TZ:
        return datetime.strptime(d, "%Y-%m-%d").replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo = TZ, hour = 0, minute = 0, second = 0, microsecond = 0)

def day_windows_local_to_utc(from_date: str, to_date: str) -> List[Tuple[int, int]]:
    start_local = parse_date_local(from_date)
    end_local_excl = parse_date_local(to_date) + timedelta(days=1)
    out: List[Tuple[int, int]] = []
    cur = start_local
    while cur < end_local_excl:
        nxt = min(cur + timedelta(days=1), end_local_excl)
        start_ts_utc = int(cur.astimezone(timezone.utc).timestamp())
        end_ts_utc   = int(nxt.astimezone(timezone.utc).timestamp())
        out.append((start_ts_utc, end_ts_utc))
        cur = nxt
    return out

# get client and trades
def get_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(30.0, connect=10.0, read=20.0),
        headers={"Accept": "application/json"},
        http2=False,  
    )

def get_trades_pages(*,
                     cond_id: str | None,
                     event_id: str | None,
                     limit: int,
                     role: str = "maker",
                     max_pages: int = 20,
                    user_wallet: str | None = None) -> List[Dict[str, Any]]:
    
    if cond_id and event_id:
        raise ValueError("use ether cond_id or event_id, not both")
    rows: List[Dict[str, Any]] = []
    cursor = None
    offset = 0
    pages = 0

    with get_client() as client:
        while True:
            params: Dict[str, Any] = {"limit": limit}
            if cond_id:
                params["market"] = cond_id
            elif event_id:
                params["eventId"] = event_id

            if user_wallet:
                params["user"] = user_wallet.lower()
        
        # send maximum one of makerOnly/takerOnly
            if role == "taker":
                params["takerOnly"] = True
            else:
                params["takerOnly"] = False
                if role == "maker":
                    params["makerOnly"] = True

            # "all" means no filter

            # paginate
            if cursor is not None:
                params["cursor"] = cursor
            else:
                params["offset"] = offset

            # call API/make request
            r = client.get(f"{DATA_API}/trades", params=params)
            if r.status_code != 200:
                print("HTTP error:", r.status_code, r.text[:300])
                r.raise_for_status()

            data = r.json()

            # dict with results + nextCursor
            if isinstance(data, dict):
                batch = data.get("results") or data.get("data") or []
                if not batch:
                    break
                rows.extend(batch)
                cursor = data.get("nextCursor") or data.get("next") or data.get("cursor")
                pages += 1
                if not cursor:
                    break
                if pages >= max_pages:
                    break
                continue

            # plain list
            if isinstance(data, list):
                if not data:
                    break
                rows.extend(data)
                pages += 1
                if len(data) < limit:
                    break
                offset += limit
                if pages >= max_pages:
                    break
                continue

            break

    return rows

# take the timestamp from trades
def extract_ts_seconds(trade: Dict[str, Any]) -> int | None:
    ts = trade.get("timestamp") or trade.get("ts") or trade.get("time")
    if ts is None:
        return None
    try:
        t = int(ts)
        if t > 2 * 10**10:
            t = t // 1000
        return t
    except Exception:
        return None
# filter trades by our chosen date
def filter_trades_by_windows(trades: List[Dict[str, Any]],
                             windows: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in trades:
        ts = extract_ts_seconds(t)
        if ts is None:
            continue
        for start_ts, end_ts in windows:
            if start_ts <= ts < end_ts:
                out.append(t)
                break
    return out

def normalize_trade_record(t: Dict[str, Any]) -> Dict[str, Any]:
    market_id = (
        t.get("conditionId")
        or t.get("market")
        or t.get("marketId")
        or t.get("market_id")
        or t.get("eventId")
    )

    # title is optional
    market_title = (
        t.get("title")
        or t.get("marketTitle")
        or t.get("marketName")
        or t.get("label")
    )

    
    outcome_lbl = t.get("outcome") or t.get("outcomeLabel") or t.get("side")
    outcome_id = (
        t.get("outcomeToken")
        or t.get("outcomeId")
        or t.get("outcome_id")
        or t.get("outcomeIndex")
        or (outcome_lbl.upper() if isinstance(outcome_lbl, str) else None)
    )

    # price & size
    price = t.get("price")
    size  = t.get("size") or t.get("quantity") or t.get("amount")
    price = float(price) if price is not None else None
    size  = float(size)  if size  is not None else None

    ts = extract_ts_seconds(t)

    # wallet views in different payloads
    proxy = t.get("proxyWallet") or t.get("wallet") or t.get("user")
    maker = t.get("maker") or t.get("makerAddress")
    taker = t.get("taker") or t.get("takerAddress")
    buyer = t.get("buyer")
    seller = t.get("seller")

    side = t.get("side") or t.get("type") or t.get("action") or t.get("outcome")
    if isinstance(side, str):
        side = side.lower()  # BUY/SELL

    return {
        "market_id": market_id,
        "market_title": market_title,
        "outcome_id": outcome_id,
        "outcome_label": outcome_lbl,
        "price": price,
        "size": size,
        "ts": ts,
        "user": str(proxy).lower() if proxy else None,    
        "maker": str(maker).lower() if maker else None,
        "taker": str(taker).lower() if taker else None,
        "buyer": str(buyer).lower() if buyer else None,
        "seller": str(seller).lower() if seller else None,
        "side": side,
        "raw": t,
    }


def signed_quantity_for_wallet(n: Dict[str, Any], wallet: str) -> float:
    size = n.get("size")
    if not size:
        return 0.0

    wl = wallet.lower()
    side  = n.get("side")

    # proxywallet user
    user = n.get("user")
    if user:
        if wl == user:
            if side in ("buy", "yes"):
                return +size
            if side in ("sell", "no"):
                return -size
        return 0.0

    # buyer/seller
    buyer = n.get("buyer"); seller = n.get("seller")
    if buyer or seller:
        if wl == buyer:
            return +size
        if wl == seller:
            return -size

    # maker/taker
    maker = n.get("maker"); taker = n.get("taker")
    if (maker or taker) and side:
        if side in ("buy", "yes"):
            if wl == taker:
                return +size
            if wl == maker:
                return -size
        if side in ("sell", "no"):
            if wl == taker:
                return -size
            if wl == maker:
                return +size

    return 0.0

#puting together the data to aggregate delta
def aggregate_delta_for_wallet(trades: List[Dict[str, Any]], wallet: str) -> List[Dict[str, Any]]:
    bucket: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for t in trades:
        n = normalize_trade_record(t)
        if not n.get("market_id") or not n.get("outcome_id"):
            continue
        
        dq = signed_quantity_for_wallet(n, wallet)
        if dq == 0.0:
            continue

        key = (str(n["market_id"]), str(n["outcome_id"]))
        b = bucket.get(key)
        if not b:
           b = {
                "market_id": str(n["market_id"]),
                "market_title": n.get("market_title"),
                "outcome_id": str(n["outcome_id"]),
                "outcome_label": n.get("outcome_label"),
                "net_shares": 0.0,
                "fills": 0,
                "avg_fill_price_sum": 0.0,
        }
        bucket[key] = b

        b["net_shares"] += dq
        if n.get("price") is not None:
            b["avg_fill_price_sum"] += float(n["price"])
        b["fills"] += 1
    
    out: List[Dict[str, Any]] = []
    for _, rec in bucket.items():
        fills = max(rec["fills"], 1)
        rec["avg_fill_price"] = rec["avg_fill_price_sum"] / fills
        del rec["avg_fill_price_sum"]
        out.append(rec)
    return out

def fetch_market_prices(market_ids: List[str]) -> Dict[str, Dict[str, float]]:
    prices: Dict[str, Dict[str, float]] = {}
    with get_client() as client:
        for mid in market_ids:
            r = client.get(f"{DATA_API}/markets/{mid}")
            if r.status_code != 200:
                continue
            data = r.json()
            outs = data.get("outcomes") or data.get("conditions") or []
            pm: Dict[str, float] = {}
            for o in outs:
                oid = o.get("id") or o.get("outcomeId")
                p = o.get("price") or o.get("lastPrice") or o.get("probability")
                if oid is None or p is None:
                    continue
                pm [str(oid)] = float(p)
            prices[str(mid)] = pm
        return prices
    
def mark_to_market(rows: List[Dict[str, Any]], price_map: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        mid = str(r["market_id"]); oid = str(r["outcome_id"])
        cp = price_map.get(mid, {}).get(oid, math.nan)
        mv = r["net_shares"] * cp if not math.isnan(cp) else math.nan
        out.append({**r, "current_price": cp, "mark_value": mv})
    return out


def main():
    import argparse
    p = argparse.ArgumentParser(description="Fetch wallet delta/positions for Polymarket trades.")
    p.add_argument("--event", dest="event_id", default=None, help="EventId om trades uit te lezen.")
    p.add_argument("--condition", dest="cond_id", default=None, help="ConditionId/market om trades uit te lezen.")
    p.add_argument("--from", dest="from_date", default=None, help="Startdatum (YYYY-MM-DD) lokaal (Europe/Amsterdam).")
    p.add_argument("--to", dest="to_date", default=None, help="Einddatum (YYYY-MM-DD) lokaal (inclusief).")
    p.add_argument("--role", choices=["maker","taker","all"], default="all", help="Filter maker/taker indien ondersteund.")
    p.add_argument("--limit", type=int, default=250, help="Per page limit (API max ~500).")
    p.add_argument("--pages", type=int, default=20, help="Max pages to fetch.")
    p.add_argument("--csv", default="wallet_positions.csv", help="Pad voor CSV output.")
    args = p.parse_args()

    # only WALLET
    wallet = (WALLET or "").strip().lower()
    if not wallet:
        print("WALLET is leeg. Vul bovenaan fetch_wallet_data.py jouw 0x-wallet in.")
        return
    print(f"🔎 Using wallet: {wallet}")

    # get trades
    trades = get_trades_pages(
        cond_id=args.cond_id,
        event_id=args.event_id,
        limit=args.limit,
        role=args.role,
        max_pages=args.pages,
        user_wallet=wallet,
    )
    print(f"Fetched trades (raw): {len(trades)}")

    # filtering time
    if args.from_date and args.to_date:
        windows = day_windows_local_to_utc(args.from_date, args.to_date)
        trades = filter_trades_by_windows(trades, windows)
        print(f"After date filter: {len(trades)}")

    if not trades:
        print("No trades found.")
        return

    # aggregate position
    positions = aggregate_delta_for_wallet(trades, wallet)
    if not positions:
        print("No positions in wallet")
        return

    # current price
    market_ids = sorted({p["market_id"] for p in positions if p.get("market_id")})
    price_map = fetch_market_prices(market_ids)
    positions = mark_to_market(positions, price_map)

    # Output
    import math as _math
    positions.sort(key=lambda r: (0 if _math.isnan(r.get("mark_value", _math.nan)) else r.get("mark_value")), reverse=True)

    print("\n== Positions / Delta ==")
    header = ["market_id","market_title","outcome_id","outcome_label","net_shares","fills","avg_fill_price"]
    print(",".join(header))
    for r in positions:
        row = [str(r.get(k,"")) for k in header]
        print(",".join(row))

    # CSV output
    try:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            import csv as _csv
            w = _csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for r in positions:
                w.writerow({k: r.get(k, "") for k in header})
        print(f"\nCSV saved: {args.csv}")
    except Exception as e:
        print("CSV failed to write:", e)



if __name__ == "__main__":
    try:
        print("fetch_wallet_data: booting…", flush=True)
        main()
        print("fetch_wallet_data: done.", flush=True)
    except Exception:
        import traceback
        traceback.print_exc()
