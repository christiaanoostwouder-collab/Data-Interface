
import csv
import httpx
from datetime import datetime, timedelta, timezone
from typing import Any, List, Dict, Tuple
from zoneinfo import ZoneInfo
from collections import defaultdict


DATA_API = "https://data-api.polymarket.com"
# choose conditionId or EventId
CONDITION_ID = ""
EVENT_ID = "57489"
# choose date 
FROM_DATE = "2025-10-15"  
TO_DATE   = "2025-10-17"
# filter by role: "maker" , "taker" or "all"
ROLE = "all"
LIMIT = 500 # max 500
# total max pages to fetch
MAX_PAGES = 20  # 10 * 100 = 1000 recent trades
CSV_PATH = "trades.csv"
TOPS_PATH = "tops.csv"



TZ = ZoneInfo("Europe/Amsterdam")  

def parse_date_local(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=TZ, hour=0, minute=0, second=0, microsecond=0)

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


# fetch trades pages
def get_trades_pages(cond_id: str,
                     event_id: str | None,
                     limit: int,
                     role: str = "maker",
                     max_pages: int = 60) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    cursor = None
    offset = 0
    pages = 0

    if cond_id and event_id:
        raise ValueError("Use either cond_id or event_id, not both.")

    with httpx.Client(timeout=30) as client:
        while True:
            params: Dict[str, Any] = {"limit": limit}

            if cond_id:
                params["market"] = cond_id
            elif event_id:
                params["eventId"] = event_id
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


def detect_wallet_keys(trade: dict[str, Any]) -> Tuple[str | None, str | None]:
    keys = trade.keys()
    if "maker" in keys and "taker" in keys:
        return (None, "maker_taker")
    for k in ("proxyWallet", "wallet", "user", "account", "trader", "userAddress", "traderAddress", "address", "owner", "pseudonym"):
        if k in keys:
            return (k, None)
    return (None, None)

def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default
    
def add_agg(agg, wallet, role, size, price, side):
    notional = size * price
    a = agg[wallet]
    a["trades"] += 1
    a["gross_size"] += size
    a ["gross_notional"] += notional
    if role == "maker":
        a["trades_maker"] += 1
        a["gross_notional_maker"] += notional
    elif role == "taker":
        a["trades_taker"] += 1
        a["gross_notional_taker"] += notional
    if side:
        s = str(side).lower()
        if "buy" in s or s == "yes":
            a ["buy_count"] += 1
            a ["buy_notional"] += notional
        elif "sell" in s or s == "no":
            a ["sell_count"] += 1
            a ["sell_notional"] += notional

def aggregate_wallets(trades: List[Dict[str, Any]]):
    agg = defaultdict(lambda: {
        "trades": 0,
        "trades_maker": 0,
        "trades_taker": 0,
        "gross_size": 0.0,
        "gross_notional": 0.0,
        "gross_notional_maker": 0.0,
        "gross_notional_taker": 0.0,
        "buy_count": 0,
        "buy_notional": 0.0,
        "sell_count": 0,
        "sell_notional": 0.0,
    })

    single_key = None
    dual_mode = None
    for t in trades:
        single_key, dual_mode = detect_wallet_keys(t)
        if single_key or dual_mode:
            break

    for t in trades:
        size = to_float(t.get("size"))
        price = to_float(t.get("price"))
        side = t.get("side") or t.get("outcome") or t.get("type")
        if ("maker" in t) and ("taker" in t):
            mk = t.get("maker"); tk = t.get("taker")
            if mk: add_agg(agg, mk, "maker", size, price, side)
            if tk: add_agg(agg, tk, "taker", size, price, side)
            continue
        if single_key and single_key in t:
            w = t.get(single_key)
            if not w:
                continue
            role = None
            lr = t.get("liquidityRole") or t.get("role")
            if isinstance(lr, str):
                lr = lr.lower()
                if "maker" in lr:
                    role = "maker"
                elif "taker" in lr:
                    role = "taker"
            if role is None:
                if t.get("taker") is True or t.get("isTaker") is True: role = "taker"
                elif t.get("maker") is True or t.get("isMaker") is True: role = "maker"
            add_agg(agg, w, role, size, price, side)
            continue

        for k in ("wallet", "user", "account", "trader", "address", "owner"):
            if k in t and t.get(k):
                add_agg(agg, t[k], None, size, price, side)
                break
    return agg

# main routine

if __name__ == "__main__":
    # calculate date windows in UTC
    windows = day_windows_local_to_utc(FROM_DATE, TO_DATE)
    for i, (s, e) in enumerate(windows, start=1):
        print(f"Window {i}: UTC {s} → {e} | "
              f"{datetime.fromtimestamp(s, tz=timezone.utc)} → {datetime.fromtimestamp(e, tz=timezone.utc)}")

    # check endpoint of conditionId
    with httpx.Client(timeout=20) as client:
        probe_params: Dict[str, Any] = {"limit": 10}
        if CONDITION_ID:
            probe_params["market"] = CONDITION_ID
        elif EVENT_ID:
            probe_params["eventId"] = EVENT_ID
        
        if ROLE == "taker":
            probe_params["takerOnly"] = True
        else:
            probe_params["takerOnly"] = False
            if ROLE == "maker":
                probe_params["makerOnly"] = True

        probe = client.get(f"{DATA_API}/trades", params=probe_params)
        print("probe", probe.status_code)
        if probe.status_code == 200:
            payload = probe.json()
            if isinstance(payload, dict):
                cnt = len(payload.get("results") or payload.get("data") or [])
            elif isinstance(payload, list):
                cnt = len(payload)
            print ("probe rows:" , cnt)

    # fetch recent pages of trades
    recent_trades = get_trades_pages(cond_id = CONDITION_ID,
                                     event_id = EVENT_ID,                                     
                                     limit=LIMIT,
                                     role=ROLE,
                                     max_pages=MAX_PAGES)
    print(f"Fetched recent trades: {len(recent_trades)}")

    # filter by date windows
    filtered = filter_trades_by_windows(recent_trades, windows)
    print(f"Kept after time filter: {len(filtered)}")


    # choose data to aggregate
    records = filtered if filtered else recent_trades
    if not records:
        print("no trades to aggregate")
    else:
        print(f"aggregate {len(records)} trades")

    print("sample keys:", sorted(list(records[0].keys()))[:25])

    # aggregate by wallet
    wallet_agg = aggregate_wallets(records)

    print("unique wallets aggregated:", len(wallet_agg))

    # leaderboard
    top_by_trades = sorted(wallet_agg.items(), key = lambda kv: kv[1]["trades"], reverse = True)[:10]
    top_by_notional = sorted(wallet_agg.items(), key = lambda kv: kv[1]["gross_notional"], reverse = True)[:10]

    print("\nTop 10 by number of trades:")
    for w, a in top_by_trades:
        print(f"{w:>42}  trades = {a['trades']:4d} "
              f"gross_notional = ${a['gross_notional']:.2f} "
              f"gross_size = {a['gross_size']:.4f}")
        
    print("\ntop 10 by gross notional:")
    for w, a in top_by_notional:
        print(f"{w:>42}  gross_notional=${a['gross_notional']:.2f}  trades={a['trades']:4d}  ")

    #wallet with biggest volume
    if wallet_agg:
        top_wallet, top_stats = max(wallet_agg.items(), key=lambda kv: kv[1]["gross_notional"])
        total_notional = sum(a.get("gross_notional", 0.0)for a in wallet_agg.values())
        share_pct = (top_stats["gross_notional"] / total_notional * 100.0) if total_notional else 0.0

        print(f"share_of_total={share_pct:.2f}%")


    # save to csv
    if filtered:
        headers = sorted({k for row in filtered for k in row.keys()})
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            w.writerows(filtered)
        print(f"Saved {len(filtered)} trades to {CSV_PATH}")
    else:
        print("No trades in the selected date windows.")
