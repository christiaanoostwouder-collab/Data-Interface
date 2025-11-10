#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import time
from typing import Any, Dict, List, Optional
import httpx

POLYMARKET_API = "https://data-api.polymarket.com"
ETHERSCAN_API = "https://api.etherscan.io/v2/api"
ETHERSCAN_KEY = "EAGTY4DWJRYZUS2WX3WJZ67E5SZJ89W4KW"

CHAIN_ID = 137
TIMEOUT = 30.0
SLEEP = 0.2

WALLET = "0x8970b56535153baadee991ca25a178ac085636b5"
LIMIT = 10
MAX_PAGES = 50
CSV_PATH = "merged_with_fees.csv"
API_DELAY = 0.15

USE_COINGECKO = True
FALLBACK_PRICE = 0.50


# Convert hex to int
def hex_to_int(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    s = str(v)
    try:
        return int(s, 16) if s.startswith(("0x", "0X")) else int(s)
    except Exception:
        return None


# Convert wei to MATIC
def wei_to_matic(wei: Optional[int]) -> Optional[float]:
    return (wei / 1e18) if wei is not None else None


# Convert safely to float
def safe_float(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


# Generic HTTP GET (JSON)
def get_json(url: str, params: Dict[str, Any], timeout: float = TIMEOUT) -> Any:
    with httpx.Client(timeout=timeout) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()


# Get MATIC price in USDC (via CoinGecko)
def get_matic_price_usdc() -> float:
    if not USE_COINGECKO:
        return float(FALLBACK_PRICE)
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": "matic-network", "vs_currencies": "usd"}
        j = get_json(url, params, timeout=10.0)
        return float(j["matic-network"]["usd"])
    except Exception:
        return float(FALLBACK_PRICE)


# Fetch merged activity from Polymarket
def fetch_merged_activity(user: str, limit: int, max_pages: int) -> List[Dict[str, Any]]:
    out = []
    offset = 0
    for _ in range(max_pages):
        params = {"user": user, "type": "MERGE", "limit": min(limit, 500), "offset": offset}
        data = get_json(f"{POLYMARKET_API}/activity", params)
        if not data:
            break
        out.extend(data)
        offset += len(data)
        if len(data) < params["limit"]:
            break
        time.sleep(API_DELAY)
    return out


# Get transaction receipt from Etherscan
def get_tx_receipt(txhash: str) -> Optional[Dict[str, Any]]:
    params = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": txhash,
        "apikey": ETHERSCAN_KEY,
    }
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.get(ETHERSCAN_API, params=params)
        r.raise_for_status()
        return r.json().get("result")


# Get transaction details from Etherscan
def get_tx_data(txhash: str) -> Optional[Dict[str, Any]]:
    params = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": txhash,
        "apikey": ETHERSCAN_KEY,
    }
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.get(ETHERSCAN_API, params=params)
        r.raise_for_status()
        return r.json().get("result")


# Compute on-chain fee for Polygon transaction
def compute_fee(txhash: str) -> Dict[str, Any]:
    receipt = get_tx_receipt(txhash)
    time.sleep(SLEEP)

    if not receipt:
        return {"gasUsed": None, "effectiveGasPrice": None, "gasPrice": None,
                "feeWei": None, "feeMATIC": None, "note": "no_receipt"}

    gas_used = hex_to_int(receipt.get("gasUsed"))
    eff_price = hex_to_int(receipt.get("effectiveGasPrice"))
    used_price = eff_price
    gas_price_legacy = None
    note = "effectiveGasPrice"

    if used_price is None:
        tx = get_tx_data(txhash)
        time.sleep(SLEEP)
        gas_price_legacy = hex_to_int(tx.get("gasPrice")) if tx else None
        used_price = gas_price_legacy
        note = "fallback_gasPrice" if gas_price_legacy else "no_price_found"

    if gas_used is None or used_price is None:
        return {"gasUsed": gas_used, "effectiveGasPrice": eff_price,
                "gasPrice": gas_price_legacy, "feeWei": None,
                "feeMATIC": None, "note": note}

    fee_wei = gas_used * used_price
    return {
        "gasUsed": gas_used,
        "effectiveGasPrice": eff_price,
        "gasPrice": gas_price_legacy if eff_price is None else eff_price,
        "feeWei": fee_wei,
        "feeMATIC": wei_to_matic(fee_wei),
        "note": note,
    }


# Write data to CSV
def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    base = [
        "proxyWallet", "timestamp", "conditionId", "type", "size", "usdcSize", "transactionHash",
        "price", "asset", "side", "outcomeIndex", "title", "slug", "icon", "eventSlug",
        "outcome", "name", "pseudonym", "bio", "profileImage", "profileImageOptimized"
    ]
    extra = ["gasUsed", "effectiveGasPrice", "gasPrice", "feeWei",
             "feeMATIC", "maticPriceUSDC", "feeUSDC", "netUSDC_after_fee", "fee_note"]
    fields = base + [f for f in extra if f not in base]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


# Main execution
def main() -> None:
    if ETHERSCAN_KEY == "YOUR_ETHERSCAN_API_KEY_HERE":
        print("Please add your Etherscan API key.")
        return

    matic_price = get_matic_price_usdc()
    print(f"[info] MATIC price: {matic_price:.4f} USDC")

    merges = fetch_merged_activity(WALLET, LIMIT, MAX_PAGES)
    print(f"[info] Merged activities: {len(merges)}")

    enriched = []
    tx_cache: Dict[str, Dict[str, Any]] = {}
    total_matic = 0.0
    total_usdc = 0.0
    tx_count = 0

    for i, m in enumerate(merges, start=1):
        tx = m.get("transactionHash") or m.get("txHash") or m.get("transaction_hash")
        row = dict(m)

        if not tx:
            row.update({
                "feeMATIC": None, "feeUSDC": None,
                "netUSDC_after_fee": None, "fee_note": "no_txhash"
            })
            enriched.append(row)
            continue

        if tx in tx_cache:
            fee = tx_cache[tx]
        else:
            print(f"[{i}/{len(merges)}] Fetching fee: {tx}")
            try:
                fee = compute_fee(tx)
            except Exception as e:
                fee = {"feeMATIC": None, "note": f"error:{e}"}
            tx_cache[tx] = fee

        fee_matic = safe_float(fee.get("feeMATIC"))
        fee_usdc = fee_matic * matic_price if fee_matic is not None else None
        usdc_size = safe_float(row.get("usdcSize"))
        net_after = (usdc_size - fee_usdc) if (usdc_size and fee_usdc) else None

        if fee_matic is not None:
            total_matic += fee_matic
            total_usdc += fee_usdc or 0
            tx_count += 1

        row.update({
            "feeMATIC": fee_matic,
            "feeUSDC": fee_usdc,
            "netUSDC_after_fee": net_after,
            "maticPriceUSDC": matic_price,
            "fee_note": fee.get("note"),
        })
        enriched.append(row)

    write_csv(enriched, CSV_PATH)
    print(f"[ok] CSV saved → {CSV_PATH}")

    # Summary
    print("\n========== FEE SUMMARY ==========")
    print(f"Transactions with fee: {tx_count}")
    print(f"Total gas: {total_matic:.6f} MATIC ({total_usdc:.6f} USDC)")
    if tx_count:
        avg_m = total_matic / tx_count
        avg_u = total_usdc / tx_count
        print(f"Average fee: {avg_m:.6f} MATIC ({avg_u:.6f} USDC)")
    print("=================================\n")


if __name__ == "__main__":
    main()
