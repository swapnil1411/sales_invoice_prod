# transformer.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Maps Mirakl Order/Refund XML into SIMPLE Mirakl JSON payloads.

Public API:
    map_mirakl_xml_to_template(xml_text: str, mode: str) -> dict
        mode = "order"  -> {"orders":  [ {...} ]}
        mode = "refund" -> {"refunds": [ {...} ]}

    transform_payload(folder_key: str, xml_text: str) -> dict | None
        folder_key in {"mirakl-order", "mirakl-refund"} -> returns filled payload
        else -> None (caller should write original payload)

Supported XML shapes:
  1) Sterling-like invoice:
     InvoiceDetail/InvoiceHeader/...
  2) Mirakl order feed body:
     <body><orders><order>...</order></orders></body>
  3) MiraklOrderRefund wrapper:
     <MiraklOrderRefund><Order>...</Order></MiraklOrderRefund>
     <MiraklOrderRefund><Refund>...</Refund></MiraklOrderRefund>
"""
from __future__ import annotations

import json, re
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import xml.etree.ElementTree as ET


# ===================== XML helpers (namespace-agnostic) =====================

def _local(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.rsplit(":", 1)[-1]
    return tag

def _children_by_local(el: ET.Element, name: str) -> List[ET.Element]:
    return [ch for ch in list(el) if _local(ch.tag) == name]

def _find_first(root: ET.Element, path: str) -> Optional[ET.Element]:
    segs = [s for s in path.strip().strip("/").split("/") if s]
    cur = [root]
    for seg in segs:
        nxt = []
        for node in cur:
            nxt.extend(_children_by_local(node, seg))
        if not nxt:
            return None
        cur = nxt
    return cur[0] if cur else None

def _find_all(root: ET.Element, path: str) -> List[ET.Element]:
    segs = [s for s in path.strip().strip("/").split("/") if s]
    cur = [root]
    for seg in segs:
        nxt = []
        for node in cur:
            nxt.extend(_children_by_local(node, seg))
        if not nxt:
            return []
        cur = nxt
    return cur

def _text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""


# ===================== Value helpers =====================

_NUM_RE = re.compile(r"^[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?$")

def _to_decimal(s: str) -> Optional[Decimal]:
    if s is None:
        return None
    s = str(s).strip()
    if not s or not _NUM_RE.match(s):
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def _sum_amounts_str(values: List[str], abs_value: bool) -> str:
    total = Decimal("0")
    for v in values:
        d = _to_decimal(v)
        if d is not None:
            total += d
    if abs_value:
        total = abs(total)
    return f"{total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"

def _to_iso8601_utc(x: str) -> str:
    """
    Normalize input date/time into ISO-8601 UTC with offset '+00:00'.
    Accepts:
      - epoch ms (>=13 digits)
      - epoch s (10 digits)
      - YYYYMMDD (8 digits) -> midnight UTC
      - ISO strings with 'Z' or timezone offset
    """
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""

    if s.isdigit():
        try:
            if len(s) >= 13:  # epoch ms
                ms = int(s[:13])
                dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
                return dt.replace(microsecond=0).isoformat().replace("+00:00", "+00:00")
            if len(s) == 10:  # epoch seconds
                sec = int(s)
                dt = datetime.fromtimestamp(sec, tz=timezone.utc)
                return dt.replace(microsecond=0).isoformat().replace("+00:00", "+00:00")
            if len(s) == 8:   # YYYYMMDD
                dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
                return dt.replace(microsecond=0).isoformat().replace("+00:00", "+00:00")
        except Exception:
            return s

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        return s


# ===================== Sterling invoice extractors (Excel rules) =====================

def _invoice_header(root: ET.Element) -> Optional[ET.Element]:
    return _find_first(root, "InvoiceDetail/InvoiceHeader") or _find_first(root, "InvoiceHeader")

def _invoice_amounts(header: ET.Element) -> List[str]:
    return [_text(e) for e in _find_all(header, "CollectionDetails/CollectionDetail/AmountCollected")]

def _invoice_currency(header: ET.Element) -> str:
    return _text(_find_first(header, "Order/PriceInfo/Currency"))

def _invoice_customer_id(header: ET.Element) -> str:
    return _text(_find_first(header, "Order/PersonInfoBillTo/PersonInfoKey"))

def _invoice_order_id_first_line(header: ET.Element) -> str:
    ld = _find_all(header, "LineDetails/LineDetail")
    first = ld[0] if ld else None
    return _text(_find_first(first, "OrderLine/Extn/ExtnMiraklOrderID")) if first is not None else ""

def _invoice_invoice_no(header: ET.Element) -> str:
    return _text(_find_first(header, "InvoiceNo"))

def _invoice_tx_date_pref_ship(header: ET.Element) -> str:
    ship = _text(_find_first(header, "Shipment/ActualShipmentDate"))
    return ship if ship else _text(_find_first(header, "DateInvoiced"))

def _invoice_type(header: ET.Element) -> str:
    return _text(_find_first(header, "InvoiceType"))

def _invoice_refund_reference_value(header: ET.Element) -> str:
    refs = _find_all(header, "LineDetails/LineDetail/OrderLine/References/Reference")
    for ref in refs:
        name = _text(_find_first(ref, "Name")).strip().upper()
        if name in {"RO-ID", "MRKL_REFUND_ID"}:
            return _text(_find_first(ref, "Value"))
    return ""


# ===================== SIMPLE payload builders =====================

def _build_order_payload_from_invoice(header: ET.Element) -> Dict[str, Any]:
    """
    orders[0] mapping (Excel):
      amount              = ROUND(SUM(CollectionDetail/AmountCollected),2)
      currency_iso_code   = Order/PriceInfo/Currency
      customer_id         = Order/PersonInfoBillTo/PersonInfoKey
      order_id            = first LineDetails/.../ExtnMiraklOrderID
      payment_status      = "OK"
      transaction_date    = Shipment/ActualShipmentDate if present else DateInvoiced
      transaction_number  = InvoiceNo
    """
    amount = _sum_amounts_str(_invoice_amounts(header), abs_value=False)
    currency = _invoice_currency(header)
    customer_id = _invoice_customer_id(header)
    order_id = _invoice_order_id_first_line(header)
    tx_date_iso = _to_iso8601_utc(_invoice_tx_date_pref_ship(header))
    inv_no = _invoice_invoice_no(header)

    return {
        "amount": amount,
        "currency_iso_code": currency,
        "customer_id": customer_id,
        "order_id": order_id,
        "payment_status": "OK",
        "transaction_date": tx_date_iso,
        "transaction_number": inv_no,
    }

def _build_refund_payload_from_invoice(header: ET.Element) -> Dict[str, Any]:
    """
    refunds[0] mapping (Excel):
      amount              = ABS(ROUND(SUM(CollectionDetail/AmountCollected),2))
      currency_iso_code   = Order/PriceInfo/Currency
      payment_status      = "OK"
      refund_id           = if UPPER(TRIM(InvoiceType))='CREDIT_MEMO' -> Reference1
                            else -> first OrderLine/References/Reference[Name in ('RO-ID','MRKL_REFUND_ID')]/Value
      transaction_date    = DateInvoiced
      transaction_number  = InvoiceNo
    Additionally, we include customer_id if available (from PersonInfoBillTo/PersonInfoKey).
    """
    amount = _sum_amounts_str(_invoice_amounts(header), abs_value=True)
    currency = _invoice_currency(header)
    inv_type = (_invoice_type(header) or "").strip().upper()
    if inv_type == "CREDIT_MEMO":
        refund_id = _text(_find_first(header, "Reference1"))
    else:
        refund_id = _invoice_refund_reference_value(header)
    tx_date_iso = _to_iso8601_utc(_text(_find_first(header, "DateInvoiced")))
    inv_no = _invoice_invoice_no(header)
    # customer_id = _invoice_customer_id(header)  # may be ""

    return {
        "amount": amount,
        "currency_iso_code": currency,
        "refund_id": refund_id,
        "payment_status": "OK",
        "transaction_date": tx_date_iso,
        "transaction_number": inv_no,
    }


# ===================== Mirakl feed/wrapper mappers to SIMPLE payloads =====================

def _map_mirakl_order_body_to_simple(root: ET.Element) -> Dict[str, Any]:
    """
    <body><orders><order>...</order></orders></body>
    Compute amount = price + shipping_price + Σ(taxes) + Σ(shipping_taxes)
    """
    order = _find_first(root, "body/orders/order") or _find_first(root, "orders/order") or root

    def _sum_nodes(nodes: List[ET.Element]) -> Decimal:
        total = Decimal("0")
        for n in nodes:
            d = _to_decimal(_text(n))
            if d is not None:
                total += d
        return total

    price = _to_decimal(_text(_find_first(order, "price"))) or Decimal("0")
    shipping = _to_decimal(_text(_find_first(order, "shipping_price"))) or Decimal("0")
    taxes = _sum_nodes(_find_all(order, "order_lines/order_line/taxes/tax/amount"))
    ship_taxes = _sum_nodes(_find_all(order, "order_lines/order_line/shipping_taxes/shipping_tax/amount"))
    total_amount = (price + shipping + taxes + ship_taxes).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    payload = {
        "amount": f"{total_amount:.2f}",
        "currency_iso_code": _text(_find_first(order, "currency_iso_code")),
        "customer_id": _text(_find_first(order, "customer/customer_id")),
        "order_id": _text(_find_first(order, "order_id")),
        "payment_status": "OK",
        "transaction_date": _to_iso8601_utc(_text(_find_first(order, "transaction_date"))),
        "transaction_number": _text(_find_first(order, "transaction_number")),
    }
    return payload

def _map_mirakl_wrapper_to_simple(root: ET.Element, mode: str) -> Optional[Dict[str, Any]]:
    """
    <MiraklOrderRefund><Order>...</Order></MiraklOrderRefund>  -> orders payload
    <MiraklOrderRefund><Refund>...</Refund></MiraklOrderRefund> -> refunds payload
    """
    order = _find_first(root, "MiraklOrderRefund/Order") or _find_first(root, "Order")
    refund = _find_first(root, "MiraklOrderRefund/Refund") or _find_first(root, "Refund")

    if mode == "order" and order is not None:
        return {
            "amount": _sum_amounts_str([_text(_find_first(order, "amount"))], abs_value=False),
            "currency_iso_code": _text(_find_first(order, "currency_iso_code")),
            "customer_id": _text(_find_first(order, "customer_id")),
            "order_id": _text(_find_first(order, "order_id")),
            "payment_status": "OK",  # fixed
            "transaction_date": _to_iso8601_utc(_text(_find_first(order, "transaction_date"))),
            "transaction_number": _text(_find_first(order, "transaction_number")),
        }

    if mode == "refund" and refund is not None:
        # Wrapper Refund typically has no customer_id; emit empty string
        return {
            "amount": _sum_amounts_str([_text(_find_first(refund, "amount"))], abs_value=True),
            "currency_iso_code": _text(_find_first(refund, "currency_iso_code")),
            # "customer_id": "",  # not available in wrapper; keep field for shape consistency
            "refund_id": _text(_find_first(refund, "refund_id")),
            "payment_status": "OK",
            "transaction_date": _to_iso8601_utc(_text(_find_first(refund, "transaction_date"))),
            "transaction_number": _text(_find_first(refund, "transaction_number")),
        }

    return None


# ===================== Public API =====================

def map_mirakl_xml_to_template(xml_text: str, mode: str) -> Dict[str, Any]:
    """
    Convert XML into SIMPLE Mirakl JSON payloads.
    mode='order'  -> {"orders":[payload]}
    mode='refund' -> {"refunds":[payload]}
    """
    root = ET.fromstring(xml_text)

    # Prefer wrapper if present
    wrapped = _map_mirakl_wrapper_to_simple(root, mode)
    if wrapped is not None:
        return {"orders": [wrapped]} if mode == "order" else {"refunds": [wrapped]}

    # Mirakl body (orders only)
    if mode == "order" and (_find_first(root, "body/orders/order") or _find_first(root, "orders/order")):
        payload = _map_mirakl_order_body_to_simple(root)
        return {"orders": [payload]}

    # Sterling fallback using Excel mappings
    header = _invoice_header(root)
    if header is not None:
        if mode == "order":
            payload = _build_order_payload_from_invoice(header)
            return {"orders": [payload]}
        else:
            payload = _build_refund_payload_from_invoice(header)
            return {"refunds": [payload]}

    # If nothing matched, return empty skeleton (never crash)
    return {"orders": []} if mode == "order" else {"refunds": []}


def transform_payload(folder_key: str, xml_text: str) -> Optional[Dict[str, Any]]:
    """
    Router for the extractor:
      - folder_key "mirakl-order"  -> map as orders payload
      - folder_key "mirakl-refund" -> map as refunds payload
      - otherwise -> None
    """
    fk = (folder_key or "").strip().lower()
    if fk == "mirakl-order":
        return map_mirakl_xml_to_template(xml_text, "order")
    if fk == "mirakl-refund":
        return map_mirakl_xml_to_template(xml_text, "refund")
    return None


# ===================== CLI (optional quick test) =====================

if __name__ == "__main__":
    import sys, argparse
    ap = argparse.ArgumentParser(description="Map Mirakl XML into simple Mirakl JSON payloads.")
    ap.add_argument("--mode", choices=["order","refund"], required=True)
    ap.add_argument("xmlfile")
    args = ap.parse_args()
    xml_text = open(args.xmlfile, "r", encoding="utf-8").read()
    result = map_mirakl_xml_to_template(xml_text, args.mode)
    print(json.dumps(result, indent=2, ensure_ascii=False))
