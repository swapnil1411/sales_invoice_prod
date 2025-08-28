#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mirakl_transformer.py
Maps Mirakl Order/Refund XML (multiple shapes) to your nested JSON templates.

Public API:
    map_mirakl_xml_to_template(xml_text: str, mode: str) -> dict
        mode = "order"  -> returns JSON shaped like ORDER_TEMPLATE_JSON (filled)
        mode = "refund" -> returns JSON shaped like REFUND_TEMPLATE_JSON (filled)

    transform_payload(folder_key: str, xml_text: str) -> dict | None
        folder_key in {"mirakl-order", "mirakl-refund"} -> returns filled template
        else -> None (caller should write payload as-is)

Supported input XML shapes:
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


# ===================== Templates (exactly as provided) =====================

ORDER_TEMPLATE_JSON = r"""{
    "InvoiceHeader": {
        "InvoiceNo": "65826232",
        "Reference1": "ref",
        "Interco": "12345",
        "DateInvoiced": 1755653117000,
        "InvoiceType": "shipment",
        "Shipment": {
            "ActualShipmentDate": 1755653117000,
            "NodeType": "DC",
            "_ShipNode": "SN001",
            "ShipmentNo": "SHIP125"
        },
        "Order": {
            "PriceInfo": {
                "Currency": "USD",
                "EnterpriseCurrency": "USD",
                "ReportingConversionRate": 1.0
            },
            "PersonInfoBillTo": {
                "PersonInfoKey": "202501030816177856072195"
            }
        },
        "LineDetails": {
            "TotalLines": "1",
            "LineDetail": [
                {
                    "LineCharges": {
                        "LineCharge": [
                            {
                                "Lookups": {
                                    "DiscountAmtABS": "5.00",
                                    "ChargeTypeDesc": "desc",
                                    "LineChargeType": "E"
                                },
                                "ChargeAmount": "10.00"
                            }
                        ]
                    },
                    "OrderLine": {
                        "LineType": "mrkl",
                        "Extn": {
                            "ExtnMiraklOrderID": "AP03542309-225112422-A"
                        }
                    }
                }
            ]
        },
        "CollectionDetails": {
            "CollectionDetail": [
                {
                    "AmountCollected": "217.66"
                }
            ]
        }
    }
}"""

REFUND_TEMPLATE_JSON = r"""{
  "InvoiceHeader": {
    "InvoiceNo": "INV123",
    "Reference1": "REFUND-12345",
    "Interco": "TO",
    "DateInvoiced": 20250725,
    "InvoiceType": "CREDIT_MEMO",
    "Shipment": {
      "ActualShipmentDate": 20250720,
      "NodeType": "DC",
      "_ShipNode": "SN001",
      "ShipmentNo": "SHIP123"
    },
    "Order": {
      "PriceInfo": {
        "Currency": "USD",
        "EnterpriseCurrency": "USD",
        "ReportingConversionRate": 1
      },
      "PersonInfoBillTo": {
        "PersonInfoKey": "P12345"
      }
    },
    "LineDetails": {
      "TotalLines": "1",
      "LineDetail": [
        {
          "OrderLine": {
            "LineType": "mrkl",
            "Extn": {
              "ExtnMiraklOrderID": "MO12345"
            },
            "References": {
              "Reference": [
                {
                  "Name": "RO-ID",
                  "Value": "MRKL-REF-99999"
                }
              ]
            }
          }
        }
      ]
    },
    "CollectionDetails": {
      "CollectionDetail": [
        {
          "AmountCollected": "100.00"
        }
      ]
    }
  }
}"""


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

def _to_epoch_ms(iso_str: str) -> int:
    """
    Accept ISO-8601 with offset or 'Z' and return epoch milliseconds.
    If it's already digits (10 or 13), pass through appropriately.
    """
    s = (iso_str or "").strip()
    if not s:
        return 0
    if s.isdigit():
        if len(s) >= 13:
            return int(s[:13])
        if len(s) == 10:
            return int(s) * 1000
        if len(s) == 8:  # YYYYMMDD -> midnight UTC
            try:
                dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return 0
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0

def _to_yyyymmdd_int(iso_str: str) -> int:
    s = (iso_str or "").strip()
    if not s:
        return 0
    if s.isdigit():
        if len(s) == 8:
            return int(s)
        if len(s) >= 13:
            ms = int(s[:13])
            dt = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
            return int(dt.strftime("%Y%m%d"))
        if len(s) == 10:
            dt = datetime.fromtimestamp(int(s), tz=timezone.utc)
            return int(dt.strftime("%Y%m%d"))
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.strftime("%Y%m%d"))
    except Exception:
        return 0


# ===================== Sterling invoice mappers (Excel rules) =====================

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

def _map_invoice_to_order_template(root: ET.Element) -> Dict[str, Any]:
    header = _invoice_header(root)
    if header is None:
        return json.loads(ORDER_TEMPLATE_JSON)  # nothing to map
    amount_str = _sum_amounts_str(_invoice_amounts(header), abs_value=False)
    currency = _invoice_currency(header)
    customer_id = _invoice_customer_id(header)
    order_id = _invoice_order_id_first_line(header)
    tx_num = _invoice_invoice_no(header)
    tx_date_iso = _invoice_tx_date_pref_ship(header)

    out = json.loads(ORDER_TEMPLATE_JSON)
    hdr = out["InvoiceHeader"]

    hdr["InvoiceNo"] = tx_num
    hdr["DateInvoiced"] = _to_epoch_ms(tx_date_iso)
    hdr["InvoiceType"] = "shipment"

    ship = hdr.setdefault("Shipment", {})
    ship["ActualShipmentDate"] = _to_epoch_ms(tx_date_iso)
    ship["NodeType"] = "DC"
    ship["_ShipNode"] = "SN001"
    ship["ShipmentNo"] = order_id

    price = hdr.setdefault("Order", {}).setdefault("PriceInfo", {})
    price["Currency"] = currency
    price["EnterpriseCurrency"] = currency
    price["ReportingConversionRate"] = 1.0

    bill_to = hdr["Order"].setdefault("PersonInfoBillTo", {})
    bill_to["PersonInfoKey"] = customer_id

    lined = hdr.setdefault("LineDetails", {})
    lined["TotalLines"] = "1"
    line0 = lined.setdefault("LineDetail", [{}])[0]
    ol = line0.setdefault("OrderLine", {})
    ol["LineType"] = "mrkl"
    extn = ol.setdefault("Extn", {})
    extn["ExtnMiraklOrderID"] = order_id

    coll0 = hdr.setdefault("CollectionDetails", {}).setdefault("CollectionDetail", [{}])[0]
    coll0["AmountCollected"] = amount_str

    return out

def _map_invoice_to_refund_template(root: ET.Element) -> Dict[str, Any]:
    header = _invoice_header(root)
    if header is None:
        return json.loads(REFUND_TEMPLATE_JSON)  # nothing to map
    amount_str = _sum_amounts_str(_invoice_amounts(header), abs_value=True)
    currency = _invoice_currency(header)
    tx_num = _invoice_invoice_no(header)
    inv_type = (_invoice_type(header) or "").strip().upper()
    if inv_type == "CREDIT_MEMO":
        refund_id = _text(_find_first(header, "Reference1"))
    else:
        refund_id = _invoice_refund_reference_value(header)
    date_invoiced = _text(_find_first(header, "DateInvoiced"))

    out = json.loads(REFUND_TEMPLATE_JSON)
    hdr = out["InvoiceHeader"]

    hdr["InvoiceNo"] = tx_num
    hdr["Reference1"] = f"REFUND-{refund_id}" if refund_id else ""
    hdr["Interco"] = "TO"
    hdr["DateInvoiced"] = _to_yyyymmdd_int(date_invoiced)
    hdr["InvoiceType"] = "CREDIT_MEMO"

    ship = hdr.setdefault("Shipment", {})
    ship["ActualShipmentDate"] = _to_yyyymmdd_int(date_invoiced)
    ship["NodeType"] = "DC"
    ship["_ShipNode"] = "SN001"
    ship["ShipmentNo"] = "SHIP123"

    price = hdr.setdefault("Order", {}).setdefault("PriceInfo", {})
    price["Currency"] = currency
    price["EnterpriseCurrency"] = currency
    price["ReportingConversionRate"] = 1

    bill_to = hdr["Order"].setdefault("PersonInfoBillTo", {})
    bill_to["PersonInfoKey"] = ""

    lined = hdr.setdefault("LineDetails", {})
    lined["TotalLines"] = "1"
    line0 = lined.setdefault("LineDetail", [{}])[0]
    ol = line0.setdefault("OrderLine", {})
    ol["LineType"] = "mrkl"
    ol.setdefault("Extn", {})["ExtnMiraklOrderID"] = ""

    refs = ol.setdefault("References", {}).setdefault("Reference", [{}])
    if not refs:
        refs.append({})
    refs[0]["Name"] = "RO-ID"
    refs[0]["Value"] = f"MRKL-REF-{refund_id}" if refund_id else ""

    coll0 = hdr.setdefault("CollectionDetails", {}).setdefault("CollectionDetail", [{}])[0]
    coll0["AmountCollected"] = amount_str

    return out


# ===================== Mirakl order feed (body) mapper =====================

def _sum_texts(els: List[ET.Element]) -> Decimal:
    total = Decimal("0")
    for e in els:
        d = _to_decimal(_text(e))
        if d is not None:
            total += d
    return total

def _map_mirakl_order_body_to_template(root: ET.Element) -> Dict[str, Any]:
    order = _find_first(root, "body/orders/order") or _find_first(root, "orders/order") or root
    # base amount = price + shipping_price
    price = _to_decimal(_text(_find_first(order, "price"))) or Decimal("0")
    shipping = _to_decimal(_text(_find_first(order, "shipping_price"))) or Decimal("0")
    taxes = _sum_texts(_find_all(order, "order_lines/order_line/taxes/tax/amount"))
    ship_taxes = _sum_texts(_find_all(order, "order_lines/order_line/shipping_taxes/shipping_tax/amount"))
    total_amount = (price + shipping + taxes + ship_taxes).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    amount_str = f"{total_amount:.2f}"

    currency = _text(_find_first(order, "currency_iso_code"))
    customer_id = _text(_find_first(order, "customer/customer_id"))
    order_id = _text(_find_first(order, "order_id"))
    tx_num = _text(_find_first(order, "transaction_number"))
    tx_date_iso = _text(_find_first(order, "transaction_date"))

    out = json.loads(ORDER_TEMPLATE_JSON)
    hdr = out["InvoiceHeader"]

    hdr["InvoiceNo"] = tx_num
    hdr["DateInvoiced"] = _to_epoch_ms(tx_date_iso)
    hdr["InvoiceType"] = "shipment"

    ship = hdr.setdefault("Shipment", {})
    ship["ActualShipmentDate"] = _to_epoch_ms(tx_date_iso)
    ship["NodeType"] = "DC"
    ship["_ShipNode"] = "SN001"
    ship["ShipmentNo"] = order_id

    price_info = hdr.setdefault("Order", {}).setdefault("PriceInfo", {})
    price_info["Currency"] = currency
    price_info["EnterpriseCurrency"] = currency
    price_info["ReportingConversionRate"] = 1.0

    bill_to = hdr["Order"].setdefault("PersonInfoBillTo", {})
    bill_to["PersonInfoKey"] = customer_id

    lined = hdr.setdefault("LineDetails", {})
    lined["TotalLines"] = "1"
    line0 = lined.setdefault("LineDetail", [{}])[0]
    ol = line0.setdefault("OrderLine", {})
    ol["LineType"] = "mrkl"
    ol.setdefault("Extn", {})["ExtnMiraklOrderID"] = order_id

    coll0 = hdr.setdefault("CollectionDetails", {}).setdefault("CollectionDetail", [{}])[0]
    coll0["AmountCollected"] = amount_str

    return out


# ===================== MiraklOrderRefund wrapper mapper =====================

def _map_mirakl_wrapper_to_template(root: ET.Element, mode: str) -> Optional[Dict[str, Any]]:
    order = _find_first(root, "MiraklOrderRefund/Order") or _find_first(root, "Order")
    refund = _find_first(root, "MiraklOrderRefund/Refund") or _find_first(root, "Refund")

    if mode == "order" and order is not None:
        amount_str = _sum_amounts_str([_text(_find_first(order, "amount"))], abs_value=False)
        currency = _text(_find_first(order, "currency_iso_code"))
        customer_id = _text(_find_first(order, "customer_id"))
        order_id = _text(_find_first(order, "order_id"))
        tx_num = _text(_find_first(order, "transaction_number"))
        tx_date_iso = _text(_find_first(order, "transaction_date"))

        out = json.loads(ORDER_TEMPLATE_JSON)
        hdr = out["InvoiceHeader"]
        hdr["InvoiceNo"] = tx_num
        hdr["DateInvoiced"] = _to_epoch_ms(tx_date_iso)
        hdr["InvoiceType"] = "shipment"

        ship = hdr.setdefault("Shipment", {})
        ship["ActualShipmentDate"] = _to_epoch_ms(tx_date_iso)
        ship["NodeType"] = "DC"
        ship["_ShipNode"] = "SN001"
        ship["ShipmentNo"] = order_id

        price_info = hdr.setdefault("Order", {}).setdefault("PriceInfo", {})
        price_info["Currency"] = currency
        price_info["EnterpriseCurrency"] = currency
        price_info["ReportingConversionRate"] = 1.0

        bill_to = hdr["Order"].setdefault("PersonInfoBillTo", {})
        bill_to["PersonInfoKey"] = customer_id

        lined = hdr.setdefault("LineDetails", {})
        lined["TotalLines"] = "1"
        line0 = lined.setdefault("LineDetail", [{}])[0]
        ol = line0.setdefault("OrderLine", {})
        ol["LineType"] = "mrkl"
        ol.setdefault("Extn", {})["ExtnMiraklOrderID"] = order_id

        coll0 = hdr.setdefault("CollectionDetails", {}).setdefault("CollectionDetail", [{}])[0]
        coll0["AmountCollected"] = amount_str
        return out

    if mode == "refund" and refund is not None:
        amount_str = _sum_amounts_str([_text(_find_first(refund, "amount"))], abs_value=True)
        currency = _text(_find_first(refund, "currency_iso_code"))
        refund_id = _text(_find_first(refund, "refund_id"))
        tx_num = _text(_find_first(refund, "transaction_number"))
        tx_date_iso = _text(_find_first(refund, "transaction_date"))

        out = json.loads(REFUND_TEMPLATE_JSON)
        hdr = out["InvoiceHeader"]
        hdr["InvoiceNo"] = tx_num
        hdr["Reference1"] = f"REFUND-{refund_id}" if refund_id else ""
        hdr["Interco"] = "TO"
        hdr["DateInvoiced"] = _to_yyyymmdd_int(tx_date_iso)
        hdr["InvoiceType"] = "CREDIT_MEMO"

        ship = hdr.setdefault("Shipment", {})
        ship["ActualShipmentDate"] = _to_yyyymmdd_int(tx_date_iso)
        ship["NodeType"] = "DC"
        ship["_ShipNode"] = "SN001"
        ship["ShipmentNo"] = "SHIP123"

        price_info = hdr.setdefault("Order", {}).setdefault("PriceInfo", {})
        price_info["Currency"] = currency
        price_info["EnterpriseCurrency"] = currency
        price_info["ReportingConversionRate"] = 1

        bill_to = hdr["Order"].setdefault("PersonInfoBillTo", {})
        bill_to["PersonInfoKey"] = ""

        lined = hdr.setdefault("LineDetails", {})
        lined["TotalLines"] = "1"
        line0 = lined.setdefault("LineDetail", [{}])[0]
        ol = line0.setdefault("OrderLine", {})
        ol["LineType"] = "mrkl"
        ol.setdefault("Extn", {})["ExtnMiraklOrderID"] = ""

        refs = ol.setdefault("References", {}).setdefault("Reference", [{}])
        if not refs:
            refs.append({})
        refs[0]["Name"] = "RO-ID"
        refs[0]["Value"] = f"MRKL-REF-{refund_id}" if refund_id else ""

        coll0 = hdr.setdefault("CollectionDetails", {}).setdefault("CollectionDetail", [{}])[0]
        coll0["AmountCollected"] = amount_str
        return out

    return None


# ===================== Public API =====================

def map_mirakl_xml_to_template(xml_text: str, mode: str) -> Dict[str, Any]:
    """
    Convert XML into the nested JSON templates using all supported shapes.
    """
    root = ET.fromstring(xml_text)

    # 1) MiraklOrderRefund wrapper (preferred when present)
    wrapped = _map_mirakl_wrapper_to_template(root, mode)
    if wrapped is not None:
        return wrapped

    # 2) Mirakl order feed body
    if mode == "order" and (_find_first(root, "body/orders/order") or _find_first(root, "orders/order")):
        return _map_mirakl_order_body_to_template(root)

    # 3) Sterling invoice fallback
    if mode == "order":
        return _map_invoice_to_order_template(root)
    else:
        return _map_invoice_to_refund_template(root)


def transform_payload(folder_key: str, xml_text: str) -> Optional[Dict[str, Any]]:
    """
    Convenience router for your extractor:
      - folder_key "mirakl-order"  -> map as order template
      - folder_key "mirakl-refund" -> map as refund template
      - otherwise -> None
    """
    fk = (folder_key or "").strip().lower()
    if fk == "mirakl-order":
        return map_mirakl_xml_to_template(xml_text, "order")
    if fk == "mirakl-refund":
        return map_mirakl_xml_to_template(xml_text, "refund")
    return None


# ===================== CLI (optional for quick testing) =====================

if __name__ == "__main__":
    import sys, argparse
    ap = argparse.ArgumentParser(description="Map Mirakl XML into nested JSON templates.")
    ap.add_argument("--mode", choices=["order","refund"], required=True)
    ap.add_argument("xmlfile")
    args = ap.parse_args()
    xml_text = open(args.xmlfile, "r", encoding="utf-8").read()
    result = map_mirakl_xml_to_template(xml_text, args.mode)
    print(json.dumps(result, indent=2, ensure_ascii=False))
