#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mirakl_transformer.py
Maps Mirakl PA01/PA02 XML payloads to the nested JSON shapes defined by
Order.txt and Refund.txt (embedded below). Supports TWO source XML patterns:

1) Sterling-like invoice:
   InvoiceDetail/InvoiceHeader/...

2) Mirakl order body:
   <body><orders><order> ... </order></orders></body>

Public API:
    map_mirakl_xml_to_template(xml_text: str, mode: str) -> dict
        mode = "order"  -> returns JSON shaped like Order.txt
        mode = "refund" -> returns JSON shaped like Refund.txt
"""
from __future__ import annotations

import json, re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET


# ===================== Embedded templates (your Order.txt / Refund.txt) =====================
# (Keep them exactly as your desired output shape; we overwrite the relevant leaves.)

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

def _find_anywhere_first_by_local(root: ET.Element, tag_local: str) -> Optional[ET.Element]:
    # BFS to find the first matching local tag anywhere in the tree
    queue = [root]
    while queue:
        node = queue.pop(0)
        if _local(node.tag) == tag_local:
            return node
        queue.extend(list(node))
    return None


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


# ===================== Source-shape detection =====================

def _looks_like_sterling_invoice(root: ET.Element) -> bool:
    return _find_anywhere_first_by_local(root, "InvoiceDetail") is not None

def _find_mirakl_order_node(root: ET.Element) -> Optional[ET.Element]:
    # Try common path, then fallback to first <order> anywhere
    node = _find_first(root, "body/orders/order")
    if node is None:
        node = _find_anywhere_first_by_local(root, "order")
    return node

def _find_mirakl_refund_node(root: ET.Element) -> Optional[ET.Element]:
    # Mirakl refund sample: <MiraklOrderRefund> <Refund> ... </Refund> </MiraklOrderRefund>
    node = _find_first(root, "MiraklOrderRefund/Refund")
    if node is None:
        node = _find_anywhere_first_by_local(root, "Refund")
    return node


# ===================== Sterling invoice mappers (existing rules) =====================

def _invoice_amounts(root: ET.Element) -> List[str]:
    return [_text(e) for e in _find_all(root, "InvoiceDetail/InvoiceHeader/CollectionDetails/CollectionDetail/AmountCollected")]

def _invoice_currency(root: ET.Element) -> str:
    return _text(_find_first(root, "InvoiceDetail/InvoiceHeader/Order/PriceInfo/Currency"))

def _invoice_customer_id(root: ET.Element) -> str:
    return _text(_find_first(root, "InvoiceDetail/InvoiceHeader/Order/PersonInfoBillTo/PersonInfoKey"))

def _invoice_order_id_first_line(root: ET.Element) -> str:
    ld = _find_all(root, "InvoiceDetail/InvoiceHeader/LineDetails/LineDetail")
    first = ld[0] if ld else None
    return _text(_find_first(first, "OrderLine/Extn/ExtnMiraklOrderID")) if first is not None else ""

def _invoice_invoice_no(root: ET.Element) -> str:
    return _text(_find_first(root, "InvoiceDetail/InvoiceHeader/InvoiceNo"))

def _invoice_tx_date_pref_ship(root: ET.Element) -> str:
    ship = _text(_find_first(root, "InvoiceDetail/InvoiceHeader/Shipment/ActualShipmentDate"))
    if ship:
        return ship
    return _text(_find_first(root, "InvoiceDetail/InvoiceHeader/DateInvoiced"))

def _invoice_type(root: ET.Element) -> str:
    return _text(_find_first(root, "InvoiceDetail/InvoiceHeader/InvoiceType"))

def _invoice_refund_reference_value(root: ET.Element) -> str:
    refs = _find_all(root, "InvoiceDetail/InvoiceHeader/LineDetails/LineDetail/OrderLine/References/Reference")
    for ref in refs:
        name = _text(_find_first(ref, "Name")).strip().upper()
        if name in {"RO-ID", "MRKL_REFUND_ID"}:
            return _text(_find_first(ref, "Value"))
    return ""

def _invoice_date_invoiced(root: ET.Element) -> str:
    return _text(_find_first(root, "InvoiceDetail/InvoiceHeader/DateInvoiced"))


# ===================== Mirakl order body mappers =====================

def _order_text(order_node: ET.Element, subpath: str) -> str:
    el = _find_first(order_node, subpath)
    return _text(el)

def _sum_mirakl_order_amount(order_node: ET.Element) -> str:
    """
    Compute amount for Mirakl body:
      preferred base = total_price
      fallback base  = price + shipping_price
      tax components = sum of all <tax/amount> and <shipping_tax/amount> anywhere under this order
      final          = base + taxes (rounded HALF_UP to 2 dp)
    """
    base = None
    for path in ["total_price", "price"]:
        t = _order_text(order_node, path)
        if t:
            base = _to_decimal(t)
            if base is not None:
                break
    if base is None:
        base = Decimal("0")

    ship = _to_decimal(_order_text(order_node, "shipping_price")) or Decimal("0")
    # If base was total_price, it may already include shipping (as in sample).
    # Detect: if total_price present -> do NOT add shipping again.
    total_price_present = _order_text(order_node, "total_price") != ""

    if not total_price_present:
        base = base + ship

    # Sum taxes across entire order subtree
    tax_total = Decimal("0")
    # Traverse for <tax> and <shipping_tax> elements and read their child <amount>
    stack = [order_node]
    while stack:
        node = stack.pop()
        lname = _local(node.tag)
        if lname in ("tax", "shipping_tax"):
            amt = _text(_find_first(node, "amount"))
            d = _to_decimal(amt)
            if d is not None:
                tax_total += d
        stack.extend(list(node))

    total = base + tax_total
    return f"{total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"


# ===================== Public API =====================

def map_mirakl_xml_to_template(xml_text: str, mode: str) -> Dict[str, Any]:
    """
    Convert Mirakl Order/Refund source XML into the nested JSON templates.
    Selects the correct extractor depending on the input XML shape.
    """
    root = ET.fromstring(xml_text)

    if mode == "order":
        mirakl_order = _find_mirakl_order_node(root)
        if mirakl_order is not None:
            # --- Map from Mirakl order body ---
            amount_str = _sum_mirakl_order_amount(mirakl_order)
            currency = _order_text(mirakl_order, "currency_iso_code")
            customer_id = _order_text(mirakl_order, "customer/customer_id")
            order_id = _order_text(mirakl_order, "order_id")
            tx_num = _order_text(mirakl_order, "transaction_number")
            tx_date_iso = _order_text(mirakl_order, "transaction_date")

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

        # --- Fallback: map from Sterling invoice ---
        amounts = _invoice_amounts(root)
        amount_str = _sum_amounts_str(amounts, abs_value=False)
        currency = _invoice_currency(root)
        customer_id = _invoice_customer_id(root)
        order_id = _invoice_order_id_first_line(root)
        tx_num = _invoice_invoice_no(root)
        tx_date_iso = _invoice_tx_date_pref_ship(root)

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

    # ---------------- refund mode ----------------
    # Prefer Mirakl refund body if present, otherwise Sterling invoice mapping.
    mirakl_refund = _find_mirakl_refund_node(root)
    if mirakl_refund is not None:
        amount_raw = _text(_find_first(mirakl_refund, "amount"))  # expect positive according to Mirakl
        amount_str = _sum_amounts_str([amount_raw], abs_value=True)
        currency = _text(_find_first(mirakl_refund, "currency_iso_code"))
        refund_id = _text(_find_first(mirakl_refund, "refund_id"))
        tx_num = _text(_find_first(mirakl_refund, "transaction_number"))
        tx_date_iso = _text(_find_first(mirakl_refund, "transaction_date"))

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

    # Fallback: Sterling invoice -> Refund template
    amounts = _invoice_amounts(root)
    amount_str = _sum_amounts_str(amounts, abs_value=True)
    currency = _invoice_currency(root)
    tx_num = _invoice_invoice_no(root)
    inv_type = (_invoice_type(root) or "").strip().upper()
    if inv_type == "CREDIT_MEMO":
        refund_id = _text(_find_first(root, "InvoiceDetail/InvoiceHeader/Reference1"))
    else:
        refund_id = _invoice_refund_reference_value(root)
    date_invoiced_iso = _invoice_date_invoiced(root)

    out = json.loads(REFUND_TEMPLATE_JSON)
    hdr = out["InvoiceHeader"]

    hdr["InvoiceNo"] = tx_num
    hdr["Reference1"] = f"REFUND-{refund_id}" if refund_id else ""
    hdr["Interco"] = "TO"
    hdr["DateInvoiced"] = _to_yyyymmdd_int(date_invoiced_iso)
    hdr["InvoiceType"] = "CREDIT_MEMO"

    ship = hdr.setdefault("Shipment", {})
    ship["ActualShipmentDate"] = _to_yyyymmdd_int(date_invoiced_iso)
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
