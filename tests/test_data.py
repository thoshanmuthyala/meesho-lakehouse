"""Validate every supplied record and independent business invariants offline.

These tests read committed data only. They neither generate data nor call AWS.
"""

import collections
import hashlib
import json
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class DatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = json.loads((ROOT / "data/delivery/manifest.json").read_text())
        cls.expected = json.loads((ROOT / "data/expected.json").read_text())
        cls.rows = []
        for item in cls.manifest["files"]:
            cls.rows.extend(
                json.loads(line)
                for line in (ROOT / "data/delivery" / item["path"])
                .read_text()
                .splitlines()
            )
        cls.unique = {row["event_id"]: row for row in cls.rows}
        cls.latest = {}
        for event in cls.rows:
            key = (event["entity_type"], event["entity_id"])
            if key not in cls.latest or cls.latest[key]["version"] < event["version"]:
                cls.latest[key] = event
        cls.tables = collections.defaultdict(dict)
        for (entity, key), event in cls.latest.items():
            cls.tables[entity][key] = event["payload"]

    def test_manifest_hashes_and_counts(self):
        for item in self.manifest["files"]:
            body = (ROOT / "data/delivery" / item["path"]).read_bytes()
            self.assertEqual(hashlib.sha256(body).hexdigest(), item["sha256"])
            self.assertEqual(len(body.splitlines()), item["rows"])
            self.assertEqual(
                {json.loads(line)["entity_type"] for line in body.splitlines()},
                {item["entity"]},
            )
        self.assertEqual(len(self.rows), 107100)
        self.assertEqual(len(self.unique), 106600)
        self.assertEqual(len(self.rows) - len(self.unique), 500)

    def test_duplicates_are_identical_and_source_versions_unambiguous(self):
        versions = {}
        for row in self.rows:
            self.assertEqual(row, self.unique[row["event_id"]])
            key = (row["entity_type"], row["entity_id"], row["version"])
            if key in versions:
                self.assertEqual(versions[key], row)
            versions[key] = row
        self.assertEqual(
            sum(
                1
                for e in self.unique.values()
                if e["entity_type"] not in {"customer", "product"}
            ),
            100000,
        )

    def test_entity_counts_and_keys(self):
        keys = {
            "customer": "customer_id",
            "product": "product_id",
            "order_item": "order_item_id",
            "payment": "payment_id",
            "shipment": "shipment_item_id",
            "return": "return_id",
            "refund": "refund_id",
        }
        for entity, expected in self.expected["silver_counts"].items():
            self.assertEqual(len(self.tables[entity]), expected)
            for key, row in self.tables[entity].items():
                self.assertEqual(key, row[keys[entity]])

    def test_relationships_and_refund_caps(self):
        t = self.tables
        orders = {r["order_id"] for r in t["order_item"].values()}
        refunded = collections.Counter()
        for item in t["order_item"].values():
            self.assertIn(item["customer_id"], t["customer"])
            self.assertIn(item["product_id"], t["product"])
            self.assertGreater(item["quantity"], 0)
            self.assertLessEqual(
                item["discount_paise"], item["quantity"] * item["unit_price_paise"]
            )
        for p in t["payment"].values():
            self.assertIn(p["order_id"], orders)
        for row in t["shipment"].values():
            self.assertIn(row["order_item_id"], t["order_item"])
        for row in t["return"].values():
            item = t["order_item"][row["order_item_id"]]
            self.assertLessEqual(row["quantity"], item["quantity"])
        for row in t["refund"].values():
            ret = t["return"][row["return_id"]]
            pay = t["payment"][row["payment_id"]]
            self.assertEqual(
                t["order_item"][ret["order_item_id"]]["order_id"], pay["order_id"]
            )
            self.assertEqual(pay["status"], "CAPTURED")
            refunded[row["payment_id"]] += row["amount_paise"]
        for key, amount in refunded.items():
            self.assertLessEqual(amount, t["payment"][key]["amount_paise"])

    def test_financial_control_totals(self):
        captured = sum(
            p["amount_paise"]
            for p in self.tables["payment"].values()
            if p["status"] == "CAPTURED"
        )
        refunds = sum(
            p["amount_paise"]
            for p in self.tables["refund"].values()
            if p["status"] == "COMPLETED"
        )
        booked = sum(
            p["quantity"] * p["unit_price_paise"] - p["discount_paise"]
            for p in self.tables["order_item"].values()
            if p["status"] != "CANCELLED"
        )
        self.assertEqual(captured, self.expected["captured_paise"])
        self.assertEqual(refunds, self.expected["refunded_paise"])
        self.assertEqual(captured - refunds, self.expected["net_collected_paise"])
        self.assertEqual(booked, self.expected["booked_value_paise"])

    def test_order_header_consistency(self):
        headers = collections.defaultdict(set)
        for row in self.tables["order_item"].values():
            headers[row["order_id"]].add(
                tuple(
                    row[k]
                    for k in ["customer_id", "ordered_at", "state", "payment_method"]
                )
            )
        self.assertEqual(len(headers), 10000)
        self.assertTrue(all(len(v) == 1 for v in headers.values()))

    def test_arrival_order_is_not_source_order(self):
        versions = [e["version"] for e in self.rows if e["entity_type"] == "order_item"]
        self.assertNotEqual(versions[:1000], sorted(versions[:1000]))
        folder = ROOT / "data/recovery/late-old-version"
        fixture = json.loads((folder / "manifest.json").read_text())
        late = json.loads((folder / fixture["files"][0]["path"]).read_text())
        self.assertEqual(late, self.unique[late["event_id"]])
        self.assertGreater(
            self.latest[("order_item", late["entity_id"])]["version"], late["version"]
        )


if __name__ == "__main__":
    unittest.main()
