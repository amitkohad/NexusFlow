import unittest

from app.process import customer_adjustment


class CustomerAdjustmentTest(unittest.TestCase):
    def test_identity_is_set_by_server(self) -> None:
        request = {
            "customer_id": "C-100045",
            "amount": 7500,
            "reason": "fee correction",
            "simulate_transient_failure": True,
        }

        spec = customer_adjustment(request, "user@example.com", "test-id")

        self.assertEqual(spec["request"], request)
        self.assertEqual(spec["variables"], {"requested_by": "user@example.com"})
        self.assertEqual(spec["process_id"], "test-id")


if __name__ == "__main__":
    unittest.main()
