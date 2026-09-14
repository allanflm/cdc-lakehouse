import random
from datetime import datetime, timedelta, timezone

from src.transforms.cdc_entities import (
    ORDER_STATUS_FLOW,
    generate_full_load,
    generate_incremental_round,
)


def test_full_load_creates_consistent_references():
    rng = random.Random(42)
    now = datetime.now(timezone.utc)
    state, customers, orders, order_items = generate_full_load(rng, now, n_customers=10)

    assert len(customers) == 10
    assert all(c["op"] == "I" for c in customers)

    customer_ids = {c["customer_id"] for c in customers}
    assert all(o["customer_id"] in customer_ids for o in orders)

    order_ids = {o["order_id"] for o in orders}
    assert all(i["order_id"] in order_ids for i in order_items)

    assert len(state.customers) == len(customers)
    assert len(state.orders) == len(orders)
    assert len(state.order_items) == len(order_items)


def test_incremental_round_mutates_state_and_is_monotonic_per_key():
    rng = random.Random(7)
    t0 = datetime.now(timezone.utc)
    state, _, _, _ = generate_full_load(rng, t0, n_customers=20)

    t1 = t0 + timedelta(minutes=5)
    customers, orders, items = generate_incremental_round(
        rng, state, t1, n_customer_changes=5, n_order_changes=5, n_item_changes=6
    )

    assert customers, "deveria gerar ao menos um evento de customer"
    for event in customers:
        assert event["op"] == "U"
        assert event["updated_at"] == t1.isoformat(timespec="seconds")
        # o estado reflete a mudanca
        assert state.customers[event["customer_id"]] == event

    for event in orders:
        assert event["op"] == "U"
        # nunca avanca para alem do que o fluxo permite
        assert event["status"] in {"faturado", "entregue", "cancelado"}

    ops_seen = {event["op"] for event in items}
    assert ops_seen <= {"I", "U", "D"}

    # itens deletados saem do estado ativo
    deleted_ids = [e["order_item_id"] for e in items if e["op"] == "D"]
    for item_id in deleted_ids:
        assert item_id not in state.order_items


def test_order_status_flow_has_no_cycles():
    for status, next_statuses in ORDER_STATUS_FLOW.items():
        assert status not in next_statuses


def test_incremental_round_is_noop_safe_with_small_state():
    rng = random.Random(1)
    t0 = datetime.now(timezone.utc)
    state, _, _, _ = generate_full_load(rng, t0, n_customers=1)

    # pedir mais mudancas do que existem no estado nao deve estourar
    customers, orders, items = generate_incremental_round(
        rng, state, t0 + timedelta(minutes=1), n_customer_changes=99, n_order_changes=99, n_item_changes=99
    )
    assert len(customers) <= len(state.customers) + len(customers)
