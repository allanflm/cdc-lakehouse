"""Logica pura de geracao e mutacao das entidades simuladas (customers, orders,
order_items). Nao faz I/O: recebe um random.Random e um instante `now`, devolve
dicts (os eventos CDC) e o novo estado. Testavel sem Databricks Connect.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime

CITIES = [
    "Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre",
    "Salvador", "Recife", "Fortaleza", "Brasilia", "Florianopolis",
]
SEGMENTS = ["varejo", "atacado", "corporativo"]
FIRST_NAMES = [
    "Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Hugo",
    "Isabela", "Joao", "Larissa", "Marcos", "Natalia", "Otavio", "Paula",
]
LAST_NAMES = [
    "Silva", "Souza", "Oliveira", "Santos", "Pereira", "Costa", "Ferreira",
    "Almeida", "Ribeiro", "Carvalho",
]

# status alcancaveis a partir do status atual; listas vazias sao terminais
ORDER_STATUS_FLOW = {
    "criado": ["faturado", "cancelado"],
    "faturado": ["entregue", "cancelado"],
    "entregue": [],
    "cancelado": [],
}

PRODUCT_CATALOG = [
    (1, "Parafuso M6", 0.15),
    (2, "Chapa de Aco 1mm", 42.90),
    (3, "Motor Eletrico 1CV", 380.00),
    (4, "Correia Industrial", 55.50),
    (5, "Rolamento 6204", 12.30),
    (6, "Sensor de Proximidade", 89.90),
    (7, "Valvula Solenoide", 145.00),
    (8, "Cabo Flexivel 2.5mm", 3.80),
    (9, "Painel de Controle", 620.00),
    (10, "Filtro de Ar Industrial", 34.20),
]


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


@dataclass
class GeneratorState:
    """Snapshot mutavel do "banco de origem" simulado."""

    customers: dict[int, dict] = field(default_factory=dict)
    orders: dict[int, dict] = field(default_factory=dict)
    order_items: dict[int, dict] = field(default_factory=dict)
    next_customer_id: int = 1
    next_order_id: int = 1
    next_order_item_id: int = 1

    def to_dict(self) -> dict:
        return {
            "customers": self.customers,
            "orders": self.orders,
            "order_items": self.order_items,
            "next_customer_id": self.next_customer_id,
            "next_order_id": self.next_order_id,
            "next_order_item_id": self.next_order_item_id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GeneratorState":
        return cls(
            customers={int(k): v for k, v in data.get("customers", {}).items()},
            orders={int(k): v for k, v in data.get("orders", {}).items()},
            order_items={int(k): v for k, v in data.get("order_items", {}).items()},
            next_customer_id=data.get("next_customer_id", 1),
            next_order_id=data.get("next_order_id", 1),
            next_order_item_id=data.get("next_order_item_id", 1),
        )


def _new_customer(rng: random.Random, customer_id: int, now: datetime) -> dict:
    name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    return {
        "customer_id": customer_id,
        "name": name,
        "city": rng.choice(CITIES),
        "segment": rng.choice(SEGMENTS),
        "updated_at": _iso(now),
        "op": "I",
    }


def _new_order(rng: random.Random, order_id: int, customer_id: int, now: datetime) -> dict:
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": "criado",
        "order_date": now.date().isoformat(),
        "updated_at": _iso(now),
        "op": "I",
    }


def _new_order_item(rng: random.Random, item_id: int, order_id: int, now: datetime) -> dict:
    product_id, product_name, unit_price = rng.choice(PRODUCT_CATALOG)
    return {
        "order_item_id": item_id,
        "order_id": order_id,
        "product_id": product_id,
        "product_name": product_name,
        "quantity": rng.randint(1, 10),
        "unit_price": unit_price,
        "updated_at": _iso(now),
        "op": "I",
    }


def generate_full_load(
    rng: random.Random,
    now: datetime,
    n_customers: int,
    avg_orders_per_customer: float = 2.0,
    avg_items_per_order: float = 2.5,
) -> tuple[GeneratorState, list[dict], list[dict], list[dict]]:
    """Gera a carga inicial. Retorna o estado resultante e os 3 lotes de eventos (op=I)."""
    state = GeneratorState()
    customer_events: list[dict] = []
    order_events: list[dict] = []
    item_events: list[dict] = []

    for _ in range(n_customers):
        cid = state.next_customer_id
        state.next_customer_id += 1
        customer = _new_customer(rng, cid, now)
        state.customers[cid] = customer
        customer_events.append(customer)

        n_orders = max(0, round(rng.gauss(avg_orders_per_customer, 1.0)))
        for _ in range(n_orders):
            oid = state.next_order_id
            state.next_order_id += 1
            order = _new_order(rng, oid, cid, now)
            state.orders[oid] = order
            order_events.append(order)

            n_items = max(1, round(rng.gauss(avg_items_per_order, 1.0)))
            for _ in range(n_items):
                iid = state.next_order_item_id
                state.next_order_item_id += 1
                item = _new_order_item(rng, iid, oid, now)
                state.order_items[iid] = item
                item_events.append(item)

    return state, customer_events, order_events, item_events


def generate_incremental_round(
    rng: random.Random,
    state: GeneratorState,
    now: datetime,
    n_customer_changes: int = 5,
    n_order_changes: int = 10,
    n_item_changes: int = 10,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Aplica mudancas plausiveis no estado in-place e retorna so os eventos
    dessa rodada (op I/U/D), com updated_at monotonico por chave.
    """
    customer_events: list[dict] = []
    order_events: list[dict] = []
    item_events: list[dict] = []

    # customers: mudam de cidade e/ou segmento (SCD2 na Silver)
    customer_ids = list(state.customers.keys())
    for cid in rng.sample(customer_ids, k=min(n_customer_changes, len(customer_ids))):
        current = state.customers[cid]
        updated = dict(current)
        updated["city"] = rng.choice([c for c in CITIES if c != current["city"]])
        if rng.random() < 0.3:
            updated["segment"] = rng.choice(SEGMENTS)
        updated["updated_at"] = _iso(now)
        updated["op"] = "U"
        state.customers[cid] = updated
        customer_events.append(updated)

    # orders: avancam de status (terminais sao ignorados)
    advanceable = [
        oid for oid, o in state.orders.items() if ORDER_STATUS_FLOW.get(o["status"])
    ]
    for oid in rng.sample(advanceable, k=min(n_order_changes, len(advanceable))):
        current = state.orders[oid]
        updated = dict(current)
        updated["status"] = rng.choice(ORDER_STATUS_FLOW[current["status"]])
        updated["updated_at"] = _iso(now)
        updated["op"] = "U"
        state.orders[oid] = updated
        order_events.append(updated)

    # order_items: parte vira update de quantidade, parte vira delete, parte eh item novo
    active_items = list(state.order_items.keys())
    n_item_changes = min(n_item_changes, len(active_items))
    if active_items and n_item_changes:
        chosen = rng.sample(active_items, k=n_item_changes)
        split = n_item_changes // 2
        for iid in chosen[:split]:
            current = state.order_items[iid]
            updated = dict(current)
            updated["quantity"] = max(1, current["quantity"] + rng.choice([-2, -1, 1, 2, 3]))
            updated["updated_at"] = _iso(now)
            updated["op"] = "U"
            state.order_items[iid] = updated
            item_events.append(updated)
        for iid in chosen[split:]:
            current = state.order_items[iid]
            deleted = dict(current)
            deleted["updated_at"] = _iso(now)
            deleted["op"] = "D"
            del state.order_items[iid]
            item_events.append(deleted)

    if state.orders:
        for _ in range(max(0, n_item_changes // 3)):
            order_id = rng.choice(list(state.orders.keys()))
            iid = state.next_order_item_id
            state.next_order_item_id += 1
            item = _new_order_item(rng, iid, order_id, now)
            state.order_items[iid] = item
            item_events.append(item)

    return customer_events, order_events, item_events
