"""Gerador de eventos CDC (Fase 1).

Uso:
    python -m src.generator.cli full --n-customers 50
    python -m src.generator.cli incremental --n-customer-changes 5 --n-order-changes 10 --n-item-changes 10

Grava JSON Lines particionado por data no Volume `cdc_lakehouse.bronze.raw`
(ou localmente em `.generator_output/` com --dry-run). O estado do "banco de
origem" simulado fica em `.generator_state/state.json` (nao versionado).
"""

import argparse
import random
import sys
from datetime import datetime, timezone

from src.transforms.cdc_entities import generate_full_load, generate_incremental_round

from . import state_store, writer


def _run_id(now: datetime) -> str:
    return now.strftime("%Y%m%dT%H%M%S")


def _write_batch(customers, orders, order_items, run_id, now, dry_run) -> None:
    for entity, events in (
        ("customers", customers),
        ("orders", orders),
        ("order_items", order_items),
    ):
        path = writer.write_events(entity, events, run_id, now.date(), dry_run=dry_run)
        if path:
            print(f"{entity}: {len(events)} eventos -> {path}")
        else:
            print(f"{entity}: nenhum evento nessa rodada")


def cmd_full(args: argparse.Namespace) -> None:
    if state_store.load_state() is not None and not args.force:
        print(
            "Ja existe estado salvo em .generator_state/state.json. "
            "Use --force para sobrescrever com uma nova carga inicial.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)
    state, customers, orders, order_items = generate_full_load(
        rng, now, n_customers=args.n_customers
    )
    _write_batch(customers, orders, order_items, _run_id(now), now, args.dry_run)
    state_store.save_state(state)
    print(
        f"Carga inicial: {len(state.customers)} customers, "
        f"{len(state.orders)} orders, {len(state.order_items)} order_items."
    )


def cmd_incremental(args: argparse.Namespace) -> None:
    state = state_store.load_state()
    if state is None:
        print(
            "Nenhum estado encontrado. Rode `full` primeiro para criar a carga inicial.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc)
    customers, orders, order_items = generate_incremental_round(
        rng,
        state,
        now,
        n_customer_changes=args.n_customer_changes,
        n_order_changes=args.n_order_changes,
        n_item_changes=args.n_item_changes,
    )
    _write_batch(customers, orders, order_items, _run_id(now), now, args.dry_run)
    state_store.save_state(state)


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--dry-run", action="store_true", help="grava em .generator_output/ em vez do Volume")
    p.add_argument("--seed", type=int, default=None, help="seed do random, para runs reproduziveis")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gerador de eventos CDC")
    sub = parser.add_subparsers(dest="command", required=True)

    p_full = sub.add_parser("full", help="carga inicial (full load)")
    _add_common_args(p_full)
    p_full.add_argument("--n-customers", type=int, default=50)
    p_full.add_argument("--force", action="store_true", help="sobrescreve estado existente")
    p_full.set_defaults(func=cmd_full)

    p_inc = sub.add_parser("incremental", help="rodada incremental (insert/update/delete)")
    _add_common_args(p_inc)
    p_inc.add_argument("--n-customer-changes", type=int, default=5)
    p_inc.add_argument("--n-order-changes", type=int, default=10)
    p_inc.add_argument("--n-item-changes", type=int, default=10)
    p_inc.set_defaults(func=cmd_incremental)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
