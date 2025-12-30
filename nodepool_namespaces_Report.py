#!/usr/bin/env python3
"""
Genera un reporte Markdown con namespaces por nodo, agrupado por node pool.

- Consulta pods por nodo usando fieldSelector: spec.nodeName=<NODE_NAME>
- Devuelve namespaces únicos (y opcionalmente conteo de pods por namespace)
- Asume que el nombre del nodo en Kubernetes es la IP privada (según requerimiento)

Ejecución:
  python nodepool_namespaces_report.py --context <CTX> --out report.md

Si no se pasa --context, se usa el contexto actual del kubeconfig.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from kubernetes import client, config
from kubernetes.client import ApiException


@dataclass(frozen=True)
class NodePool:
    name: str
    ocid: str
    node_names: List[str]  # en este caso, IPs privadas


# Node pools definidos con la información provista
NODEPOOLS: List[NodePool] = [
    NodePool(
        name="LOC_PRD_31",
        ocid="ocid1.nodepool.oc1.iad.aaaaaaaasohuq3wqggkejptexop2qmryo572wxdp5ovwoyld6n225a2kulgq",
        node_names=[
            "10.140.13.91",
            "10.140.13.66",
            "10.140.13.80",
            "10.140.13.114",
            "10.140.13.102",
            "10.140.13.94",
            "10.140.13.72",
        ],
    ),
    NodePool(
        name="LOC_PRD_APLICACIONES_31",
        ocid="ocid1.nodepool.oc1.iad.aaaaaaaa3cd533y3wa7kp6paf3xahx7xdzkrfnhoyobhggc4hneyrhzr54sa",
        node_names=[
            "10.140.13.96",
            "10.140.13.120",
            "10.140.13.109",
            "10.140.13.78",
            "10.140.13.118",
        ],
    ),
]


def load_kube_config(context: str | None) -> None:
    """
    Carga kubeconfig local (por defecto ~/.kube/config).
    """
    try:
        if context:
            config.load_kube_config(context=context)
        else:
            config.load_kube_config()
    except Exception as e:
        raise RuntimeError(f"No fue posible cargar kubeconfig. Detalle: {e}") from e


def list_namespaces_by_node(v1: client.CoreV1Api, node_name: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Retorna:
      - namespaces únicos ordenados
      - conteo de pods por namespace (solo pods no terminales, por defecto)
    """
    # Filtra pods asignados al nodo. Se consulta en todos los namespaces.
    field_selector = f"spec.nodeName={node_name}"

    try:
        pods = v1.list_pod_for_all_namespaces(
            field_selector=field_selector,
            watch=False,
            limit=0,  # sin paginado explícito; el client maneja la respuesta
        ).items
    except ApiException as e:
        raise RuntimeError(f"Fallo al listar pods para nodeName={node_name}. API error: {e}") from e

    # Se cuentan pods por namespace. Se excluyen pods finalizados por defecto.
    counts: Dict[str, int] = {}
    for p in pods:
        phase = (p.status.phase or "").lower()
        # Excluir succeeded/failed reduce ruido en clusters con jobs/cronjobs.
        if phase in {"succeeded", "failed"}:
            continue
        ns = p.metadata.namespace
        counts[ns] = counts.get(ns, 0) + 1

    namespaces = sorted(counts.keys())
    return namespaces, counts


def build_markdown_report(nodepools: List[NodePool], context: str | None) -> str:
    v1 = client.CoreV1Api()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    ctx_line = context if context else "(kubeconfig current-context)"

    lines: List[str] = []
    lines.append("# Reporte de namespaces por nodo (por node pool)")
    lines.append("")
    lines.append(f"- Generado: `{now}`")
    lines.append(f"- Contexto Kubernetes: `{ctx_line}`")
    lines.append("")
    lines.append("---")
    lines.append("")

    for pool in nodepools:
        lines.append(f"## Node Pool: `{pool.name}`")
        lines.append("")
        lines.append(f"- OCID: `{pool.ocid}`")
        lines.append(f"- Nodos (nodeName): {len(pool.node_names)}")
        lines.append("")

        for node in pool.node_names:
            try:
                namespaces, counts = list_namespaces_by_node(v1, node)
            except Exception as e:
                lines.append(f"### Nodo: `{node}`")
                lines.append("")
                lines.append(f"- Error: `{e}`")
                lines.append("")
                continue

            lines.append(f"### Nodo: `{node}`")
            lines.append("")
            if not namespaces:
                lines.append("- Namespaces: *(sin pods activos o sin coincidencias)*")
                lines.append("")
                continue

            # Tabla simple: namespace | pods activos (no terminales)
            lines.append("| Namespace | Pods activos |")
            lines.append("|---|---:|")
            for ns in namespaces:
                lines.append(f"| `{ns}` | {counts.get(ns, 0)} |")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Genera reporte Markdown de namespaces por nodo, por node pool.")
    p.add_argument("--context", default=None, help="Contexto de kubeconfig a usar (opcional).")
    p.add_argument("--out", default=None, help="Ruta de salida del Markdown (opcional). Si no se pasa, imprime por stdout.")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    load_kube_config(args.context)

    md = build_markdown_report(NODEPOOLS, args.context)

    if args.out:
        try:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(md)
        except Exception as e:
            print(f"Error escribiendo archivo de salida: {e}", file=sys.stderr)
            return 2
    else:
        print(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
