#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
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
    node_names: List[str]  # En este caso: IP privada (spec.nodeName)


# Ajustar aquí si cambian pools/nodos
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


# ----------------------------
# Categorización por namespace
# ----------------------------

# Palabras clave (case-insensitive).
SUPPORT_APPS_KWS = [r"activo", r"tokin", r"\bepm\b", r"vipo"]
INTEGRATIONS_KWS = [r"-integraciones"]
PLATFORM_KWS = [
    r"cattle", r"rancher", r"\bargo\b", r"harbor", r"fleet",
    r"cert-manager", r"devops", r"conf-manager",
]
BASE_SYSTEM_KWS = [
    r"\bkube\b", r"prometheus", r"thanos", r"eck8", r"heartbeat",
    r"ingress", r"\bvpa\b", r"synthetics", r"uptime",
]

# Precedencia: el primer match gana.
CATEGORY_RULES: List[Tuple[str, List[str]]] = [
    ("integraciones", INTEGRATIONS_KWS),
    ("apps_soporte", SUPPORT_APPS_KWS),
    ("plataforma", PLATFORM_KWS),
    ("base_sistema", BASE_SYSTEM_KWS),
]


def compile_rules() -> List[Tuple[str, List[re.Pattern]]]:
    compiled: List[Tuple[str, List[re.Pattern]]] = []
    for cat, kws in CATEGORY_RULES:
        compiled.append((cat, [re.compile(k, re.IGNORECASE) for k in kws]))
    return compiled


COMPILED_RULES = compile_rules()


def classify_namespace(ns: str) -> str:
    for cat, patterns in COMPILED_RULES:
        for p in patterns:
            if p.search(ns):
                return cat
    return "otros"


# ----------------------------
# Kubernetes helpers
# ----------------------------

def load_kube_config(context: str | None) -> None:
    try:
        if context:
            config.load_kube_config(context=context)
        else:
            config.load_kube_config()
    except Exception as e:
        raise RuntimeError(f"No fue posible cargar kubeconfig. Detalle: {e}") from e


def list_pods_on_node(v1: client.CoreV1Api, node_name: str) -> List[client.V1Pod]:
    field_selector = f"spec.nodeName={node_name}"
    try:
        return v1.list_pod_for_all_namespaces(field_selector=field_selector, watch=False).items
    except ApiException as e:
        raise RuntimeError(f"Fallo al listar pods para nodeName={node_name}. API error: {e}") from e


def is_active_pod(p: client.V1Pod) -> bool:
    phase = (p.status.phase or "").lower()
    # Se excluyen terminales para evitar ruido (jobs/cronjobs finalizados)
    return phase not in {"succeeded", "failed"}


def summarize_node(pods: List[client.V1Pod]) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Retorna:
      - conteo por namespace (pods activos)
      - conteo por categoría (pods activos)
    """
    ns_counts: Dict[str, int] = {}
    cat_counts: Dict[str, int] = {}

    for p in pods:
        if not is_active_pod(p):
            continue
        ns = p.metadata.namespace
        ns_counts[ns] = ns_counts.get(ns, 0) + 1

        cat = classify_namespace(ns)
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    return ns_counts, cat_counts


def pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (part * 100.0) / total


# ----------------------------
# Markdown report
# ----------------------------

def build_markdown(nodepools: List[NodePool], context: str | None) -> str:
    v1 = client.CoreV1Api()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    ctx_line = context if context else "(kubeconfig current-context)"

    lines: List[str] = []
    lines.append("# Reporte de pods por nodo (por node pool) + mix por categoría")
    lines.append("")
    lines.append(f"- Generado: `{now}`")
    lines.append(f"- Contexto Kubernetes: `{ctx_line}`")
    lines.append("")
    lines.append("## Criterios de categorización")
    lines.append("- `apps_soporte`: ns contiene alguna de: `activo`, `tokin`, `epm`, `vipo`")
    lines.append("- `integraciones`: ns contiene `-integraciones`")
    lines.append("- `plataforma`: ns contiene: `cattle`, `rancher`, `argo`, `harbor`, `fleet`, `cert-manager`, `devops`, etc.")
    lines.append("- `base_sistema`: ns contiene: `kube`, `prometheus`, `eck8`, `heartbeat`, `ingress`, `vpa`, etc.")
    lines.append("")
    lines.append("---")
    lines.append("")

    for pool in nodepools:
        lines.append(f"## Node Pool: `{pool.name}`")
        lines.append("")
        lines.append(f"- OCID: `{pool.ocid}`")
        lines.append(f"- Nodos (nodeName): {len(pool.node_names)}")
        lines.append("")

        # Resumen acumulado pool
        pool_cat_totals: Dict[str, int] = {}
        pool_total_pods = 0

        for node in pool.node_names:
            lines.append(f"### Nodo: `{node}`")
            lines.append("")

            try:
                pods = list_pods_on_node(v1, node)
                ns_counts, cat_counts = summarize_node(pods)
            except Exception as e:
                lines.append(f"- Error: `{e}`")
                lines.append("")
                continue

            total_active = sum(ns_counts.values())
            if total_active == 0:
                lines.append("- Pods activos: *(sin coincidencias / sin pods activos)*")
                lines.append("")
                continue

            # Acumular pool
            pool_total_pods += total_active
            for k, v in cat_counts.items():
                pool_cat_totals[k] = pool_cat_totals.get(k, 0) + v

            # Tabla por categoría
            lines.append("**Mix por categoría (pods activos):**")
            lines.append("")
            lines.append("| Categoría | Pods | % |")
            lines.append("|---|---:|---:|")
            for cat in ["apps_soporte", "integraciones", "plataforma", "base_sistema", "otros"]:
                c = cat_counts.get(cat, 0)
                lines.append(f"| `{cat}` | {c} | {pct(c, total_active):.1f}% |")
            lines.append("")
            lines.append(f"- Total pods activos (no terminales): **{total_active}**")
            lines.append("")

            # Tabla por namespace (como el reporte anterior)
            lines.append("| Namespace | Pods activos | Categoría | % del nodo |")
            lines.append("|---|---:|---|---:|")
            for ns in sorted(ns_counts.keys()):
                c = ns_counts[ns]
                cat = classify_namespace(ns)
                lines.append(f"| `{ns}` | {c} | `{cat}` | {pct(c, total_active):.1f}% |")
            lines.append("")

        # Resumen pool
        lines.append("### Resumen del node pool")
        lines.append("")
        if pool_total_pods == 0:
            lines.append("- Sin pods activos en los nodos listados (o sin coincidencias con nodeName).")
            lines.append("")
        else:
            lines.append("| Categoría | Pods | % del pool |")
            lines.append("|---|---:|---:|")
            for cat in ["apps_soporte", "integraciones", "plataforma", "base_sistema", "otros"]:
                c = pool_cat_totals.get(cat, 0)
                lines.append(f"| `{cat}` | {c} | {pct(c, pool_total_pods):.1f}% |")
            lines.append("")
            lines.append(f"- Total pods activos (no terminales) en el pool: **{pool_total_pods}**")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


# ----------------------------
# CLI
# ----------------------------

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Genera reporte Markdown de pods por nodo y mix por categoría."
    )
    p.add_argument("--context", default=None, help="Contexto de kubeconfig a usar (opcional).")
    p.add_argument("--out", default=None, help="Archivo de salida Markdown (opcional). Si no se pasa, stdout.")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    load_kube_config(args.context)

    md = build_markdown(NODEPOOLS, args.context)

    if args.out:
        try:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(md)
        except Exception as e:
            print(f"Error escribiendo salida: {e}", file=sys.stderr)
            return 2
    else:
        print(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
