#!/usr/bin/env python3
"""
setup_databricks.py — Configura automáticamente el workspace de Azure Databricks.

Acciones que realiza:
  1. Crea un clúster de Spark (single-node, económico para dev).
  2. Crea el directorio /flujo-databricks en Repos.
  3. Clona el repositorio de GitHub en Databricks Repos.
  4. Crea el directorio /Shared/flujo-databricks para los experimentos MLflow.

Requisitos previos:
  pip install databricks-sdk

Variables de entorno necesarias:
  DATABRICKS_HOST   — URL del workspace (ej. https://adb-xxxx.azuredatabricks.net)
  DATABRICKS_TOKEN  — Personal Access Token de Databricks

Uso:
  export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
  export DATABRICKS_TOKEN="dapiXXXXXXXXXXXXXXXX"
  python deploy/setup_databricks.py
"""

import os
import sys
import time


def require_env(var: str) -> str:
    value = os.environ.get(var)
    if not value:
        print(f"❌  Variable de entorno requerida no encontrada: {var}")
        print(f"   Ejecuta: export {var}='<valor>'")
        sys.exit(1)
    return value


def main() -> None:
    host = require_env("DATABRICKS_HOST").rstrip("/")
    token = require_env("DATABRICKS_TOKEN")

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.compute import (
            AutoScale,
            ClusterSpec,
            DataSecurityMode,
            RuntimeEngine,
        )
        from databricks.sdk.service.workspace import ImportFormat, Language
    except ImportError:
        print("❌  Instala el SDK de Databricks: pip install databricks-sdk")
        sys.exit(1)

    print(f"\n🔌 Conectando a: {host}")
    client = WorkspaceClient(host=host, token=token)

    # ------------------------------------------------------------------
    # 1. Crear clúster de Spark (single-node, económico)
    # ------------------------------------------------------------------
    print("\n🖥️  Creando clúster de Spark (single-node)...")
    cluster_name = "flujo-databricks-dev"

    # Verificar si ya existe
    existing = [c for c in client.clusters.list() if c.cluster_name == cluster_name]
    if existing:
        cluster_id = existing[0].cluster_id
        print(f"   ✅ Clúster existente encontrado: {cluster_id}")
    else:
        cluster = client.clusters.create_and_wait(
            cluster_name=cluster_name,
            spark_version="14.3.x-scala2.12",   # Databricks Runtime 14.3 LTS
            node_type_id="Standard_D4as_v4",     # 16 GB RAM, 4 vCores — disponible en eastus2
            num_workers=0,                        # single-node (sin workers = más barato)
            spark_conf={
                "spark.master": "local[*]",
            },
            custom_tags={"project": "flujo-databricks", "ResourceClass": "SingleNode"},
            autotermination_minutes=30,           # Apagado automático tras 30 min inactivo
            data_security_mode=DataSecurityMode.SINGLE_USER,
        )
        cluster_id = cluster.cluster_id
        print(f"   ✅ Clúster creado: {cluster_id}")

    # ------------------------------------------------------------------
    # 2. Crear directorio de MLflow experiments
    # ------------------------------------------------------------------
    print("\n📁 Creando directorio para experimentos MLflow...")
    experiment_path = "/Shared/flujo-databricks/dev"
    try:
        client.workspace.mkdirs(path=experiment_path)
        print(f"   ✅ Directorio creado: {experiment_path}")
    except Exception as exc:
        print(f"   ℹ️  Directorio ya existe o error ignorado: {exc}")

    # ------------------------------------------------------------------
    # 3. Clonar repositorio en Databricks Repos
    # ------------------------------------------------------------------
    print("\n📦 Configurando Databricks Repos...")
    repo_path = "/Repos/flujo-databricks"
    github_url = "https://github.com/joespinch/flujo-databricks"

    existing_repos = [r for r in client.repos.list() if r.path == repo_path]
    if existing_repos:
        print(f"   ✅ Repo ya existe: {repo_path}")
    else:
        try:
            repo = client.repos.create(
                url=github_url,
                provider="github",
                path=repo_path,
            )
            print(f"   ✅ Repo clonado en: {repo.path}")
        except Exception as exc:
            print(f"   ⚠️  No se pudo clonar el repo automáticamente: {exc}")
            print("   📋 Hazlo manualmente: Workspace → Repos → Add Repo → pega la URL de GitHub")

    # ------------------------------------------------------------------
    # 4. Resumen final
    # ------------------------------------------------------------------
    print("\n" + "=" * 55)
    print("   🎉 CONFIGURACIÓN DE DATABRICKS COMPLETADA")
    print("=" * 55)
    print(f"   Workspace : {host}")
    print(f"   Clúster   : {cluster_name} ({cluster_id})")
    print(f"   Repo      : {repo_path}")
    print(f"   MLflow    : {experiment_path}")
    print("=" * 55)
    print("\n📋 Próximos pasos:")
    print("   1. Abre el workspace en el navegador:")
    print(f"      {host}")
    print("   2. Ve a Repos → flujo-databricks → notebooks")
    print("   3. Ejecuta los notebooks en orden: 01 → 02 → 03 → 04")
    print("   4. Adjunta cada notebook al clúster:", cluster_name)
    print()


if __name__ == "__main__":
    main()
