"""
Exportação de resultados de tarefas para diretórios por task_id.
Gera relatório em texto, planilha CSV e arquivos de saída (stdout/stderr) para exportação.
"""

import csv
import os
from datetime import datetime
from typing import Any, Dict, Optional

from context_server.logging_config import get_logger

logger = get_logger(__name__)


def _format_ts(ts: Optional[float]) -> str:
    """Formata timestamp Unix para string legível."""
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return str(ts)


def export_task_result(
    exports_base: str,
    task_id: str,
    task_row: Dict[str, Any],
    result_row: Dict[str, Any],
) -> Optional[str]:
    """
    Cria o diretório da tarefa em exports_base/task_id e gera:
    - relatorio.txt: relatório em texto
    - resultado.csv: uma linha com resumo (planilha)
    - saida_stdout.txt: saída padrão do script (se houver)
    - saida_stderr.txt: saída de erro do script (se houver)

    Args:
        exports_base: Diretório base de exportação (ex.: config.EXPORTS_BASE).
        task_id: ID da tarefa.
        task_row: Dicionário com dados da tarefa (get_task_by_id).
        result_row: Dicionário com status_final, duration_seconds, stdout_log, stderr_log, completed_at.

    Returns:
        Caminho do diretório criado (exports_base/task_id) ou None em caso de erro.
    """
    if not task_id or not task_row or not result_row:
        logger.warning("export_task_result: dados insuficientes")
        return None

    task_dir = os.path.join(exports_base, task_id)
    try:
        os.makedirs(task_dir, exist_ok=True)
    except OSError as e:
        logger.error("Falha ao criar diretório de exportação %s: %s", task_dir, e)
        return None

    submitted_at = task_row.get("submitted_at")
    completed_at = result_row.get("completed_at")
    status_final = result_row.get("status_final", "")
    duration_seconds = result_row.get("duration_seconds") or 0
    stdout_log = result_row.get("stdout_log")
    stderr_log = result_row.get("stderr_log")
    if stdout_log is None:
        stdout_log = ""
    if stderr_log is None:
        stderr_log = ""

    # --- relatorio.txt ---
    report_path = os.path.join(task_dir, "relatorio.txt")
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("RELATÓRIO DE EXECUÇÃO DA TAREFA\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"ID da tarefa:      {task_id}\n")
            f.write(f"Arquivo:          {task_row.get('filename', '')}\n")
            f.write(f"Tipo:              {task_row.get('task_type', '')}\n")
            f.write(f"Usuário:          {task_row.get('user_id', '')}\n")
            f.write(f"Submetida em:     {_format_ts(submitted_at)}\n")
            f.write(f"Concluída em:     {_format_ts(completed_at)}\n")
            f.write(f"Status final:      {status_final}\n")
            f.write(f"Duração (s):      {duration_seconds}\n")
            f.write(f"Nó alocado:       {task_row.get('allocated_node_id') or '—'}\n")
            f.write(f"Requisitos:       CPU {task_row.get('required_cpu_threads')} threads, ")
            f.write(f"RAM {task_row.get('required_memory_gb')} GB, Disco {task_row.get('required_disk_gb')} GB\n")
            f.write("\n" + "-" * 60 + "\n")
            f.write("SAÍDA PADRÃO (stdout)\n")
            f.write("-" * 60 + "\n")
            f.write(stdout_log if stdout_log.strip() else "(vazio)\n")
            f.write("\n" + "-" * 60 + "\n")
            f.write("SAÍDA DE ERRO (stderr)\n")
            f.write("-" * 60 + "\n")
            f.write(stderr_log if stderr_log.strip() else "(vazio)\n")
    except OSError as e:
        logger.error("Falha ao escrever relatório %s: %s", report_path, e)

    # --- resultado.csv (uma linha de resumo para planilha) ---
    csv_path = os.path.join(task_dir, "resultado.csv")
    try:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([
                "task_id", "task_type", "filename", "user_id", "status_final",
                "duration_seconds", "submitted_at", "completed_at", "allocated_node_id",
                "required_cpu_threads", "required_memory_gb", "required_disk_gb",
            ])
            w.writerow([
                task_id,
                task_row.get("task_type", ""),
                task_row.get("filename", ""),
                task_row.get("user_id", ""),
                status_final,
                duration_seconds,
                _format_ts(submitted_at),
                _format_ts(completed_at),
                task_row.get("allocated_node_id") or "",
                task_row.get("required_cpu_threads") or "",
                task_row.get("required_memory_gb") or "",
                task_row.get("required_disk_gb") or "",
            ])
    except OSError as e:
        logger.error("Falha ao escrever CSV %s: %s", csv_path, e)

    # --- saida_stdout.txt e saida_stderr.txt ---
    if stdout_log:
        out_path = os.path.join(task_dir, "saida_stdout.txt")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(stdout_log)
        except OSError as e:
            logger.error("Falha ao escrever saida_stdout.txt: %s", e)
    if stderr_log:
        err_path = os.path.join(task_dir, "saida_stderr.txt")
        try:
            with open(err_path, "w", encoding="utf-8") as f:
                f.write(stderr_log)
        except OSError as e:
            logger.error("Falha ao escrever saida_stderr.txt: %s", e)

    logger.info("Exportação da tarefa %s salva em %s", task_id, task_dir)
    return task_dir
