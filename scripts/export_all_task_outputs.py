import os
import sys
import sqlite3
import json
import csv
from datetime import datetime

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "nodes.db"))
EXPORTS_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "exports"))

def format_ts(ts):
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return str(ts)

def clean_log(val):
    if not val:
        return ""
    if isinstance(val, bytes):
        return val.decode('utf-8', errors='replace')
    if isinstance(val, str):
        if (val.startswith("b'") and val.endswith("'")) or (val.startswith('b"') and val.endswith('"')):
            try:
                import codecs
                content = val[2:-1]
                return codecs.escape_decode(bytes(content, "utf-8"))[0].decode("utf-8", errors="replace")
            except Exception:
                pass
    return str(val)

def export_task_result_standalone(exports_base, task_id, task_row, result_row):
    if not task_id or not task_row:
        return None

    task_dir = os.path.join(exports_base, task_id)
    os.makedirs(task_dir, exist_ok=True)

    submitted_at = task_row.get("submitted_at")
    completed_at = result_row.get("completed_at") if result_row else None
    status_final = (result_row.get("status_final") if result_row else None) or task_row.get("status", "")
    duration_seconds = (result_row.get("duration_seconds") if result_row else 0) or 0
    stdout_log = clean_log(result_row.get("stdout_log") if result_row else "")
    stderr_log = clean_log(result_row.get("stderr_log") if result_row else "")

    # --- relatorio.txt ---
    report_path = os.path.join(task_dir, "relatorio.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("RELATÓRIO DE EXECUÇÃO DA TAREFA\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"ID da tarefa:      {task_id}\n")
        f.write(f"Arquivo:          {task_row.get('filename', '')}\n")
        f.write(f"Tipo:              {task_row.get('task_type', '')}\n")
        f.write(f"Usuário:          {task_row.get('user_id', '')}\n")
        f.write(f"Submetida em:     {format_ts(submitted_at)}\n")
        f.write(f"Concluída em:     {format_ts(completed_at)}\n")
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

    # --- resultado.csv ---
    csv_path = os.path.join(task_dir, "resultado.csv")
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
            format_ts(submitted_at),
            format_ts(completed_at),
            task_row.get("allocated_node_id") or "",
            task_row.get("required_cpu_threads") or "",
            task_row.get("required_memory_gb") or "",
            task_row.get("required_disk_gb") or "",
        ])

    # --- saida_stdout.txt e saida_stderr.txt ---
    if stdout_log:
        out_path = os.path.join(task_dir, "saida_stdout.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(stdout_log)
    if stderr_log:
        err_path = os.path.join(task_dir, "saida_stderr.txt")
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(stderr_log)

    return task_dir

def analyze_and_export():
    if not os.path.exists(DB_PATH):
        print(f"Erro: Banco de dados nao encontrado em {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    tasks = cursor.execute("""
        SELECT 
            t.task_id, t.status, t.user_id, t.submitted_at, t.filename, t.task_type,
            t.allocated_node_id, t.required_cpu_threads, t.required_memory_gb, t.required_disk_gb,
            tr.status_final, tr.duration_seconds, tr.completed_at, tr.cpu_peak_percent,
            tr.ram_peak_mb, tr.stdout_log, tr.stderr_log
        FROM tasks t
        LEFT JOIN task_results tr ON tr.task_id = t.task_id
        ORDER BY t.submitted_at DESC
    """).fetchall()

    print(f"=== ANALISE DE EXECUCOES ({len(tasks)} TAREFAS REGISTRADAS) ===\n")

    summary_counts = {}
    exported_tasks = []

    for task in tasks:
        t_dict = dict(task)
        status = t_dict.get("status", "desconhecido")
        summary_counts[status] = summary_counts.get(status, 0) + 1

        tid = t_dict["task_id"]
        filename = t_dict.get("filename") or "N/A"
        submitted = format_ts(t_dict.get("submitted_at"))
        completed = format_ts(t_dict.get("completed_at"))
        duration = t_dict.get("duration_seconds")
        duration_str = f"{duration:.2f}s" if duration is not None else "N/A"

        stdout = clean_log(t_dict.get("stdout_log"))
        stderr = clean_log(t_dict.get("stderr_log"))

        out_dir = export_task_result_standalone(EXPORTS_BASE, tid, t_dict, t_dict)
        
        artifacts = []
        if out_dir and os.path.exists(out_dir):
            for fname in os.listdir(out_dir):
                if fname not in ("relatorio.txt", "resultado.csv", "saida_stdout.txt", "saida_stderr.txt"):
                    artifacts.append(fname)

        exported_tasks.append({
            "task_id": tid,
            "filename": filename,
            "status": status,
            "user_id": t_dict.get("user_id"),
            "node": t_dict.get("allocated_node_id") or "—",
            "submitted_at": submitted,
            "completed_at": completed,
            "duration": duration_str,
            "stdout_len": len(stdout),
            "stderr_len": len(stderr),
            "output_dir": out_dir,
            "extra_artifacts": artifacts
        })

    print("--- RESUMO POR STATUS ---")
    for st, cnt in summary_counts.items():
        print(f" - {st}: {cnt} tarefa(s)")
    print("\n" + "="*80)

    print("\n--- DETALHAMENTO DE EXECUCOES MAIS RECENTES ---")
    for item in exported_tasks:
        print(f"\n[Tarefa ID: {item['task_id']}]")
        print(f"  Arquivo: {item['filename']} | Status: {item['status']} | Usuario: {item['user_id']}")
        print(f"  No Alocado: {item['node']} | Duracao: {item['duration']}")
        print(f"  Submetida: {item['submitted_at']} | Concluida: {item['completed_at']}")
        print(f"  Log stdout: {item['stdout_len']} caracteres | Log stderr: {item['stderr_len']} caracteres")
        if item['output_dir']:
            print(f"  [Output]: {item['output_dir']}")
            if item['extra_artifacts']:
                print(f"  [Artefatos Extra]: {', '.join(item['extra_artifacts'])}")
            files_list = [f for f in os.listdir(item['output_dir'])]
            print(f"  [Arquivos Gerados]: {', '.join(files_list)}")

    conn.close()

if __name__ == "__main__":
    analyze_and_export()
