# Execuções canônicas e logs (rodada de novembro/2026)

Ambiente: Kind `tcc-cluster` (K8s v1.32.2, 1 control-plane + 3 workers), broker com
`LiquidAI/lfm2.5-1.2b-instruct` (temperatura 0) via Ollama. Pedido do Exp. 3 padronizado
em 9,45 Gi (9676Mi) nos dois braços.

| Exp. | Braço | JSON canônico | Log |
|---|---|---|---|
| 1 | baseline | exp1/baseline/exp1_baseline_1794680704.json | exp1/logs/exp1_baseline_20261114_151950.log |
| 1 | híbrido  | exp1/hibrido/exp1_hybrid_1794681912.json | exp1/logs/exp1_hybrid_20261114_152504.log |
| 2 | baseline | exp2/baseline/exp2_baseline_1794868547.json | exp2/logs/exp2_baseline_20261116_193431.log |
| 2 | híbrido  | exp2/hibrido/exp2_hybrid_1794868781.json | exp2/logs/exp2_hybrid_20261116_193547.log |
| 3 | baseline | exp3/baseline/exp3_baseline_1794682243.json | exp3/logs/exp3_baseline_20261114_154910.log (+ _kubectl_describe.txt) |
| 3 | híbrido  | exp3/hibrido/exp3_hybrid_1794682379.json | exp3/logs/exp3_hybrid_20261114_155043.log (+ _context_server.log) |
| 4 | baseline | exp4/baseline/exp4_baseline_1794682513.json | exp4/logs/exp4_baseline_20261114_155300.log |
| 4 | híbrido  | exp4/hibrido/exp4_hybrid_1794683915.json | exp4/logs/exp4_hybrid_20261114_155713.log |

## Descartados (em `logs/descartados/`)

- Exp. 2, 14/11 (baseline 154513 e híbrido 154557, JSONs 1794681956 e 1794682150): o pod de
  estresse foi removido com `--wait=false` e o `apply` seguinte encontrou o pod antigo ainda
  terminando; o nó `worker2` ficou a 14m de CPU (sem estresse) e a corrida é inválida.
  Corrigido (`--wait=true` + validação de CPU >= 500m) e refeito em 16/11.
- Exp. 2, 16/11 (193149 e 193153): abortados, Docker Desktop parado.
- Exp. 4 híbrido, 14/11 (155514): corrida entre despacho da fila e o pin de imagem
  (`wait_queued`); corrigido em `scripts/run_exp4_hybrid.py` e refeito (155713).

## Observação sobre os JSONs
Os JSONs de agosto/2026 permanecem nas pastas e servem de comparação entre rodadas
(ex.: concordância preferência-LLM x regra no Exp. 4: 9/9 em agosto, 3/9 em novembro).

## Sensibilidade à temperatura do LLM (offline, sem cluster)

- Script: `scripts/eval_llm_temperature.py` (executado dentro do container `context-server`).
- Resultado: `llm_temperatura/llm_temperatura_1794870242.json`; log: `llm_temperatura/logs/llm_temperatura_analise_20261116_225000.log`.
- Desenho: T = 0, 0,5 e 1,0; 4 scripts de avaliação; 10 repetições cada (120 execuções); Analista e
  Strategist com os prompts reais; Librarian em cold start; semente não fixada.
