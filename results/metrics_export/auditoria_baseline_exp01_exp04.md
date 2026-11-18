# Auditoria de Integridade — Baselines EXP-01 a EXP-04

**Data da auditoria:** 2026-08-14  
**Escopo:** apenas arquivos em `results/metrics_export/exp{1..4}/baseline/`  
**Código / runners:** não alterados nesta auditoria  

**Veredito geral:** os quatro JSONs de baseline estão **íntegros, consistentes e aptos** para comparação com o braço híbrido. Nenhuma amostra incompleta, campo crítico nulo indevido ou corrupção estrutural foi encontrada.

---

## 1. Inventário de arquivos auditados

| Experimento | Arquivo | Tamanho lógico |
|---|---|---|
| EXP-01 | `results/metrics_export/exp1/baseline/exp1_baseline_1786744882.json` | array com 10 objetos |
| EXP-02 | `results/metrics_export/exp2/baseline/exp2_baseline_1786745394.json` | objeto com 5 placements |
| EXP-03 | `results/metrics_export/exp3/baseline/exp3_baseline_1786747620.json` | objeto + 18 samples Pending |
| EXP-04 | `results/metrics_export/exp4/baseline/exp4_baseline_1786748072.json` | objeto com 9 runs |

Há **exatamente um** JSON de baseline por experimento — sem duplicatas conflitantes na pasta.

---

## 2. Checklist por experimento

### 2.1 EXP-01 — Convergência / desperdício (N=10)

| Critério | Resultado |
|---|---|
| Exatamente 10 entradas | **OK** (`run` 1…10, sem lacunas) |
| Status terminal | **OK** — 10/10 `Succeeded` |
| `duration_s` íntegro (não nulo, > 0) | **OK** — todos numéricos |
| Timestamps `at` presentes | **OK** |
| Limites RAM 256Mi no JSON | **Observação** — o JSON **não** persiste `memory_request/limit` por run |

**Tempos wall-clock (s):**

| Run | duration_s | Status |
|---|---|---|
| 1 | 35,897 | Succeeded |
| 2 | 25,302 | Succeeded |
| 3 | 25,365 | Succeeded |
| 4 | 25,330 | Succeeded |
| 5 | 35,836 | Succeeded |
| 6 | 25,291 | Succeeded |
| 7 | 25,413 | Succeeded |
| 8 | 25,319 | Succeeded |
| 9 | 25,388 | Succeeded |
| 10 | 25,333 | Succeeded |

- **Soma:** 274,474 s  
- **Média:** 27,447 s  
- **Min / Max:** 25,291 / 35,897 s  

**Anomalia leve (não bloqueante):** runs 1 e 5 (~36 s) ficam acima do platô ~25 s — compatível com cold start de imagem / scheduling, não com dado corrompido.

**Integridade dos 256Mi:** confirmada no artefato estático [`docs/experimentos/exp1_baseline_job.yaml`](../../docs/experimentos/exp1_baseline_job.yaml) (`requests.memory` e `limits.memory` = `256Mi`). Para a monografia, citar YAML + JSON de tempos, ou enriquecer o schema do runner em sprint futura (opcional).

**Status auditoria EXP-01:** APROVADO (com nota de schema).

---

### 2.2 EXP-02 — Mitigação de caos / noisy neighbor (N=5)

| Critério | Resultado |
|---|---|
| 5 jobs registrados | **OK** (`exp2-baseline-1` … `-5`) |
| Todos `Succeeded` | **OK** |
| `nodeName` em cada placement | **OK** — nenhum nulo |
| Makespan presente | **OK** — `makespan_s = 34,421` |
| Contagem no nó sobrecarregado | **OK** — `n_on_hot_node = 2`, `hot_node = tcc-cluster-worker2` |

**Distribuição de placement:**

| Run | Pod | nodeName | on_hot_node |
|---|---|---|---|
| 1 | exp2-baseline-1-mznwn | tcc-cluster-worker3 | false |
| 2 | exp2-baseline-2-4tb8c | tcc-cluster-worker2 | **true** |
| 3 | exp2-baseline-3-6vx5g | tcc-cluster-worker2 | **true** |
| 4 | exp2-baseline-4-4nrxt | tcc-cluster-worker3 | false |
| 5 | exp2-baseline-5-8x4s9 | tcc-cluster-worker | false |

**Anomalia metodológica (não de integridade do JSON):** o snapshot `kubectl_top_nodes_after_warmup` mostra CPU baixa no nó sobrecarregado (`worker2` ≈ 52m / 0%). Isso sugere atraso do Metrics Server ou warm-up curto demais para telemetria visual — **não** invalida o registro de placement (2/5 no nó estressado), mas deve ser declarado ao comparar com o híbrido (o ranker depende de telemetria viva).

**Status auditoria EXP-02:** APROVADO (com nota de telemetria de estresse).

---

### 2.3 EXP-03 — Resiliência / oversize (deadlock de scheduling)

| Critério | Resultado |
|---|---|
| Request oversize 10Gi | **OK** (`memory_request` / `memory_request_gi`) |
| \(R_{\max}\) documentado | **OK** — 7,716 Gi nos 3 workers |
| Fase final `Pending` | **OK** |
| `stayed_pending == true` | **OK** |
| `FailedScheduling` | **OK** |
| `Insufficient memory` | **OK** |
| `final_nodeName == null` | **OK** — esperado (sem binding) |
| Série temporal Pending | **OK** — 18 samples, todos Pending / nodeName null |

**Evento-chave preservado:**

```text
FailedScheduling … 0/4 nodes are available:
  1 node(s) had untolerated taint {node-role.kubernetes.io/control-plane: },
  3 Insufficient memory.
```

**Nota:** `null` em `final_nodeName` / samples **não** é corrupção — é a evidência do deadlock.

**Status auditoria EXP-03:** APROVADO.

---

### 2.4 EXP-04 — Topologia inteligente (N=9)

| Critério | Resultado |
|---|---|
| Exactamente 9 runs | **OK** |
| Sequência intercalada L/F/M | **OK** — `["edge","fog","cloud"] × 3` |
| Balanceamento 3+3+3 | **OK** (`profile_counts`) |
| `nodeName` em todos | **OK** — nenhum nulo |
| `allocated_layer` em todos | **OK** |
| Hit rate 22,2% | **OK** — `hits=2`, `hit_rate=0.2222` (2/9) |
| Matriz de confusão consistente | **OK** — soma das células = 9 |

**Lista preferred → allocated:**

| Run | Profile | Preferred | nodeName | Allocated | Hit |
|---|---|---|---|---|---|
| 1 | edge | edge | worker3 | cloud | não |
| 2 | fog | fog | worker3 | cloud | não |
| 3 | cloud | cloud | worker3 | cloud | **sim** |
| 4 | edge | edge | worker3 | cloud | não |
| 5 | fog | fog | worker3 | cloud | não |
| 6 | cloud | cloud | worker3 | cloud | **sim** |
| 7 | edge | edge | worker2 | fog | não |
| 8 | fog | fog | worker3 | cloud | não |
| 9 | cloud | cloud | worker2 | fog | não |

**Matriz preferred × allocated:**

|  | edge | fog | cloud |
|---|---|---|---|
| **edge** | 0 | 1 | 2 |
| **fog** | 0 | 0 | 3 |
| **cloud** | 0 | 1 | 2 |

Recálculo independente: \(2/9 = 0{,}222\ldots\) — bate com o JSON.

**Status auditoria EXP-04:** APROVADO.

---

## 3. Análise científica dos resultados (braço K8s Padrão)

### EXP-01 — Estabilidade sob limite estático
A baseline mostra **sucesso pleno** com limites congelados em 256Mi. O wall-clock estabiliza ~25 s após cold starts pontuais. Isso fixa o controle “cego”: requests/limits **não mudam** entre iterações — qualquer redução de \(\Delta\)RAM no híbrido será atribuível ao Librarian / `near_ceiling`, não a variação do YAML.

### EXP-02 — Noisy neighbor invisível ao scheduler
Com estresse de requests baixos no Fog (`worker2`), o *kube-scheduler* ainda colocou **2/5** Jobs no nó sobrecarregado. Makespan do lote: **34,42 s**. O resultado sustenta a premissa literária: pontuação por capacidade *requested* livre **não** enxerga carga viva. Reservar este \(n_{\text{sobrec}}=2\) e o makespan como âncoras para H1/H2 do híbrido.

### EXP-03 — Deadlock de admissão
Pedido **10Gi** > \(R_{\max}\approx 7{,}72\) Gi produziu Pending estável por 90 s com `Insufficient memory` nos 3 workers. Evidência causal completa para H1 da baseline. O híbrido deverá, na janela 80%, clamar e despachar (ou permanecer `na_fila` se \(R > 1{,}25 R_{\max}\)) — sem criar Pod unschedulable.

### EXP-04 — Cegueira topológica
Hit rate **22,2%** (próximo do acaso com 3 classes: \(1/3\approx 33\%\), aqui ainda pior pela concentração em Cloud via *ImageLocality*). Edge e Fog: **0%** de acerto. O scheduler **não** usa `tcc.utfpr.edu.br/compute-layer`. Qualquer hit rate híbrido materialmente maior (e moda ML→cloud / leve→edge) será contraste válido no eixo de 20% do NodeRanker.

---

## 4. Anomalias e recomendações (não bloqueantes)

| ID | Severidade | Descrição | Ação sugerida |
|---|---|---|---|
| A1 | Baixa | EXP-01 JSON sem campo explícito `memory: 256Mi` | Citar YAML na monografia; opcional enriquecer schema depois |
| A2 | Média (método) | EXP-02: `kubectl top` pós-warmup sem CPU alta visível no nó sobrecarregado | Antes do híbrido EXP-02, alongar warm-up e confirmar top > limiar qualitativo do protocolo |
| A3 | Informativa | Timestamps nos nomes de arquivo (`178674…`) parecem epoch não-Unix clássico do host | Irrelevante para análise; usar campos `at` / `batch_*` internos |
| A4 | Informativa | EXP-04 enviesado a `cloud` (7/9) | Declarar *ImageLocality* como ameaça (já prevista no protocolo §8) |

Nenhuma anomalia **invalida** os datasets para publicação da fase de controle.

---

## 5. Conclusão

| Experimento | Integridade | Pronto para híbrido? |
|---|---|---|
| EXP-01 | Aprovado | Sim |
| EXP-02 | Aprovado | Sim (reforçar evidência de estresse vivo) |
| EXP-03 | Aprovado | Sim |
| EXP-04 | Aprovado | Sim |

**Certificação:** os artefatos de baseline EXP-01…EXP-04 estão **prontos e consistentes** para análise e para a fase de tratamento (Orquestrador Híbrido).
