# Experimento 4 — Topologia inteligente (afinidade Edge / Fog / Cloud)

**ID:** EXP-04  
**Arquivo de código sob teste:** `compute_layer.py` (`preferred_layer_for_workload`, `layer_affinity_score`), `node_ranker.py` (peso **20%** `WEIGHT_LAYER`), logs `[TCC-METRICS][LAYER]`  
**Braços:** *kube-scheduler* sem afinidade de camada vs. NodeRanker com eixo ordinal Edge–Fog–Cloud

---

## 1. Pergunta de pesquisa

Em um lote **misto** (scripts leves vs. cargas `ML-heavy`), o orquestrador aloca de acordo com a **topologia declarada** (ML → `cloud`, leves → `edge`/`fog`) com taxa de acerto superior à do escalonador padrão, que **não possui** o rótulo `tcc.utfpr.edu.br/compute-layer` como plugin de score?

O *kube-scheduler* só respeitaria essa topologia se o Pod trouxesse `nodeSelector` / `nodeAffinity` de camada. A baseline deste experimento **omite** essas afinidades de propósito: é o scheduler “de fábrica”, cego à taxonomia do TCC. O tratamento obtém `preferred_layer` do Analista (`task_type`, `complexity_score`) e dos recursos, e soma até **20 pontos** no score:

| Distância ordinal | Pontos (`WEIGHT_LAYER = 20`) |
|---|---|
| match (`d = 0`) | 20 |
| adjacente edge↔fog ou fog↔cloud (`d = 1`) | 10 |
| opostas edge↔cloud (`d = 2`) | 0 |

Camadas dos workers: labels da Sprint 0 (`edge` / `fog` / `cloud`). Control-plane fora.

---

## 2. Hipóteses

**H1 (acerto):** no híbrido, a taxa  
\( \text{hit} = \#\{preferred = allocated\} / N \)  
é maior que a taxa “ao acaso” da baseline, onde `allocated` é a label do `nodeName` e `preferred` é a **mesma regra** `preferred_layer_for_workload` aplicada *offline* ao script (para a baseline ter um alvo comparável, mesmo sem o broker).

**H2 (separação de classes):** a moda dos Jobs **ML-heavy** no híbrido é o worker `cloud`; a moda dos **leves** é `edge` (ou `fog` se o perfil CPU-bound/RAM ≥ 1,5 GB empurrar para fog — ver regra abaixo).

**H3 (baseline):** a distribuição dos mesmos scripts no K8s Padrão **não** se alinha à classe de workload (próxima de uniforme entre os 3 workers, ou enviesada por *ImageLocality*/capacidade, mas **não** por ML vs. hello).

**H0:** hit rate híbrido ≈ hit rate baseline (o eixo de 20% é dominado por carga 40% + imagem 25% + disco 15%).

A H0 é **plausível**: *warm* de 25% pode superar camada de 20%. Por isso o protocolo **zera histórico de imagem** (reset SQLite no início do lote híbrido). **Não** usar imagens distintas por classe: isso confunde o eixo de camada com *ImageLocality* e com o peso \(w_i\).

**Controle de imagem e cache (obrigatório na recorre):**

- Imagem única nos dois braços e nas nove tarefas: `python:3.11-slim`.
- Pré-pull dessa imagem nos três workers **antes** de cada braço, para equalizar o estado inicial de cache.
- Híbrido: após o planejamento (status `na_fila`), **fixar** `docker_image` e `k8s_job_spec.image` em `python:3.11-slim` (procedimento de laboratório, análogo à injeção de memória do EXP-03). Sem o pin, o Librarian escolhe `tensorflow/tensorflow:latest` nas tarefas ML.
- Os scripts ML mantêm `import tensorflow` no AST (marcador de classificação); em runtime o import é `try/except` e a carga útil é Python puro.
- **Não** resetar *warm* entre tarefas. Após a 1ª execução, o eixo \(w_i\) (peso 25) pode aparecer no ranking; documentar como fator residual, sem invalidar o experimento.

Controle: **mesma imagem → mesmo pré-pull → mesmo estado inicial de cache → diferença relevante no placement = mecanismo de decisão**.

---

## 3. Regra de preferência (código vigente — não reinventar na monografia)

`preferred_layer_for_workload`:

| Condição (primeira que casar, na ordem do código) | Camada |
|---|---|
| `task_type == ML-heavy` **ou** categoria com `ml` / `tensorflow` / `keras` | **cloud** |
| `complexity_score ≥ 8` | **cloud** |
| RAM ≥ 4 GB **ou** CPU ≥ 4 | **cloud** |
| `complexity_score ≥ 5` | **fog** |
| `CPU-bound` **ou** RAM ≥ 1,5 GB **ou** CPU ≥ 2 | **fog** |
| demais (hello, IO leve, complexity baixa) | **edge** |

O Analista só marca `ML-heavy` com import explícito de TF/torch/keras/sklearn. Os scripts do lote devem **forçar** essa distinção de forma legível no AST (não “ML” só no nome do arquivo).

Scripts já previstos no repositório (reutilizar na execução, não reescrever agora):

| Papel no lote | Arquivo de referência | Preferência esperada |
|---|---|---|
| Leve | `scripts/eval_tasks/task_edge_light.py` | `edge` |
| Intermediário | `scripts/eval_tasks/task_fog_cpu.py` | `fog` |
| Pesado / ML | `scripts/eval_tasks/task_cloud_ml.py` | `cloud` |

---

## 4. Cenário

**Lote misto sugerido (N = 9, múltiplo das 3 classes):** 3 leves + 3 fog/CPU + 3 ML, **em ordem intercalada** (L, F, M, L, F, M, …) para não vazar um burst só de ML no mesmo nó por fila.

**Serial vs. paralelo:** preferir **serial** (uma tarefa termina antes da próxima) neste experimento, para **não** confundir com o Experimento 2. O eixo é topologia, não noisy neighbor. Submissão paralela misturaria `dvCpu` no score e enfraqueceria o peso de 20%.

**Reset DB** antes do lote híbrido: elimina `warm_count` da Fase 1 histórica.

**Baseline:** 9 Jobs equivalentes, sem label de camada, mesma ordem, **mesma imagem** `python:3.11-slim` (já pré-puxada nos três workers).

---

## 5. Procedimento

### 5.1 Calibrar o alvo *offline* (os dois braços)

Antes de submeter, para cada script anotar o `preferred_layer` **esperado pela regra da §3** (e, no híbrido, conferir depois o campo `tasks.preferred_compute_layer` e o Analista em `multi_agent_trace`). Se o LLM classificar um hello como `ML-heavy`, isso é **falha de Analista**, não do NodeRanker — separar na tabela: `preferred_regra` vs `preferred_ia` vs `allocated`.

Métrica principal de **ranker:** `allocated` vs `preferred_ia` (o que o sistema realmente otimizou).  
Métrica auxiliar de **pipeline completo:** `allocated` vs `preferred_regra`.

### 5.2 Braço Kubernetes Padrão

1. Pré-pull de `python:3.11-slim` nos três workers.
2. Aplicar os 9 Jobs (serial), sem afinidade de camada, imagem `python:3.11-slim`.
3. Mapear `nodeName` → label `tcc.utfpr.edu.br/compute-layer`.
4. Calcular hit vs `preferred_regra` (a baseline não tem IA).

### 5.3 Braço Orquestrador Híbrido

1. Pré-pull de `python:3.11-slim` nos três workers (mesmo estado de cache da baseline).
2. Reset SQLite (*cold start*).
3. Nove `/submit_task` seriais. Após cada planejamento (`na_fila`), pin da imagem para `python:3.11-slim`; só então a fila despacha.
4. Para cada uma: `[TCC-METRICS][LAYER] preferred=… chosen=… hostname=… score_layer=… score_total=…` e `[SCHEDINSPECTOR]`.
5. `score_layer` esperado: 20 no match, 10 adjacente, 0 oposto. Registrar `score_image_affinity` / `warm_task_count` como fator residual de \(w_i\).

Se o worker `cloud` estiver unschedulable (falta de recursos para o spec ML), o ranker escolhe fog (10 pontos) — **acerto adjacente**, não falha total. Reportar hit **exato** e hit **adjacente** (`d ≤ 1`) como duas taxas.

---

## 6. Métricas e comparação

| Métrica | Baseline | Híbrido |
|---|---|---|
| Hit exato (3 classes) | label(node) vs regra | `allocated_compute_layer` vs `preferred_compute_layer` |
| Hit adjacente | \(d \le 1\) | idem |
| Matriz 3×3 preferred × allocated | sim | sim |
| `score_layer` do vencedor | N/A | 0 / 10 / 20 |
| Fração ML no worker cloud | contar | contar |
| Fração leves no worker edge | contar | contar |
| Makespan / duração | controle (não é o DV principal) | controle |

Comparação estatística simples (N=9 é pequeno): **não** forçar p-valor como resultado único. Usar:

- diferença absoluta de hit rate;
- matriz de confusão de camadas;
- se houver tempo, repetir o lote 3 vezes (N=27) após recriar cluster ou reset, para ver estabilidade.

Chance uniforme em 3 nós: hit exato esperado ≈ 33%. O tratamento precisa **superar claramente** esse piso (ex. ≥ 6/9 exatos, ou ≥ 8/9 adjacentes) para ser argumentável na banca.

---

## 7. Critérios de sucesso

- **Forte:** hit exato híbrido ≥ 67% (6/9) **e** todos os ML em `cloud` **e** nenhum leve em `cloud` (opostas).
- **Moderado:** hit adjacente ≥ 89% (8/9) com `score_layer` ≥ 10 no vencedor; ML nunca em `edge`.
- **Fraco / H0:** ML e hello no mesmo worker com a mesma frequência da baseline — o peso 20% não aparece no ranking (`score_layer` irrelevante frente a `score_resources`).

A baseline **não precisa ser pior em duração**. Ela precisa ser **não-informativa quanto à camada**. Se por acaso o scheduler colocar ML no maior nó e esse nó for o `cloud` (heurística de capacidade), anotar: o acerto baseline seria **espurio** (capacidade, não label). Por isso a matriz e o `score_layer` importam mais que o hit isolado.

---

## 8. Ameaças

- **Confundir com Experimento 2:** lote serial.
- **Warm 25% > layer 20%:** reset DB no início; não reutilizar a série do Experimento 1 no mesmo banco. Após \(k=1\), \(w_i\) pode acumular — fator residual, não confundir com *ImageLocality* da baseline (equalizada pelo pré-pull).
- **Assimetria de imagem (recorre):** **proibida**. Não usar `tensorflow/tensorflow:latest` só no híbrido.
- **Taint / cloud no control-plane:** o manifesto atual coloca `cloud` num **worker**. Não usar análises antigas que citavam `tcc-cluster-control-plane` como cloud (`docs/roteiro_avaliacao.md` está desatualizado nesse ponto).
- **Spec ML maior que o worker cloud:** vira Experimento 3 disfarçado. Conferir que o Job ML **cabe** no allocatable do worker `cloud` **sem** clamp. Se não couber, o ranker irá para fog — enviesa H2.
- **Classificação errada do Analista:** coluna separada na tabela; não culpar o NodeRanker.

---

## 9. Relação com os outros experimentos (para não misturar narrativas)

| Exp. | Eixo | O que **não** reivindicar aqui |
|---|---|---|
| 1 | limits ao longo do tempo | hit de camada |
| 2 | desvio de carga / makespan | topologia |
| 3 | admissão oversize | afinidade |
| 4 | peso 20% + `preferred_layer` | “K8s é aleatório” — ele não é; é cego à label TCC |

---

## 10. Artefatos da sprint de execução (não agora)

- Matriz preferred × allocated (baseline e híbrido).
- Nove linhas `[TCC-METRICS][LAYER]`.
- `kubectl get nodes -L tcc.utfpr.edu.br/compute-layer` no dia da coleta.
- Nota se `score_image_affinity` foi zero (confirmação de que o warm não dominou).
