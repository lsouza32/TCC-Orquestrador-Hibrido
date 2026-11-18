# Experimento 1 — Convergência e desperdício (aprendizado contínuo)

**ID:** EXP-01  
**Arquivo de código sob teste:** `ai_service.py` (Librarian + `apply_history_safety_margin`), `tasks_crud.py` (`success_rate`, `near_ceiling` 85%), `persist_effective_k8s_dispatch`  
**Braços:** Kubernetes Padrão (limites estáticos) vs. Orquestrador Híbrido (malha SQLite)

---

## 1. Pergunta de pesquisa

O Librarian, alimentado por `task_results` (picos reais) e pelo `k8s_job_spec` **efetivo** gravado no despacho, **reduz o desperdício de CPU/RAM** ao longo de reexecuções da **mesma** tarefa, em relação a um Job Kubernetes com requests/limits **congelados**?

Não se afirma Q-learning nem política ótima. O mecanismo implementado é **calibragem histórica em malha fechada**:

```
execução → picos + status no SQLite
        → success_rate por imagem × libs
        → near_ceiling se pico ≥ 85% do limit alocado
        → margem 1.25× (uma vez, teto 2.0× do piso)
        → novo k8s_job_spec na iteração seguinte
```

---

## 2. Hipóteses

**H1 (desperdício):** no braço híbrido, o delta de RAM  
`ΔRAM = RAM_limit_GB − pico_RAM_GB`  
**diminui ou estabiliza** entre a 1ª e a 10ª corrida, após o histórico existir (`samples > 0`).

**H2 (imagem):** a partir da 2ª corrida, `docker_image` permanece a da 1ª (catálogo + `success_rate`), salvo falha de execução.

**H3 (baseline):** o Job K8s com limits estáticos **não altera** requests/limits entre as 10 corridas; o desperdício médio permanece da ordem da folga inicial (superdimensionamento constante) **ou** o estrangulamento permanece se os estáticos forem justos demais.

**H0:** não há diferença sistemática de `ΔRAM` médio (corridas 6–10) entre os braços, após controlar a mesma carga.

---

## 3. Variáveis

| Tipo | Variável | Operacionalização |
|---|---|---|
| Independente | Política de dimensionamento | `k8s-static` vs `hybrid-rl` |
| Independente | Iteração | \(k = 1 \ldots 10\), **sequencial** (a \(k+1\) só começa após terminal da \(k\)) |
| Dependente | `ΔRAM`, `ΔCPU` | limit alocado vs pico observado |
| Dependente | `success_rate` da imagem | só híbrido; extraído do JSON do Librarian / CRUD |
| Dependente | `near_ceiling` | flag e `near_ceiling_ratio` no `ceiling_signals` |
| Dependente | `planning_s` | só híbrido (`[TCC-METRICS][ALOCACAO]`) |
| Controle | Script, imagem baseline, cluster, ausência de concorrência | Sprint 0 |

A sequência é **obrigatoriamente serial**. Concorrência misturaria o Experimento 2 (carga) com este (aprendizado).

---

## 4. Cenário

**Workload:** um único script representativo de carga **mensurável** (CPU e RAM não triviais), com bibliotecas estáveis no AST — para o JOIN por libs do Librarian ter interseção. Candidatos já existentes no repositório (a escolher na sprint de execução, sem implementar agora):

- sonda com dependência de catálogo (`scripts/eval_tasks/task_requests_probe.py`), ou
- carga média CPU (`task_fog_cpu.py`),

desde que o pico de RAM seja **reprodutível** o bastante para o 85% fazer sentido.

**N = 10** submissões **idênticas** por braço.

**Cold start híbrido:** SQLite sem `tasks`/`task_results` anteriores (reset). A corrida \(k=1\) é o **pior caso** do tratamento (Librarian sem amostras; piso de catálogo + p95 vazio). As corridas \(k \ge 2\) é onde H1/H2 podem aparecer.

**Baseline estática:** escolher **um** par request/limit **antes** da série e **não mudar**. Duas opções (declarar qual no caderno):

1. **Folga clássica** — limits iguais ao piso de catálogo da imagem (ex.: mínimos `min_cpu` / `min_ram_gb` da tabela Docker), típico “pede folgado e esquece”.
2. **Espelho da 1ª predição híbrida** — copiar o `k8s_job_spec` da corrida híbrida \(k=1\) e congelá-lo nas 10 baseline. Compara “IA uma vez” vs “IA que reaprende”.

Recomendação para a monografia: **opção 2** como comparação principal (mesmo ponto de partida) e, se houver tempo, opção 1 como sensibilidade.

---

## 5. Procedimento (protocolo, ainda sem script)

### 5.1 Braço Kubernetes Padrão

1. Cluster no estado Sprint 0; namespace `tcc-baseline` vazio.
2. Aplicar Job+ConfigMap com limits **fixos**, sem `nodeSelector`.
3. Esperar `Succeeded` ou timeout.
4. Registrar: `nodeName`, duração, picos via `kubectl top pod` (amostra durante Running), requests/limits do YAML (constantes).
5. Apagar o Job (TTL ou `kubectl delete`) **antes** da próxima corrida, para não acumular carga.
6. Repetir até \(k=10\).

O *kube-scheduler* pode mudar o **nó** entre corridas; isso é esperado e deve ser anotado. O que **não** muda são os recursos pedidos — é isso que define o desperdício estático.

### 5.2 Braço Orquestrador Híbrido

1. Reset de `tasks`/`task_results`.
2. `POST /submit_task` com o mesmo arquivo.
3. Aguardar `concluida` / `falha`.
4. Extrair da corrida \(k\):
   - `[TCC-METRICS][PLANEJAMENTO]` (CPU/RAM req/lim)
   - `[TCC-METRICS][EFICIENCIA]` (`Estimado IA` vs `Pico RAM`, `Delta`)
   - `[TCC-METRICS][TELEMETRIA-REAL]`
   - `librarian_output.piso_hardware`, `safety_margin.applied`, `image_success_stats`, `ceiling_signals`
   - `dispatch_meta` (spec efetivo pós-fila)
5. Só então submeter \(k+1\).

---

## 6. Métricas e comparação

| Métrica | K8s Padrão | Híbrido | Como comparar |
|---|---|---|---|
| RAM/CPU pedidos na iteração \(k\) | constante | pode variar (piso, margem 1.25×, clamp de planejamento 8 CPU / 8 GB) | série temporal |
| Pico RAM/CPU | Metrics Server | Metrics Server + `task_results` | mesma unidade |
| `ΔRAM_k` | limit estático − pico | limit **efetivo** − pico | gráfico \(k\) vs `ΔRAM`; média \(k=1..3\) vs \(k=8..10\) |
| Taxa de sucesso | Succeeded/10 | `concluida`/10 | igualdade desejável (aprendizado não deve quebrar a tarefa) |
| `near_ceiling` | N/A | flag por iteração | deve aparecer se o pico encostar em 85% do limit |
| `success_rate` da imagem | N/A | 1.0 após sucessos | H2 |
| Superdimensionamento relativo | \(\Delta / \text{limit}\) | idem | % de folga |

**Leitura esperada (se H1 valer):**

- \(k=1\) híbrido: folga possivelmente grande (catálogo) **ou** justa demais (aí `near_ceiling` dispara em \(k=2\)).
- Se picos ≥ 85% do limit: margem **+25%** no piso da iteração seguinte (não composta: uma aplicação por planejamento, teto 2×).
- Se picos bem abaixo do limit: o código **não reduz** o piso automaticamente (não há “shrink” simétrico ao boost). Isso deve ser reportado com honestidade: a malha atual é **anti-estrangulamento** (`near_ceiling` → sobe recurso), não um controlador que aperta folga ociosa. O desperdício cai sobretudo quando a 1ª alocação era **subdimensionada** e depois estabiliza, ou quando se compara com uma baseline **grosseiramente superdimensionada** (opção 1).

Essa limitação é parte do estado da arte do código e deve constar na discussão da monografia — não é falha do experimento; é o mecanismo real.

---

## 7. Critérios de sucesso científico

O experimento **corrobora** a tese do Librarian se **pelo menos um** dos quadros abaixo ocorrer, com os dados brutos anexados:

1. **Anti-OOM / anti-throttle:** baseline estática justa demais falha ou estrangula; o híbrido, após `near_ceiling`, sobe o limit e as corridas seguintes concluem com pico < 85% do novo teto.
2. **Estabilização informada:** a partir de \(k \ge 3\), imagem constante e `ΔRAM` do híbrido ≤ `ΔRAM` da baseline opção 1 (catálogo folgado), com mesma taxa de sucesso.
3. **Rastro causal:** logs mostram `ceiling_signals.near_ceiling=true` → `safety_margin.applied=true` → `cpu_limit`/`memory_limit` efetivos maiores na iteração seguinte.

O experimento **não** corrobora “RL clássico” nem “convergência a ótimo de desperdício” se apenas H2 (mesma imagem) ocorrer sem movimento de recursos — nesse caso reportar como **warm start de imagem**, que já é resultado (peso 25% no ranker + Librarian), mas não prova eficiência de CPU/RAM.

---

## 8. Ameaças e controles

- **Variância do pico:** uma única amostra de Metrics Server subestima pico. No híbrido os picos são mesclados ao longo do `Running` (`k8s_watcher`). Na baseline, amostrar `kubectl top` em intervalo curto (ex. 2 s) durante toda a execução, ou aceitar viés e declará-lo.
- **Histórico cruzado:** não rodar outros experimentos no mesmo SQLite antes desta série.
- **Clamp 8 GB / 8 CPU** no planejamento: se o piso estourar esse teto, a série satura — anotar.
- **Warm de camada/nó:** este experimento **não** avalia placement; o nó pode repetir por *ImageLocality* / warm. Não interpretar hostname estável como prova do Librarian.

---

## 9. Artefatos a produzir na sprint de execução (não agora)

- Tabela 10×2 (iteração × braço): limits, picos, deltas, status, imagem, `near_ceiling`.
- Gráfico `ΔRAM` vs \(k\).
- Trechos de `[TCC-METRICS][PLANEJAMENTO]` e `[EFICIENCIA]` das corridas 1, 2 e 10.
- JSON bruto do `librarian_output` (já persistido em `ai_raw_response`).
