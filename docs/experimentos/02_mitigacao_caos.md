# Experimento 2 — Mitigação de caos (balanceamento vs. noisy neighbor)

**ID:** EXP-02  
**Arquivo de código sob teste:** `node_ranker.py` (vetor de desvio de carga), `queue_processor.py` (re-ranking na fila), `strategist_guard.py`  
**Braços:** *kube-scheduler* default vs. NodeRanker (40% recursos / desvio de carga)

---

## 1. Pergunta de pesquisa

Diante de um **vizinho ruidoso** (um worker com CPU viva alta), o NodeRanker **desvia** tarefas concorrentes para nós relativamente ociosos, reduzindo o **tempo total de execução (makespan)** do lote em relação ao escalonador padrão do Kubernetes?

Fundamento na literatura: o *kube-scheduler* pontua principalmente por **fração de recursos *requested* ainda livres** (*LeastAllocated* / *NodeResourcesFit*). Um processo que consome CPU **sem** requests equivalentes (ou um estresse que cabe nos requests já existentes) **não** desloca a pontuação default. O NodeRanker, ao contrário, constrói

\[
dvCpu_i = lCpu_{avg} - lCpu_i,\quad
dvMem_i = lMem_{avg} - lMem_i
\]

sobre carga **efetiva** (telemetria 0–100). Nó acima da média de carga recebe \(dv < 0\) e é penalizado no eixo de 40 pontos.

---

## 2. Hipóteses

**H1 (placement):** no braço híbrido, a **moda** dos hostnames das 5 tarefas **não** coincide com o nó estressado (ou a fração nele é menor que na baseline).

**H2 (makespan):** o tempo até a última tarefa do lote terminar é **menor** no híbrido do que na baseline, nas mesmas 5 cargas e no mesmo perfil de estresse.

**H3 (vetor):** o log `[TCC-METRICS][SCHEDINSPECTOR] Vetor NodeRanker` mostra o nó sobrecarregado com `dvCpu` negativo e score de recursos inferior aos ociosos.

**H0:** a distribuição de `nodeName` e o makespan não diferem além da variação de empate do scheduler.

---

## 3. Variáveis

| Tipo | Variável | Operacionalização |
|---|---|---|
| Independente | Política de placement | `k8s-default` vs `noderanker` |
| Independente | Nó sobrecarregado | um worker escolhido a priori (registrar hostname + camada) |
| Independente | Intensidade do estresse | Job `stress` pinado nesse nó, CPU alta sustentada |
| Dependente | Hostname de cada uma das 5 tarefas | `spec.nodeName` / `allocated_node_id` |
| Dependente | Makespan do lote | \(t_{\text{última terminal}} - t_{\text{primeira submissão}}\) |
| Dependente | Duração individual | `duration_seconds` / Job completion |
| Dependente | `dvCpu`, `lCpu`, `score_resources` | só híbrido, Top 3 do ranker |
| Controle | Mesmo quinteto de scripts; sem histórico de warm enviesando (reset DB); Metrics Server ok |

Peso do eixo sob teste: **40/100**. Afinidade de imagem (25) e camada (20) devem ser **neutralizadas** o máximo possível: mesma imagem para as 5 tarefas, workloads com `preferred_layer` semelhante, SQLite sem `warm_count` da imagem. Caso contrário o desvio de carga pode perder para warm/camada — declarar se não for possível.

---

## 4. Cenário

1. **Estresse:** um Pod/Job permanente (ou de longa duração) **fixado** com `nodeSelector: kubernetes.io/hostname: <nó-sobrecarregado>`, consumindo CPU de forma agressiva (ex. `stress-ng` / loop busy). Requests do estresse devem ser **modestos ou nulos** de propósito: assim o *kube-scheduler* ainda vê o nó como “com capacidade pedida livre”, que é exatamente o *noisy neighbor* clássico. (Se o estresse pedir 100% da CPU em *requests*, a baseline também evitaria o nó — o contraste desaparece.)

2. **Carga útil:** **5 tarefas concorrentes** (submissão em rajada, intervalo ≈ 0 entre elas), CPU-bound leves/médias, **idênticas** entre si para isolar o efeito do nó sobrecarregado.

3. O estresse **permanece ativo** durante todo o lote nos dois braços.

4. **Não** usar o control-plane. Escolher um dos três workers; repetir o protocolo é válido com outro worker como checagem de robustez (opcional).

---

## 5. Procedimento

### 5.1 Preparação comum

1. Sprint 0 ok; `kubectl top nodes` mostra o nó sobrecarregado com CPU claramente acima dos outros **depois** de subir o estresse (critério qualitativo: ocupação visivelmente maior, ex. > 70% vs. < 30% nos demais — ajustar ao allocatable real).
2. Reset SQLite no braço híbrido.
3. Não deixar Jobs de experimentos anteriores.

### 5.2 Braço Kubernetes Padrão

1. Subir o estresse pinado.
2. Confirmar `kubectl top nodes`.
3. Aplicar **5 Jobs** baseline **sem** `nodeSelector` (o scheduler escolhe).
4. Não espaçar as criações (rajada).
5. Coletar `nodeName`, `startTime`, `completionTime`, fase.
6. Encerrar o lote; **manter o estresse** se o braço híbrido for em seguida no mesmo cluster, **ou** recriar o estresse após wipe — o importante é a mesma intensidade.

Espera-se que **parte** dos 5 Jobs caia no nó sobrecarregado: os requests da carga útil ainda “cabem”. Não é necessário que os 5 caiam lá; a métrica é a **contagem** nesse hostname vs. o híbrido.

### 5.3 Braço Orquestrador Híbrido

1. Mesmo estresse, mesmo instante relativo (`kubectl top` comparável).
2. Cinco `POST /submit_task` em rajada.
3. A fila reconsulta `get_active_nodes` e re-ranqueia **por tarefa**. A 1ª pode ver carga X; a 5ª já pode ver as anteriores despachadas — isso é realista e deve ser relatado (efeito de *online* vs. *batch snapshot*).
4. Grep `[TCC-METRICS][SCHEDINSPECTOR]` e `[ALOCACAO]` das cinco.

**Atenção de implementação:** o NodeRanker filtra nós cujo CPU/RAM **efetivos** < pedido. Se o estresse reduzir a capacidade efetiva abaixo do request da tarefa, o nó sobrecarregado sai do pool (filtro duro) — resultado ainda mais forte, mas aí o contraste deixa de ser só o vetor \(dvCpu\) e passa a ser também o filtro. Anotar se o nó sobrecarregado foi **eliminado** ou apenas **rebaixado**.

---

## 6. Métricas e comparação

| Métrica | Baseline | Híbrido | Interpretação |
|---|---|---|---|
| \(n_{\text{sobrec}}\) | quantas das 5 no nó estressado | idem | H1: híbrido < baseline |
| Makespan do lote | s | s | H2 |
| Soma das durações | s | s | suporte; pode divergir do makespan se houver paralelismo |
| Tempo até Running | Pending delay | `queue_wait_s` + dispatch | o híbrido pode ser mais lento para **decidir** (IA) — reportar separado do tempo de **execução** |
| Vetor Top 3 | N/A | `dvCpu`, `lCpu`, `score_total` | H3, evidência causal |
| Camada alocada | label do nó | `allocated_compute_layer` | controle (não é o eixo deste exp.) |

**Separar latência de planejamento** (`planning_s`, só híbrido) do makespan de **execução no cluster**. A tese de balanceamento fala de execução, não de tempo de LLM. Reportar:

- Makespan **de cluster** (do 1º Pod Running ou do 1º Job created até o último Succeeded).
- Makespan **ponta a ponta** (1ª submissão HTTP/kubectl até o último terminal) — o híbrido inclui Ollama e será maior; não usar isso como prova de pior scheduling.

---

## 7. Critérios de sucesso

Corroboração **forte:** \(n_{\text{sobrec, híbrido}} = 0\) e H3 visível nos logs, com makespan de cluster ≤ baseline.

Corroboração **moderada:** \(n_{\text{sobrec, híbrido}} < n_{\text{sobrec, baseline}}\) e durações individuais no nó sobrecarregado (baseline) visivelmente maiores (estrangulamento).

**Falha em rejeitar H0:** mesma distribuição de nós (ex. todos no mesmo worker por *ImageLocality* ou empate de score) — nesse caso o experimento ainda é útil: documenta quando o eixo de 40% **não** supera cache de imagem / requests iguais.

---

## 8. Ameaças

- **ImageLocality** (baseline) e **warm 25%** (híbrido) puxam para o nó que já tem a imagem — possivelmente o nó sobrecarregado se o estresse usou a mesma imagem. Usar imagem de estresse **distinta** da carga útil.
- **Métricas atrasadas:** cache allocatable 45 s e ciclo do watcher 10 s. Submeter a rajada só após `kubectl top` estável.
- **Kind em um único host físico:** o “nó ocioso” compete pelo mesmo CPU do laptop. O desvio ainda é válido **dentro do cluster** (cgroups), mas o ganho de makespan pode ser menor que em metal separado. Declarar o hardware.
- **Cinco tarefas e três nós:** empates são frequentes. Preferir reportar contagens, não um único “vencedor”.

---

## 9. Artefatos da sprint de execução (não agora)

- Tabela 5 tarefas × 2 braços: hostname, duração, start/end.
- Snapshot `kubectl top nodes` imediatamente antes da rajada.
- Três linhas `[SCHEDINSPECTOR] Vetor NodeRanker` (primeira, terceira e quinta tarefa).
- Makespan de cluster vs. ponta a ponta, em parágrafo separado.
