# Sprint 0 — Setup inicial do ambiente experimental

**Status:** documentação (aguardando aprovação supervisor / Gemini)  
**Não executa código nesta fase.** Scripts de carga e coleta ficam para as sprints seguintes.

---

## 1. Objetivo da Sprint 0

Deixar o laboratório **reproduzível e simétrico** para os quatro experimentos: o mesmo cluster Kind, as mesmas labels Edge/Fog/Cloud, o mesmo Metrics Server e **dois canais de submissão isolados**:

| Canal | Nome na monografia | Caminho |
|---|---|---|
| Tratamento | **Orquestrador Híbrido** | `POST /submit_task` → broker (`context-server`) → NodeRanker → Job K8s |
| Linha de base | **Kubernetes Padrão** (*kube-scheduler*) | `kubectl apply` de Job/ConfigMap **sem** passar pelo broker |

A linha de base segue o modelo clássico descrito em Burns et al., *Borg, Omega, and Kubernetes* (ACM Queue / CACM): o *kube-scheduler* filtra nós viáveis e pontua com plugins default (em especial *NodeResourcesFit* sobre **requests declarados**, não sobre utilização viva). Não é um sorteio uniforme; é um escalonador de **capacidade pedida**. O contraste científico com o orquestrador é exatamente este: o tratamento usa telemetria real, histórico SQLite e topologia Edge/Fog/Cloud.

---

## 2. Hipótese de ambiente (o que deve estar verdadeiro antes do Experimento 1)

1. Cluster Kind com **1 control-plane** (sem execução de tarefas) e **3 workers** tipados.
2. Metrics Server respondendo `kubectl top nodes` / `kubectl top pods`.
3. Namespace dedicado à baseline (`tcc-baseline`) vazio no início de cada experimento.
4. Broker (`context-server` + Ollama) no ar **somente** para o canal híbrido; a baseline **não** chama `/submit_task`.
5. Banco SQLite do broker em estado conhecido (reset de `tasks` / `task_results` quando o experimento exigir histórico limpo).

---

## 3. Cluster Kind: criar, inspecionar, destruir

Manifesto de referência no repositório: `kind-multi-node.yaml`.

Ordem dos nós no manifesto (e nomes típicos gerados pelo Kind, cluster `tcc-cluster`):

| Papel Kind | Hostname esperado | Label `tcc.utfpr.edu.br/compute-layer` |
|---|---|---|
| control-plane | `tcc-cluster-control-plane` | *(sem label de camada; taint `NoSchedule` típico)* |
| 1º worker | `tcc-cluster-worker` | `edge` |
| 2º worker | `tcc-cluster-worker2` | `fog` |
| 3º worker | `tcc-cluster-worker3` | `cloud` |

**Criar (procedimento documentado, não executar agora):**

```text
kind create cluster --name tcc-cluster --config kind-multi-node.yaml
kubectl cluster-info --context kind-tcc-cluster
kubectl get nodes --show-labels
```

**Conferir labels (obrigatório após cada recreate):**

```text
kubectl get nodes -L tcc.utfpr.edu.br/compute-layer
```

Se as labels não coincidirem com a tabela (Kind pode numerar workers de forma diferente em recreates), **reaplicar manualmente** — a fonte da verdade para o NodeRanker é a label, lida pelo watcher e persistida em `nodes.compute_layer`:

```text
kubectl label node <hostname> tcc.utfpr.edu.br/compute-layer=edge --overwrite
kubectl label node <hostname> tcc.utfpr.edu.br/compute-layer=fog --overwrite
kubectl label node <hostname> tcc.utfpr.edu.br/compute-layer=cloud --overwrite
```

**Limpar entre experimentos (cluster intacto, cargas mortas):**

```text
kubectl delete jobs,pods,cm -l app=tcc-task-runner --all-namespaces --ignore-not-found
kubectl delete namespace tcc-baseline --ignore-not-found
kubectl create namespace tcc-baseline
```

**Destruir o cluster (quando a Sprint 0 ou um experimento exigir cluster virgem):**

```text
kind delete cluster --name tcc-cluster
```

O control-plane **não** é alvo de Jobs de tarefa: o manifesto não o rotula para execução e o taint `NoSchedule` impede o *kube-scheduler* de colocar Pods de workload ali, salvo toleration explícita (que **não** deve ser usada nos experimentos).

---

## 4. Metrics Server

Manifesto de referência: `metrics-server.yaml` (imagem `registry.k8s.io/metrics-server/metrics-server:v0.8.1`).

No Kind (certificados autoassinados) o Deployment já deve incluir os argumentos de TLS inseguro típicos desse manifesto. Após aplicar:

```text
kubectl apply -f metrics-server.yaml
kubectl -n kube-system rollout status deploy/metrics-server
kubectl top nodes
```

**Papel no desenho experimental:**

- Canal híbrido: o watcher / telemetria de picos (`cpu_peak_percent`, `ram_peak_mb`) e o vetor de carga do NodeRanker dependem de métricas vivas.
- Canal baseline: o *kube-scheduler* **default não consome** Metrics Server para escolher o nó. Instalá-lo serve para **observar** a mesma telemetria nos dois braços (`kubectl top pod`) e comparar desperdício/estrangulamento com instrumento comum.

Critério de prontidão da Sprint 0: `kubectl top nodes` retorna CPU/RAM para os três workers.

---

## 5. Canal Baseline — Jobs direto no Kubernetes (sem broker)

Objetivo: o *kube-scheduler* ser o **único** decisor de *placement*. Recursos são **estáticos** (requests/limits fixos no YAML), sem `nodeSelector` de hostname e sem afinidade Edge/Fog/Cloud (salvo quando um experimento pedir o contrário, e isso será declarado).

### 5.1 Princípios do artefato baseline

Cada corrida baseline é um `Job` + `ConfigMap` no namespace `tcc-baseline`:

- `restartPolicy: Never`
- `backoffLimit: 0` (espelha o Job do broker)
- `image` idêntica à do tratamento naquele experimento
- `resources.requests` / `limits` **fixos** (não recalculados por histórico)
- **sem** `nodeSelector: kubernetes.io/hostname`
- **sem** label de camada no Pod (o scheduler não vê Edge/Fog/Cloud)
- script montado via ConfigMap em `/app/scripts` (mesmo padrão do `k8s_job_executor.py`)

Labels recomendadas para coleta:

```text
app=tcc-baseline
experiment=<id>          # exp1 | exp2 | exp3 | exp4
run=<n>
arm=k8s-default
```

### 5.2 O que a baseline **não** faz

- Não chama Ollama / Analista / Librarian / Strategist.
- Não grava `tasks` / `task_results` no SQLite do broker.
- Não aplica clamp de 80% nem `near_ceiling`.
- Não injeta vencedor do NodeRanker.

A coleta da baseline será, nas sprints de execução: timestamps do Job/Pod (`creationTimestamp`, `startTime`, `completionTime`), nó (`spec.nodeName`), fase (`Pending`/`Running`/`Succeeded`/`Failed`), `kubectl top` periódico e, se desejado, logs do container. Isso será protocolado por experimento; **não há script nesta pasta ainda**.

### 5.3 Simetria de workload

O **mesmo arquivo Python** usado no `/submit_task` deve ser o `data` do ConfigMap baseline. Diferença permitida: apenas o caminho de submissão e a política de recursos/placement. Qualquer divergência de imagem, comando ou dataset invalida o par tratamento vs. controle.

---

## 6. Canal Híbrido — broker

Pré-condições (Compose / kubeconfig do broker):

- `context-server` com kubeconfig apontando para `kind-tcc-cluster`
- Ollama com o modelo configurado (`OLLAMA_MODEL`, hoje `LiquidAI/lfm2.5-1.2b-instruct`)
- Watcher gravando workers como `provider_type=kubernetes` (ciclo ~10 s)
- `kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes` acessível a partir do broker

Reset de histórico quando o experimento exigir cold start (já existe no repositório `scripts/reset_eval_db.py`; **não executar nesta sprint**): apaga `tasks` + `task_results`, preserva nós e catálogo Docker.

Coleta nativa do tratamento (já implementada):

- Logs `[TCC-METRICS][PLANEJAMENTO]`, `[SCHEDINSPECTOR]`, `[LAYER]`, `[ALOCACAO]`, `[EXECUCAO-FIM]`, `[TELEMETRIA-REAL]`, `[EFICIENCIA]`, `[REALOCACAO]`
- SQLite: `planning_completed_at`, `dispatched_at`, `preferred_compute_layer`, `allocated_compute_layer`, `allocated_node_id`, `prediction_delta_ram`, picos CPU/RAM, `dispatch_meta.best_effort`

---

## 7. Variáveis que devem ser fixadas em todos os experimentos

| Variável de controle | Valor / regra |
|---|---|
| Cluster | Kind `tcc-cluster`, 3 workers tipados |
| Namespace baseline | `tcc-baseline` |
| Imagem de runtime | a mesma nos dois braços, por experimento |
| Modelo LLM | o mesmo em todo o TCC (registrar versão) |
| Taint do control-plane | não tolerar |
| Concorrência extra | zero, salvo o Experimento 2 (estresse deliberado) |
| Ordem dos braços | **baseline primeiro** ou **híbrido primeiro** — escolher e **repetir igual** em todos os experimentos; registrar |

Recomendação de ordem (a validar com o supervisor): em cada cenário, rodar **K8s Padrão** com cluster “frio” e depois **Híbrido**, **ou** recriar o cluster entre braços. Não misturar Jobs baseline e Jobs do broker no mesmo instante, para não contaminar o vetor de carga.

---

## 8. Métricas transversais (aparecem em todos os experimentos)

Definições operacionais para a monografia:

| Métrica | Fórmula / origem | Unidade |
|---|---|---|
| Makespan da corrida | `t_fim − t_submit` (último Pod Succeeded/Failed − submissão) | s |
| Duração de execução | tempo com container Running até terminal | s |
| Tempo até agendar | `startTime − creationTimestamp` (baseline) ou `dispatched_at − submitted_at` (híbrido) | s |
| Tempo em Pending | intervalo na fase Pending; ∞ censurado se abortar por timeout | s |
| Pico CPU / RAM | Metrics Server (ambos) e `task_results` (só híbrido) | % / MB |
| Delta RAM | `RAM_alocada_GB − pico_RAM_GB` (positivo = desperdício) | GB |
| Acerto de camada | `allocated_layer == preferred_layer` (híbrido); na baseline, camada = label do `nodeName` | 0/1 |

Timeout experimental sugerido (a homologar): **180 s** em Pending sem transitar para Running ⇒ registrar como *unschedulable* / falha de alocação, não esperar eternamente.

---

## 9. Ameaças à validade (Sprint 0 já deve mitigar)

- **Capacidade real dos workers Kind** varia com RAM do host Docker. Medir `kubectl describe node` (Allocatable) **antes** do Experimento 3 e anotar no caderno.
- **Cache de imagem** favorece o nó que já puxou a imagem (plugin *ImageLocality* na baseline e peso 25% *warm* no NodeRanker). Reset de DB remove o warm do broker, mas **não** o cache Docker do nó. Para comparação justa de placement, aceitar esse viés ou recriar o cluster entre braços.
- **Contaminação de carga:** um braço não pode deixar Pods rodando quando o outro começa.
- **Aleatoriedade residual do kube-scheduler** quando scores empatam: repetir N corridas (definido por experimento) e reportar moda/proporção, não uma única alocação.

---

## 10. Critério de aceite da Sprint 0 (checklist)

Antes de abrir o Experimento 1, o ambiente está pronto se e somente se:

1. `kubectl get nodes -L tcc.utfpr.edu.br/compute-layer` mostra três workers `edge` / `fog` / `cloud` e control-plane sem carga de tarefa.
2. Metrics Server saudável (`kubectl top nodes` ok).
3. Namespace `tcc-baseline` criado e vazio.
4. Um Job **manual** de fumaça (hello) sobe no baseline e termina `Succeeded`.
5. Uma submissão **manual** via `/submit_task` (hello) termina `concluida` no broker, com log `[TCC-METRICS][ALOCACAO]` e `allocated_node_id` no formato `k8s:{hostname}`.
6. Capacidade allocatable CPU/RAM de cada worker anotada (tabela no caderno experimental).

**Não avançar para as sprints de execução sem aprovação deste documento e dos quatro protocolos.**
