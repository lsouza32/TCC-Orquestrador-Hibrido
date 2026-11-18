# Experimento 3 — Resiliência extrema (anti-deadlock e oversize)

**ID:** EXP-03  
**Arquivo de código sob teste:** `queue_processor.py` (`_rank_with_optional_best_effort`, `_clamp_k8s_spec_to_capacity`), `node_ranker.max_effective_resources_among`  
**Braços:** Pod Kubernetes `Pending` (não cabe) vs. fila híbrida com **clamp best-effort (regra dos 80%)**

---

## 1. Pergunta de pesquisa

Quando o pedido de RAM (ou CPU) **excede a capacidade efetiva do maior worker**, o orquestrador **evita o deadlock de agendamento** (Pod eterno em `Pending`) e ainda assim **despacha** um Job executável, ao passo que o *kube-scheduler* recusa o binding?

Este é o experimento de **resiliência de admissão**, não de ótimo de placement.

---

## 2. O que o código realmente faz (pré-condição obrigatória)

O clamp **não** é “sempre corta até o teto do cluster”. Em `_rank_with_optional_best_effort`:

1. Ranqueia com o pedido original. Se algum nó cabe → despacha **sem** clamp.
2. Se ninguém cabe, calcula o teto efetivo \((C_{\max}, R_{\max}, D_{\max})\).
3. Define o piso de tolerância:

\[
floor = 0{,}80 \times required
\]

4. **Se** \(C_{\max} < floor_{cpu}\) **ou** \(R_{\max} < floor_{ram}\) (ou disco, se pedido > 0) → **não cria Job**; a tarefa **permanece `na_fila`**. Não há Pod `Pending`.
5. **Senão** clampa:

\[
required' = \max(floor,\; \min(required,\; max\_efetivo))
\]

   alinha `cpu_limit`/`memory_limit` do spec, **re-ranqueia** e despacha. `dispatch_meta.best_effort=true`.

Consequência para o desenho: o pedido oversize \(R\) tem de satisfazer, **medido no cluster real da Sprint 0**:

\[
R_{\max} < R \le \frac{R_{\max}}{0{,}80} = 1{,}25\, R_{\max}
\]

(o análogo vale para CPU se o eixo oversize for CPU).

**Exemplo:** se o maior worker tem **8 GiB** allocatable efetivo, um pedido de **10 GiB** está na janela (8 < 10 ≤ 10). Se o maior worker tem **4 GiB**, **10 GiB está fora** (floor = 8 > 4): o híbrido **espera na fila** e **não** “força a execução”. Nesse hardware o protocolo deve usar, por exemplo, **4,5–5,0 GiB**, não 10 GiB.

O enunciado “tarefa de 10 GB” só é válido após a tabela de allocatable da Sprint 0 confirmar que 10 GB ∈ \((R_{\max},\; 1{,}25 R_{\max}]\). Caso contrário, **redimensionar o pedido** e registrar o valor usado. Não alterar o código para caber o exemplo.

Há ainda o teto de **planejamento** `RAM ∈ [0,25; 8]` GB em `analyze_task_requirements`. Se o Strategist/Librarian nunca emitir > 8 GB, o oversize precisa ser induzido de outro modo (ver §4).

---

## 3. Hipóteses

**H1 (baseline):** Job com `memory: 10Gi` (ou o \(R\) calibrado) **sem** `nodeSelector`, em worker cujo allocatable < \(R\), permanece `Pending` / `Unschedulable` até o timeout experimental (180 s), com evento `Insufficient memory`.

**H2 (tratamento, janela 80%):** a mesma demanda, submetida ao broker, gera log de clamp (`alocação ajustada dentro da tolerância de 80%`), `best_effort` no `dispatch_meta`, Job **Running/Succeeded** (ou Falha de *aplicação*, não de scheduling), e `memory_limit` efetivo ≤ \(R_{\max}\).

**H3 (anti-Pending):** não existe Pod do tratamento em `Pending` > 60 s por falta de memória (o watcher trataria Unschedulable com realocação; o sucesso deste exp. é **nem criar** o Pod unschedulable).

**H0:** ambos os braços ficam indefinidamente sem executar (Pending vs. `na_fila` eterna) — ocorre se \(R\) estiver **acima** da janela 1,25×, ou se o planejamento capar o pedido para um valor que **já cabe** (aí não há oversize real).

---

## 4. Como produzir o oversize nos dois braços

O Librarian tende a não pedir 10 GB (teto 8 GB no planejamento). Para o experimento ser sobre a **fila**, não sobre o LLM:

| Braço | Como fixar o pedido oversize |
|---|---|
| K8s Padrão | YAML com `resources.limits.memory` / `requests.memory` = \(R\) calibrado, **maior** que \(R_{\max}\) |
| Híbrido | Garantir que `required_memory_gb` e o `k8s_job_spec` persistido **antes da fila** sejam \(R\). Na sprint de execução isso pode ser: (a) tarefa cujo catálogo/mínimos já sejam altos **e** injeção pontual no SQLite/`ai_raw_response` **após** o planejamento e **antes** do `process_queue_job` (procedimento de laboratório, não mudança permanente de código); ou (b) usar o teto 8 GB se \(R_{\max} < 8\) e \(8 \le 1{,}25 R_{\max}\). |

A opção (b) é a mais limpa se a Sprint 0 mostrar, por exemplo, workers com ~6–7 GiB efetivos: o próprio teto de 8 GB já é oversize **dentro** da janela 80%. Preferir (b) sempre que a matemática fechar — menos intervenção manual.

**Não** usar `nodeSelector` na baseline: o teste é “o cluster inteiro não tem nó que satisfaz o request”.

---

## 5. Cenário

1. Medir \(R_{\max}\) efetivo (allocatable − DaemonSets; na prática `kubectl describe node` Allocatable memory, e o que o NodeRanker vê como `effective_memory_gb` com telemetria). Usar o **mínimo** entre allocatable K8s e efetivo do ranker, para não “passar” no filtro duro por um número que o kubelet recusa.
2. Escolher \(R\) na janela da §2.
3. Workload: script **leve** (hello / sonda curta). O objeto do teste é **agendar**, não treinar ML. Se o clamp cortar RAM, o script ainda deve caber no teto (hello cabe; um treino TF pode morrer por OOM — isso seria outro fenômeno, contaminante).
4. Um único Job por braço (N=1 é aceitável para evidência qualitativa; repetir 3 vezes só para confirmar que Pending não é corrida).

---

## 6. Procedimento

### 6.1 Braço Kubernetes Padrão

1. `kubectl apply` Job com memória \(R\).
2. Observar 180 s.
3. Capturar: `kubectl describe pod` (Events: `FailedScheduling`, `Insufficient memory`), fase `Pending`, `nodeName` vazio.
4. Abortar e apagar (não deixar Pending eterno no cluster).

### 6.2 Braço Orquestrador Híbrido

1. Submeter (ou injetar \(R\) conforme §4) e deixar a fila processar.
2. Evidências obrigatórias:
   - log `Fila: tarefa … alocação ajustada dentro da tolerância de 80%`
   - `dispatch_meta.best_effort`
   - spec efetivo com `memory_limit` parseado ≤ \(R_{\max}\)
   - status `executando` → `concluida` (script leve)
   - `[TCC-METRICS][ALOCACAO]` com hostname
3. Controle negativo (opcional, mas recomendado ao supervisor): uma segunda submissão com \(R' > 1{,}25 R_{\max}\) deve **permanecer `na_fila`** e **não** criar Job — prova que o 80% não é “executa a qualquer custo”.

---

## 7. Métricas e comparação

| Métrica | K8s Padrão | Híbrido (janela 80%) | Híbrido (acima de 1,25×) |
|---|---|---|---|
| Fase aos 180 s | `Pending` | `Succeeded` / `concluida` | `na_fila` (sem Pod) |
| Binding | nenhum | `k8s:{hostname}` | nenhum |
| Memória do spec | \(R\) (inalterada) | \(required' \le R_{\max}\) | \(R\) inalterado, sem Job |
| Eventos | `Unschedulable` | Job criado | log “nenhum nó … mínimo de 80%” |
| Tempo até Running | ∞ (censurado) | finito | ∞ censurado, mas **sem** objeto Pending |

A comparação justa com a literatura é: Borg/Kubernetes **rejeitam** o Pod no filtro de recursos (*predicate* / *NodeResourcesFit*) e o deixam na fila interna do scheduler (`Pending`). O tratamento **reescreve o pedido** para o teto viável, dentro de uma folga de 20% sobre o piso original, e só então ranqueia.

---

## 8. Critérios de sucesso

- **Principal:** H1 e H2 simultâneos no \(R\) calibrado.
- **De honestidade científica:** o controle negativo (pedido > 1,25×) demonstra o limite da resiliência — o sistema **não** mente “sempre executa”.
- **Não** contar como sucesso um Job que rodou porque o planejamento já tinha cortado para ≤ \(R_{\max}\) **antes** da fila (aí não houve oversize na admissão). Verificar `required_memory_gb` **na entrada** de `process_queue_job`.

---

## 9. Ameaças

- Kind reporta allocatable menor que a RAM do Docker Desktop; medir na hora.
- Telemetria nula nos workers K8s aplica fator 0,5 (`K8S_NULL_TELEMETRY_FACTOR`): o “efetivo” do ranker pode ser **metade** do allocatable. O \(R_{\max}\) do clamp é o efetivo do ranker, **não** o Allocatable cru. Calibrar \(R\) contra o que `get_top_nodes_for_task` realmente vê (log `NodeRanker:` / teste isolado na sprint).
- OOMKill após clamp: só se o workload não for leve. Manter hello.
- Realocação Unschedulable (60 s) no watcher é caminho **distinto** do clamp. Se aparecer `[REALOCACAO]`, o clamp **falhou** em prevenir o Pending — reportar como desvio do protocolo.

---

## 10. Artefatos da sprint de execução (não agora)

- Tabela de allocatable vs. efetivo NodeRanker por worker (Sprint 0 + este exp.).
- `kubectl describe pod` da baseline (Pending).
- Logs da fila (clamp 80% + `best_effort`).
- Spec efetivo JSON (`k8s_job_spec` pós-`persist_effective_k8s_dispatch`).
- (Opcional) evidência `na_fila` do controle > 1,25×.
