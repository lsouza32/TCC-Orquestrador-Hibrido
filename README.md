# Orquestrador Híbrido com LLM e Decisões Determinísticas para Alocação de Recursos em Sistemas Distribuídos

Código, *scripts* de avaliação e resultados do Trabalho de Conclusão de Curso
(UTFPR, *campus* Apucarana) de Lucas Santos Souza, orientado por André Luiz
Tinassi D'Amato. O artigo está em [`docs/artigo-TCC-final/`](docs/artigo-TCC-final/).

## O que é

Um *broker* (Context Server) que fica **ao lado** do `kube-scheduler` do
Kubernetes, e não no lugar dele. Ele recebe um *script* Python e:

1. um LLM pequeno (`LiquidAI/lfm2.5-1.2b-instruct`, via Ollama, temperatura 0)
   **interpreta o código** e propõe uma especificação de Job (perfil da tarefa,
   imagem, `requests` e `limits`). Ele **não** escolhe o nó;
2. regras determinísticas validam essa especificação, escolhem o nó com o
   **NodeRanker** (`S = 40r + 25w + 15d + 20ℓ`) e aplicam a **admissão
   *best-effort*** (regra dos 80%);
3. o Job é criado no cluster com um seletor de `hostname`, um monitor registra os
   picos de uso e o histórico realimenta a próxima execução.

### Perfis de tarefa

O LLM (agente Analista) classifica cada *script* em um de três perfis:

| Perfil | Significado | Exemplo |
|---|---|---|
| `CPU-bound` | limitado pelo processador | laços e cálculo numérico |
| `IO-bound` | limitado por entrada e saída | leitura de arquivos, rede |
| `ML-heavy` | treino ou inferência de aprendizado de máquina | `tensorflow`, `torch`, `keras`, `sklearn` |

A camada preferida é uma função determinística do perfil, da complexidade (1 a
10) e dos recursos. A primeira condição satisfeita decide:

| Camada | Condição |
|---|---|
| `cloud` | `ML-heavy`, ou complexidade ≥ 8, ou memória ≥ 4 Gi, ou CPU ≥ 4 |
| `fog` | complexidade ≥ 5, ou `CPU-bound`, ou memória ≥ 1,5 Gi, ou CPU ≥ 2 |
| `edge` | nenhuma das anteriores |

### Admissão *best-effort*

Se nenhum nó comporta a requisição, ela é reduzida ao maior valor que algum nó
oferece, desde que isso seja **pelo menos 80%** do pedido. Do contrário, a tarefa
permanece na fila e nenhum Pod é criado. Equivalente a admitir pedidos até
`1,25 × R_max`.

## Estrutura

| Caminho | Conteúdo |
|---|---|
| `context_server/` | API Flask, agentes LLM (`services/ai_service.py`), NodeRanker, regra de camada, execução de Jobs no Kubernetes, fila |
| `node_agent/` | agente por nó (registro, *heartbeat* de 30 s, `/status`, execução em Docker) |
| `web_interface/` | interface web (submissão e acompanhamento de tarefas) |
| `scripts/` | experimentos (`run_exp{1..4}_{baseline,hybrid}.py`), análise de temperatura, exportação de métricas |
| `scripts/eval_tasks/` | *scripts* Python usados como carga nos experimentos |
| `tests/` | testes unitários (`pytest`) |
| `results/metrics_export/` | JSONs e *logs* de cada execução (ver `LEIAME_logs.md`) |
| `docs/experimentos/` | roteiros e manifestos YAML dos experimentos |
| `docs/artigo-TCC-final/` | fonte LaTeX do artigo, referências e figuras |
| `kind-multi-node.yaml` | cluster Kind (1 *control-plane* + 3 *workers* = `edge`, `fog`, `cloud`) |

## Requisitos

- Docker Desktop (ou Docker Engine) com Compose v2
- [Kind](https://kind.sigs.k8s.io/) e `kubectl`
- Python 3.11 ou superior (para os *scripts* e os testes)
- Ollama, para servir o modelo. O `docker-compose.yml` já inclui um contêiner
  Ollama com `gpus: all`; sem GPU NVIDIA, remova essa linha

## Execução

Os comandos abaixo são para PowerShell. Em Linux/macOS troque `$env:VAR` por
`export VAR` e `%USERPROFILE%`/`$HOME\.kube` pelo caminho equivalente.

### 1. Cluster Kubernetes (Kind)

```powershell
kind create cluster --name tcc-cluster --config kind-multi-node.yaml
kubectl apply -f metrics-server.yaml
kubectl get nodes
```

O `metrics-server.yaml` fornece o uso de CPU e memória por nó, necessário para a
telemetria do *broker*. Confirme com `kubectl top nodes` depois de alguns instantes.

### 2. Credencial do *broker* para o cluster

O contêiner do Context Server acessa o cluster pela rede Docker do Kind. Gere um
*kubeconfig* com o endereço interno:

```powershell
kind get kubeconfig --internal --name tcc-cluster | Set-Content "$env:USERPROFILE\.kube\config-broker"
```

### 3. Subir o Context Server, a interface e o Ollama

```powershell
docker compose up -d --build
docker exec ollama ollama pull LiquidAI/lfm2.5-1.2b-instruct
```

O modelo padrão é `LiquidAI/lfm2.5-1.2b-instruct`; para trocá-lo, defina
`OLLAMA_MODEL` antes do `docker compose` e baixe o modelo correspondente
(catálogo: <https://ollama.com/LiquidAI/lfm2.5-1.2b-instruct>). A chave `GOOGLE_GEMINI_API_KEY` é opcional e **não** é
usada pelo fluxo do artigo; se for usar, defina-a no ambiente e não a grave em
arquivos versionados.

Endereços: API em `http://localhost:5000`, interface em `http://localhost:80`.
O arquivo `context_server/config.py` e o `web_interface/config.js` trazem o IP
`192.168.0.50` como padrão; ajuste para o endereço da sua máquina se for usar
nós externos.

### 4. Rodar um experimento

Cada experimento tem um braço padrão (Kubernetes puro) e um híbrido (*broker*).
Execute a partir da raiz do repositório, com o cluster e o Context Server no ar:

```powershell
python -m pip install requests
python scripts/run_exp1_baseline.py ; python scripts/run_exp1_hybrid.py
python scripts/run_exp2_baseline.py ; python scripts/run_exp2_hybrid.py
python scripts/run_exp3_baseline.py ; python scripts/run_exp3_hybrid.py --memory-gi 9.45
python scripts/run_exp4_baseline.py ; python scripts/run_exp4_hybrid.py
```

| Exp. | O que mostra |
|---|---|
| 1 | dimensionamento de memória por histórico (requisição inicial × após a realimentação) |
| 2 | colocação de Jobs com um nó sob estresse de CPU |
| 3 | admissão *best-effort*: requisição de 9,45 Gi acima da capacidade de qualquer nó |
| 4 | camada preferida × camada em que o Job executou |

Os *scripts* apagam o banco do Context Server (`data/nodes.db`) no início de
cada rodada (*cold start*), gravam o JSON em `results/metrics_export/expN/` e o
*log* em `results/metrics_export/expN/logs/`.

### 5. Sensibilidade à temperatura (offline, sem cluster)

```powershell
docker cp scripts context-server:/app/scripts
docker exec context-server python /app/scripts/eval_llm_temperature.py --reps 10 --temps 0 0.5 1.0
```

O script usa os agentes reais do *broker* e, por isso, roda dentro do contêiner
`context-server`, que alcança o Ollama pela rede Docker. O modelo deve estar
baixado (passo 3).

### 6. Testes unitários

```powershell
python -m pip install pytest flask sqlalchemy apscheduler requests kubernetes
python -m pytest tests -q
```

### 7. Nós com `node_agent` (opcional)

O `node_agent` registra uma máquina no Context Server e envia *heartbeat* a cada
30 s. Em cada máquina:

```powershell
python -m pip install -r node_agent/requirements.txt
$env:CONTEXT_SERVER_URL = "http://<ip-do-servidor>:5000"
python node_agent/node_agent.py
```

Os experimentos do artigo usam Jobs do Kubernetes e não dependem do `node_agent`.

## Reprodutibilidade e limites

- Os experimentos rodaram em **um único hospedeiro**, com camadas lógicas
  (rótulos `tcc.utfpr.edu.br/compute-layer` nos *workers* do Kind). Não há
  conclusão sobre latência de rede real.
- A semente de amostragem do LLM não é fixada. Com temperatura 0 a saída tende
  a ser a mesma, mas o resultado pode variar entre versões do Ollama e do modelo.
- Cada braço foi executado uma vez. Os pesos do NodeRanker (40/25/15/20) foram
  definidos manualmente e não foram calibrados.

## Licença

A definir pelo autor.
