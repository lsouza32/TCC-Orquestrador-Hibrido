# Imagem Docker para tarefas de ML (TensorFlow + extras)

Usada pelos tipos de tarefa **médio** e **pesado** que rodam scripts como `task-cnn-training.py` (matplotlib, scikit-learn, seaborn, OpenCV, kagglehub).

## Build no nó

Na **pasta do node_agent** (ex.: `C:\node_agent`), onde está o `node_agent.py` e o `Dockerfile`:

```cmd
docker build -t tcc-tf-ml:latest -f Dockerfile .
```

O **ponto (.)** no final é o contexto de build (diretório atual). Não é necessária a pasta `tf-ml`.

O Context Server está configurado para usar a imagem `tcc-tf-ml:latest` nesses tipos de tarefa.

## Fila de tarefas no broker

Todas as tarefas submetidas entram na fila. O broker **não** processa a fila em intervalo fixo: ele processa sempre que recebe o **registro/heartbeat** de um nó em `POST /register_node`. Ou seja, ao manter o node_agent rodando e enviando heartbeat, o nó colabora para que as tarefas em espera sejam alocadas e executadas. Quanto mais estável e frequente o heartbeat, mais rápido as tarefas na fila podem ser despachadas para os nós disponíveis.
