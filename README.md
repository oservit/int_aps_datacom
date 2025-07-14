# Integração Datacom LSB - ERP ↔ APS

Sistema de integração automatizada entre Oracle ERP e SQL Server OPCENTER para sincronização de dados APS (Advanced Planning & Scheduling).

## 📋 Visão Geral

Este projeto implementa uma DAG do Apache Airflow que executa a cada 5 minutos para sincronizar dados entre:
- **Oracle ERP** (SYSDATACOM) → **SQL Server OPCENTER** (OPCENTER_DATACOM_IO)
- **SQL Server** (LSB_INT_Programacao) → **Oracle** (TB_LSB_INT_PROGRAMACAO)

## 🔄 Fluxo de Integração

1. **Verificação de Flag**: Consulta `TB_CONFIG.RODA_INTEGRACAO_APS` no Oracle
2. **Extração**: Executa 12 funções Oracle para extrair dados APS
3. **Transformação**: Mapeia colunas e converte tipos de dados
4. **Carga**: Insere dados nas tabelas SQL Server correspondentes
5. **Sincronização Reversa**: Copia dados de programação do SQL Server para Oracle
6. **Finalização**: Reseta flag de integração para 'N'

## 🗂️ Estrutura do Projeto

```
integracao_datacom_lsb/
├── dags/
│   └── erp_aps_integracao.py      # DAG principal do Airflow
├── include/
│   └── queries_integra_aps.sql    # Queries das funções Oracle
├── CLAUDE.md                      # Documentação técnica
└── README.md                      # Este arquivo
```

## ⚙️ Funções da DAG

### Tarefas Principais
- **`checa_flag`**: Branch que verifica se integração deve executar
- **`extrai_envia_sqlserver`**: Processa 12 funções Oracle e carrega no SQL Server
- **`copia_programacao`**: Sincronização reversa de programação
- **`ajusta_roda_integraca_aps`**: Reseta flag de controle

### Dados Integrados
- Recursos e recursos secundários
- Materiais e produtos
- Listas técnicas e ordens
- Estoques, compras e vendas
- Programação APS

## 🔧 Dependências

### Software
- **Apache Airflow** 2.2+
- **Python** 3.x
- **Oracle Client** (para conexão Oracle)
- **SQL Server ODBC Driver**

### Bibliotecas Python
```python
pandas
airflow[oracle,mssql]
```

### Conexões Airflow
- **`oracle_svc_aps`**: Conexão Oracle ERP
- **`sqlserver-opcenter`**: Conexão SQL Server OPCENTER

## 🌐 Acesso

### Interface Airflow
- **URL**: `http://servidor-airflow:8080`
- **DAG ID**: `erp_aps_integracao`

### Controle de Execução
Para habilitar/desabilitar a integração:
```sql
-- Habilitar
UPDATE TB_CONFIG SET VALUE = 'S' WHERE PARAM = 'RODA_INTEGRACAO_APS';

-- Desabilitar  
UPDATE TB_CONFIG SET VALUE = 'N' WHERE PARAM = 'RODA_INTEGRACAO_APS';
```

## 📧 Monitoramento

- **Notificações**: Emails automáticos para `dba@datacom.com.br` em caso de erro
- **Logs**: Disponíveis na interface do Airflow
- **Frequência**: Execução a cada 5 minutos

## 🚀 Execução

A DAG executa automaticamente conforme agendamento. Para execução manual:
1. Acesse a interface do Airflow
2. Localize a DAG `erp_aps_integracao`
3. Clique em "Trigger DAG"

## 📊 Tabelas Envolvidas

### Oracle → SQL Server
- `recursos` ← FC_RETURN_RS_REL_LSB_INT_RECURSOS
- `materiais` ← FC_RETURN_RS_REL_LSB_INT_MATERIAIS
- `ordens` ← FC_RETURN_RS_REL_LSB_INT_ORDENS
- `estoque` ← FC_RETURN_RS_REL_LSB_INT_ESTOQUES
- *... e outras 8 tabelas*

### SQL Server → Oracle
- `LSB_INT_Programacao` → `TB_LSB_INT_PROGRAMACAO`