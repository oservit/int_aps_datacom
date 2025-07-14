from datetime import datetime, timedelta
from pathlib import Path
import re
import pandas as pd
from decimal import Decimal

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Arquivo com os SELECTs das functions da package (um por linha, comentado com --SELECT …)
QUERIES_FILE = Path("/root/airflow/include/queries_integra_aps.sql")

default_args = {
    "owner": "datacom",
    "retries": 0,
}

# 1) converte nome SQL Server → tipo Python
SQLSERVER_TO_PY = {
    "int":         "int64",
    "bigint":      "int64",
    "smallint":    "int64",
    "tinyint":     "int64",
    "bit":         "bool",
    "money":       "float64",
    "smallmoney":  "float64",
    "decimal":     "float64",
    "numeric":     "float64",
    "float":       "float64",
    "real":        "float64",
    "varchar":     "object",
    "nvarchar":    "object",
    "char":        "object",
    "nchar":       "object",
    "text":        "object",
    "ntext":       "object",
    "datetime":    "datetime64[ns]",
    "smalldatetime": "datetime64[ns]",
    "date":        "datetime64[ns]",
    "datetime2":   "datetime64[ns]",
    "time":        "object",      # manter como string
    # adicione se surgir outro tipo
}

DATETIME_TYPES = {"datetime", "datetime2", "smalldatetime"}
DATE_TYPES     = {"date"}
TIME_TYPES     = {"time"}

# Dicionário para mapear nomes de colunas Oracle -> SQL Server se necessário
COLUMN_MAPPING = {
    # Adicione aqui mapeamentos específicos se os nomes forem diferentes
    # Por exemplo:
    # 'nome_oracle': 'nome_sqlserver'
}

def enviar_email_erro(erro_msg: str, contexto: str = ""):
    """
    Envia email de erro usando a procedure PC_SEND_MAIL do Oracle.
    Extrai apenas a mensagem principal do erro, sem stack trace.
    """
    try:
        # Extrai apenas a mensagem principal do erro
        if ":" in erro_msg:
            # Pega a primeira parte antes dos dois pontos (geralmente a mensagem principal)
            erro_principal = erro_msg.split(":")[0].strip()
        else:
            erro_principal = erro_msg.strip()

        # Limita o tamanho da mensagem
        if len(erro_principal) > 200:
            erro_principal = erro_principal[:200] + "..."

        # Monta a mensagem do email
        if contexto:
            mensagem = f"Falha identificada durante: {contexto}\n\nErro: {erro_principal}"
        else:
            mensagem = f"Erro: {erro_principal}"

        # Conecta no Oracle e chama a procedure
        oracle = OracleHook.get_hook(conn_id="oracle_svc_aps")

        sql_email = """
        BEGIN
          PC_SEND_MAIL(
            PSENDER    => 'dba@datacom.com.br',
            PRECIPIENT => 'dba@datacom.com.br',
            PSUBJECT   => 'Falha na integração APS',
            PMESSAGE   => :mensagem
          );
          COMMIT;
        END;
        """

        oracle.run(sql_email, parameters={"mensagem": mensagem})
        print(f">>> Email de erro enviado para dba@datacom.com.br")

    except Exception as email_error:
        print(f">>> ERRO ao enviar email: {str(email_error)}")

def get_sqlserver_schema(mssql_hook, full_name: str):
    """
    full_name pode vir como 'dbo.minha_tabela' ou apenas 'minha_tabela'.
    Devolve {coluna: tipo} preservando a ordem física.
    """
    if "." in full_name:
        schema, name = full_name.split(".", 1)
    else:
        schema, name = "dbo", full_name          # default

    rows = mssql_hook.get_records(
        """
        SELECT COLUMN_NAME, DATA_TYPE
          FROM INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_SCHEMA = %s
           AND TABLE_NAME   = %s
         ORDER BY ORDINAL_POSITION
        """,
        parameters=(schema, name),
    )
    if not rows:
        raise ValueError(f"Colunas não encontradas para {full_name}")
    return {col.lower(): dtype.lower() for col, dtype in rows}

def normalize_column_names(df, schema_map):
    """
    Normaliza os nomes das colunas do DataFrame para corresponder ao schema do SQL Server.
    Tenta diferentes variações de nomes de colunas.
    """
    # Cria um mapa de nomes normalizados
    normalized_schema = {k.lower().replace('_', ''): k for k in schema_map.keys()}

    # Cria uma cópia do DataFrame
    df_normalized = df.copy()

    # Renomeia colunas conforme necessário
    rename_map = {}
    for df_col in df.columns:
        df_col_normalized = df_col.lower().replace('_', '')

        # Verifica se há correspondência direta
        if df_col in schema_map:
            continue

        # Verifica mapeamento customizado
        if df_col in COLUMN_MAPPING and COLUMN_MAPPING[df_col] in schema_map:
            rename_map[df_col] = COLUMN_MAPPING[df_col]
            continue

        # Verifica correspondência normalizada
        if df_col_normalized in normalized_schema:
            rename_map[df_col] = normalized_schema[df_col_normalized]

    if rename_map:
        print(f"\n>>> Renomeando colunas: {rename_map}")
        df_normalized = df_normalized.rename(columns=rename_map)

    return df_normalized

def cast_df_to_schema(df, schema_map):
    """
    Converte as colunas do DataFrame para os tipos corretos baseado no schema do SQL Server.
    """
    for col, sql_type in schema_map.items():
        if col not in df.columns:
            continue
        sql_type = sql_type.lower()

        target = SQLSERVER_TO_PY.get(sql_type, "object")

        if sql_type in DATETIME_TYPES:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

        elif sql_type in DATE_TYPES:
            temp_datetime = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
            df[col] = temp_datetime.apply(lambda x: x.date() if pd.notnull(x) else None)

        elif sql_type in TIME_TYPES:
            temp_datetime = pd.to_datetime(df[col], errors="coerce")
            df[col] = temp_datetime.apply(lambda x: x.time() if pd.notnull(x) else None)

        elif target == "int64":
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(0).astype("int64")
        elif target == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif target == "bool":
            df[col] = df[col].fillna(False).astype(bool)
        else:
            df[col] = df[col].astype(object)

    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'object':
            df[col] = df[col].where(pd.notnull(df[col]), None)

    return df

def safe_insert_rows(mssql_hook, table, df, target_fields, commit_every=1000):
    """
    Insere linhas de forma segura, convertendo tipos problemáticos.
    """
    rows = []
    for _, row in df.iterrows():
        row_values = []
        for field in target_fields:
            value = row[field]
            if pd.isna(value) or value is None:
                row_values.append(None)
            elif isinstance(value, (pd.Timestamp, datetime)):
                if hasattr(value, 'to_pydatetime'):
                    row_values.append(value.to_pydatetime())
                else:
                    row_values.append(value)
            else:
                row_values.append(value)
        rows.append(tuple(row_values))

    mssql_hook.insert_rows(
        table=table,
        rows=rows,
        target_fields=target_fields,
        commit_every=commit_every
    )

def checa_flag(**_):
    """
    Se TB_CONFIG.PARAM='RODA_INTEGRACAO_APS' tiver valor 'S' → continua com extrai_envia_sqlserver,
    se tiver valor 'N' → pula para fim_sem_execucao.
    """
    try:
        sql = """
        SELECT value
          FROM TB_CONFIG
         WHERE param = 'RODA_INTEGRACAO_APS'
        """
        oracle = OracleHook.get_hook(conn_id="oracle_svc_aps")
        flag = oracle.get_first(sql)

        # Verificar explicitamente se o valor é 'S'
        return "extrai_envia_sqlserver" if flag and flag[0] == 'S' else "fim_sem_execucao"
    except Exception as e:
        enviar_email_erro(str(e), "verificação da flag de execução")
        raise

def extrai_envia_sqlserver(**_):
    """
    Executa cada SELECT das functions Oracle e grava resultado no SQL Server.
    """
    try:
        if not QUERIES_FILE.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {QUERIES_FILE}")

        oracle = OracleHook.get_hook(conn_id="oracle_svc_aps")
        mssql  = MsSqlHook(mssql_conn_id="sqlserver-opcenter", database='OPCENTER_DATACOM_IO')

        with QUERIES_FILE.open(encoding="utf-8") as f:
            selects = [
                line.lstrip("-").strip().replace(';', '')
                for line in f
                if line.strip().lower().startswith("--select")
            ]

        for stmt in selects:
            try:
                df = oracle.get_pandas_df(stmt)
                df.columns = df.columns.str.lower()
                if df.empty:
                    continue

                func_full = re.search(r'table\(([^)]+?)\s*\)', stmt, flags=re.I).group(1)
                func_name = func_full.split(".")[-1]
                destino   = re.sub(r"^fc_return_rs(?:_rel)?_", "", func_name, flags=re.I)
                destino   = re.sub(r"[();\s]", "", destino).lower()
                destino   = 'dbo.' + destino
                if 'estoques' in destino:
                    destino = destino.replace('estoques', 'estoque')

                print(f"\n>>> Processando tabela: {destino}")

                schema = get_sqlserver_schema(mssql, destino)
                print(f"\n>>> Colunas do Oracle (DataFrame): {list(df.columns)}")
                print(f">>> Colunas do SQL Server (Schema): {list(schema.keys())}")

                df = normalize_column_names(df, schema)

                missing_cols = [col for col in schema.keys() if col not in df.columns]
                if missing_cols:
                    print(f"\n>>> AVISO: Colunas do SQL Server não encontradas no DataFrame: {missing_cols}")
                    for col in missing_cols:
                        df[col] = None

                extra_cols = [col for col in df.columns if col not in schema.keys()]
                if extra_cols:
                    print(f">>> AVISO: Colunas extras no DataFrame que não existem no SQL Server: {extra_cols}")
                    df = df.drop(columns=extra_cols)

                df = cast_df_to_schema(df, schema)

                cols_ordered = list(schema.keys())
                df = df[cols_ordered]

                print("\n>>> Tipos do DF após cast:")
                print(df.dtypes)

                if len(df) > 0:
                    print("\n>>> Primeira linha do DataFrame:")
                    first_row = df.iloc[0].to_dict()
                    for col, val in first_row.items():
                        if val is not None:
                            print(f"  {col}: {val} (tipo: {type(val)})")

                date_cols = [col for col in df.columns if schema.get(col, '').lower() in
                             DATETIME_TYPES | DATE_TYPES | TIME_TYPES]

                if date_cols:
                    print(f"\n>>> Colunas de data/hora encontradas: {date_cols}")

                mssql.run(f"TRUNCATE TABLE {destino}", autocommit=True)

                safe_insert_rows(
                    mssql_hook=mssql,
                    table=destino,
                    df=df,
                    target_fields=cols_ordered,
                    commit_every=1000
                )
                print(f">>> Inserção bem-sucedida para {destino}")

            except Exception as table_error:
                # Envia email com erro específico da tabela
                erro_msg = f"Erro ao processar tabela {destino if 'destino' in locals() else 'desconhecida'}: {str(table_error)}"
                enviar_email_erro(str(table_error), f"processamento da tabela {destino if 'destino' in locals() else 'desconhecida'}")
                print(f"\n>>> ERRO ao processar tabela: {erro_msg}")
                # Re-raise para interromper o processo
                raise

    except Exception as e:
        enviar_email_erro(str(e), "extração e envio para SQL Server")
        raise

def copia_programacao(**_):
    """Copia LSB_INT_Programacao (SQL Server) → SYSDATACOM.TB_LSB_INT_PROGRAMACAO (Oracle)."""
    try:
        mssql  = MsSqlHook(mssql_conn_id="sqlserver-opcenter")
        oracle = OracleHook.get_hook(conn_id="oracle_svc_aps")

        df = mssql.get_pandas_df("SELECT * FROM LSB_INT_Programacao")
        if df.empty:
            return

        oracle.run("TRUNCATE TABLE SYSDATACOM.TB_LSB_INT_PROGRAMACAO", autocommit=True)
        oracle.insert_rows(
            table="SYSDATACOM.TB_LSB_INT_PROGRAMACAO",
            rows=list(df.itertuples(index=False, name=None)),
            target_fields=df.columns.tolist(),
            commit_every=1000,
        )
    except Exception as e:
        enviar_email_erro(str(e), "cópia da programação")
        raise

def ajusta_roda_integraca_aps(**_):
    """
    Função para atualizar o parâmetro RODA_INTEGRACAO_APS para 'N' na tabela TB_CONFIG.
    Esta função é utilizada em DAGs do Airflow para desabilitar a integração APS após execução bem-sucedida.
    """
    try:
        oracle = OracleHook.get_hook(conn_id="oracle_svc_aps")

        # Query para atualizar o valor do parâmetro RODA_INTEGRACAO_APS para 'N'
        sql = """
        UPDATE TB_CONFIG
        SET VALUE = 'N'
        WHERE PARAM = 'RODA_INTEGRACAO_APS'
        """

        # Executar a atualização
        oracle.run(sql, autocommit=True)

        # Verificar se a atualização foi bem-sucedida
        verification_sql = """
        SELECT VALUE FROM TB_CONFIG
        WHERE PARAM = 'RODA_INTEGRACAO_APS'
        """

        result = oracle.get_first(verification_sql)

        if result and result[0] == 'N':
            print(">>> Parâmetro RODA_INTEGRACAO_APS atualizado com sucesso para 'N'")
        else:
            print(">>> Falha ao atualizar o parâmetro RODA_INTEGRACAO_APS")
            raise Exception("Falha ao verificar atualização do parâmetro RODA_INTEGRACAO_APS")

        return result

    except Exception as e:
        enviar_email_erro(str(e), "atualização do parâmetro RODA_INTEGRACAO_APS")
        raise

with DAG(
    dag_id="erp_aps_integracao",
    description="Integração Oracle ↔ SQL Server (APS) — versão Airflow 2.2",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 5, 27),
    catchup=False,
    max_active_runs=1,  # Impede execuções simultâneas
    default_args=default_args,
    tags=["oracle", "sqlserver", "aps"],
) as dag:

    inicio = DummyOperator(task_id="inicio")

    branch = BranchPythonOperator(
        task_id="checa_flag",
        python_callable=checa_flag,
        provide_context=True,
    )

    extrai_envia = PythonOperator(
        task_id="extrai_envia_sqlserver",
        python_callable=extrai_envia_sqlserver,
        provide_context=True,
    )

    copia_prog = PythonOperator(
        task_id="copia_programacao",
        python_callable=copia_programacao,
        provide_context=True,
    )

    ajusta_tb_config = PythonOperator(
        task_id="ajusta_roda_integraca_aps",
        python_callable=ajusta_roda_integraca_aps,
        provide_context=True,
    )

    fim_ok = DummyOperator(
        task_id="fim_ok",
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    fim_skip = DummyOperator(task_id="fim_sem_execucao")

    # Encadeamento
    inicio >> branch
    branch >> extrai_envia >> copia_prog >> ajusta_tb_config >> fim_ok
    branch >> fim_skip
