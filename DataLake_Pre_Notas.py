#
#   SCRIPT OTIMIZADO PARA GERAR O MINIDATALAKE DO PRE NOTAS
#
#
import os
import time
import gc
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# ============================================================
# CONFIG BANCO (PREENCHER) db_portal
# ============================================================
DB_CONFIG = {
    'host': '',           # Substitua pelo seu host
    'port': 3306,                  # Porta padrão do PostgreSQL
    'database': '',       # Substitua pelo nome do banco
    'user': '',         # Substitua pelo usuário
    'password': ''        # Substitua pela senha
}

# ============================================================
# DIRETÓRIOS
# ============================================================
OUTPUT_BASE_DIR = r'C:\DataLake\arquivos\Update_pre_nota\tabelas'

PARQUET_PAGAMENTOS = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Fatos_Pre_Nota_pagamentos.parquet')
PARQUET_PEDIDOS = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Fatos_Pedidos_Compra.parquet')
PARQUET_AUX_PRE_NOTA = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Aux_Pre_Nota.parquet')
PARQUET_AUX_COMPRAS = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Aux_Compras.parquet')
PARQUET_AUX_PAGAMENTOS = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Aux_Pagamentos.parquet')

# ============================================================
# HADOOP
# ============================================================
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']

# ============================================================
# SPARK OTIMIZADO
# ============================================================
print("🚀 Iniciando Spark...")

spark = SparkSession.builder \
    .appName("DataLake_Parquet_Optimized") \
    .master("local[*]") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.default.parallelism", "16") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print(f"✅ Spark {spark.version} pronto!")

# ============================================================
# FUNÇÃO DE LEITURA JDBC - MODO SERIAL (UMA CONEXÃO)
# ============================================================
def ler_jdbc_serial(query):
    """Lê dados com UMA única conexão (sem paralelismo)"""
    url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({query}) AS t") \
        .option("user", DB_CONFIG['user']) \
        .option("password", DB_CONFIG['password']) \
        .option("driver", "org.postgresql.Driver") \
        .option("fetchsize", "50000") \
        .option("queryTimeout", "600") \
        .load() 

    return df

# ============================================================
# FUNÇÃO EXPORTAÇÃO OTIMIZADA (MODO SERIAL)
# ============================================================
def exportar_parquet(query, output_path, descricao, coluna_data=None):
    print("=" * 70)
    print(f"📊 EXPORTANDO: {descricao}")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    try:
        df = ler_jdbc_serial(query)
        print("📥 Dados carregados")

        if coluna_data and coluna_data in df.columns:
            print(f"🔧 Particionando por ano/mês: {coluna_data}")

            df = df.withColumn("ano", year(col(coluna_data))) \
                   .withColumn("mes", month(col(coluna_data)))

            # Remove nulos para evitar pastas vazias
            df = df.filter(col("ano").isNotNull())

            # Evita shuffle pesado
            df = df.coalesce(8)

            df.write \
                .mode("overwrite") \
                .option("partitionOverwriteMode", "dynamic") \
                .partitionBy("ano", "mes") \
                .option("compression", "snappy") \
                .option("parquet.block.size", 500000) \
                .parquet(output_path)

        else:
            print("⚠️ Sem particionamento")
            df = df.coalesce(8)

            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .option("parquet.block.size", 500000) \
                .parquet(output_path)

        print(f"✅ Finalizado: {descricao}")
        return True

    except Exception as e:
        print(f"❌ Erro em {descricao}: {e}")
        return False

# ============================================================
# QUERY 1: Tabela de Pagamentos (Pre Nota)
# ============================================================
QUERY_PAGAMENTOS = """ 
WITH 
agregados AS ( 
    SELECT
        pre.payment_request_id AS id_pedido_pagamento,
        Max(CASE WHEN pre.event_type = 'proforma_invoice' THEN pre.created_at END) AS dt_pre_nota_add,
        Max(CASE WHEN pre.event_type = 'sent_for_proforma_invoice' THEN pre.created_at END) AS dt_envio_para_pre_nota,
        Min(CASE WHEN pre.event_type = 'sent_for_classified_invoice' THEN pre.created_at END) AS dt_pendente_classificacao,
        Min(CASE WHEN pre.event_type = 'classified_invoice' THEN pre.created_at END) AS dt_classificacao,
        Min(CASE WHEN pre.event_type = 'sent_for_approval' THEN pre.created_at END) AS dt_envio_para_aprovacao,
        Max(CASE WHEN pre.event_type = 'approved' THEN pre.created_at END) AS dt_aprovacao
    FROM payment_request_events as pre
    WHERE pre.event_type IN ('sent_for_approval','approved' ,'proforma_invoice', 'sent_for_proforma_invoice','sent_for_classified_invoice','classified_invoice')
    GROUP BY pre.payment_request_id
),
fornecedor_info AS (
    SELECT DISTINCT ON (pre.payment_request_id)
        pre.payment_request_id,
        pre.details->'supplier'->>'id' AS id_fornecedor,
        pre.details->'supplier'->>'name' AS fornecedor
    FROM payment_request_events pre
    WHERE pre.payment_request_id IN (SELECT id_pedido_pagamento FROM agregados)
        AND pre.details->'supplier' IS NOT NULL
    ORDER BY pre.payment_request_id, pre.created_at DESC
),
pedidos_filtrados AS (
    SELECT 
        pr.id,
        pr.request_number,
        pr.status,
        pr.priority,
        pr.created_at,
        pr.due_date,
        pr.competition_date_dre,
        pr.creator_id,
        pr.supplier_protheus_id,
        pr.company_id
    FROM payment_requests pr
    WHERE pr.created_at >= '2026-05-01' --limitar consulta de dados
      AND pr.deleted_at IS NULL
      --AND LOWER(pr.description) NOT LIKE '%fechamento%' -- não trazer pedidos que sejam fechamentos
),
eventos_filtrados AS (
    SELECT 
        pre.payment_request_id,
        pre.event_type,
        pre.created_at
    FROM payment_request_events pre
    WHERE pre.payment_request_id IN (SELECT id FROM pedidos_filtrados)
),
eventos AS (
    SELECT
        pf.id AS id_pedido_pagamento,
        pf.request_number AS numero_solicitacao,
        ef.event_type AS historico_tipo_evento,
        ef.created_at AS dt_evento_criado_em,
        CASE
            WHEN pf.status = 'finished' THEN 'Finalizado'
            WHEN pf.status = 'paid_invoice' THEN 'Nota Paga'
            WHEN pf.status = 'pending_paid_invoice' THEN 'Pendente de Pagamento'
            WHEN pf.status = 'classified_invoiced' THEN 'Nota Classificada'
            WHEN pf.status = 'created' THEN 'Criado'
            WHEN pf.status = 'rejected' THEN 'Rejeitado'
            WHEN pf.status = 'approved' THEN 'Pedido Aprovado'
            WHEN pf.status = 'approved_partially' THEN 'Aprovado Parcial'
            WHEN pf.status = 'pending_proforma_invoice' THEN 'Pendente Pré-Nota'
            WHEN pf.status = 'pending_approval' THEN 'Aguardando Aprovação'
            WHEN pf.status = 'pending_classified_invoice' THEN 'Pendente Classificação'
            WHEN pf.status = 'excluded' THEN 'Excluído'
            ELSE pf.status
        END AS status_atual,
        CASE 
            WHEN pf.priority = 'urgent' THEN 'urgente'
            WHEN pf.priority = 'normal' THEN 'normal'
            WHEN pf.priority = 'more_than_five_thousand' THEN 'acima de 5 mil'
            ELSE pf.priority
        END AS prioridade_pedido,
        pf.competition_date_dre AS dt_competencia_dre,
        TO_CHAR(pf.created_at, 'DD/MM/YYYY') AS dt_criacao,
        TO_CHAR(pf.due_date, 'DD/MM/YYYY') AS dt_vencimento,
        pf.creator_id AS id_criador,
        SPLIT_PART(pf.supplier_protheus_id, '-', 1) AS id_protheus,
        c.id_protheus AS filial,
        rc.id_protheus AS cr,
        CONCAT('https://portal.gnflorestal.com.br/payment-requests/', pf.id) AS link
    FROM pedidos_filtrados pf
        INNER JOIN eventos_filtrados ef ON pf.id = ef.payment_request_id
        INNER JOIN (
            SELECT DISTINCT ON (payment_request_id) payment_request_id, result_center_id 
            FROM payment_request_items
        ) pri ON pri.payment_request_id = pf.id
        INNER JOIN result_centers rc ON pri.result_center_id = rc.id
        INNER JOIN companies c ON pf.company_id = c.id
)
SELECT
    e.id_pedido_pagamento,
    e.numero_solicitacao,
    e.historico_tipo_evento,
    e.dt_evento_criado_em,
    e.status_atual,
    e.prioridade_pedido,
    e.dt_competencia_dre,
    e.dt_criacao,
    e.dt_vencimento,
    e.id_criador,
    e.id_protheus,
    e.filial,
    e.cr,
    e.link,
    a.dt_pre_nota_add,
    a.dt_envio_para_pre_nota,
    a.dt_pendente_classificacao,
    a.dt_classificacao,
    a.dt_envio_para_aprovacao,
    a.dt_aprovacao,
    fi.id_fornecedor,
    fi.fornecedor
FROM eventos e
LEFT JOIN agregados a ON e.id_pedido_pagamento = a.id_pedido_pagamento
LEFT JOIN fornecedor_info fi ON e.id_pedido_pagamento = fi.payment_request_id
 """

# ============================================================
# QUERY 2: Tabela de Pedidos de Compra
# ============================================================
QUERY_PEDIDOS = """ 
-- Primeiro: identifica pedidos com produto aprovado
WITH pedidos_com_produto_aprovado AS (
    SELECT DISTINCT b2.purchase_request_id
    FROM budgets b2
    JOIN budget_products bp ON bp.budget_id = b2.id
    WHERE bp.is_approved = TRUE
),
-- Segundo: busca dados APENAS dos eventos relevantes
eventos AS (
    SELECT
        pr.id AS id_pedido_de_compra,
        pr.creator_id AS id_do_solicitante,
        pr.approver_id AS id_aprovador,
        b2.supplier_id AS id_fornecedor,
        pre.event_type AS historico_do_evento,
        pre.created_at AS evento_criado_em,
        pr.request_number AS numero_requisicao,
        split_part(rc."name", ' ', 1) AS Cod_CR,
        s.cnpj AS cnpj,
        s.company_name,
        s.contact_name AS nome_contato,
        pr.status AS Status_solicitacao_pedido,
        CASE 
            WHEN pr.priority = 'urgent' THEN 'urgente'
            WHEN pr.priority = 'normal' THEN 'normal'
            WHEN pr.priority = 'more_than_five_thousand' THEN 'acima de 5 mil'
            ELSE pr.priority
        END AS prioridade_pedido,
        pr.created_at AS solicitacao_pedido_criado_em,
        --pr.deleted_at AS solicitacao_pedido_excluida_em,
        i.emission_date AS data_de_emissao_fatura,
        i.invoice_number AS numero_fatura_nf,
        i.created_at AS fatura_criada_em,
        b2.delivery_estimated_date as dt_estimada_entrega,
        i.invoice_competition_date_dre AS dt_competencia_dre,
        c.id_protheus AS filial,
        c.company_name as nome_companhia,
        concat('https://portal.gnflorestal.com.br/purchase-requests/', pr.id) as link
    FROM purchase_request_events pre
    INNER JOIN purchase_requests pr ON pre.purchase_request_id = pr.id
    JOIN pedidos_com_produto_aprovado ppa ON ppa.purchase_request_id = pr.id
    JOIN result_centers rc ON pr.result_center_id = rc.id
    JOIN budgets b2 ON b2.purchase_request_id = pr.id
    LEFT JOIN invoices i ON i.budget_id = b2.id
    JOIN suppliers s ON b2.supplier_id = s.id
    LEFT JOIN companies c ON pr.company_id = c.id
    WHERE pr.created_at >= '2026-05-01' 
      AND pr.deleted_at IS NULL
      AND pre.event_type IN (
          'dt_envio_para_aprovacao',     -- passo 5
          'approved',                    -- passo 6
          'sent_for_invoiced',           -- passo 7
          'pending_invoiced',            -- passo 8
          'invoiced',                    -- passo 9
          'sent_for_proforma_invoice',   -- passo 12
          'proforma_invoice',            -- passo 13
          'sent_for_classified_invoice', -- passo 14
          'classified_invoice'           -- passo 15
      )
),
-- Terceiro: agregado de eventos (agora com os mesmos filtros)
agregados AS (
    SELECT
        pr.id AS id_pedido_de_compra,
        MIN(pre.created_at) FILTER (WHERE pre.event_type = 'sent_for_approval') AS dt_envio_para_aprovacao,
        MAX(pre.created_at) FILTER (WHERE pre.event_type = 'approved') AS dt_max_approved,
        MAX(pre.created_at) FILTER (WHERE pre.event_type = 'proforma_invoice') AS dt_anexo_pre_nota,
        MAX(pre.created_at) FILTER (WHERE pre.event_type = 'sent_for_proforma_invoice') AS dt_max_sent_for_proforma,
        MIN(pre.created_at) FILTER (WHERE pre.event_type = 'sent_for_classified_invoice') AS dt_pendente_classificacao,
        MIN(pre.created_at) FILTER (WHERE pre.event_type = 'classified_invoice') AS dt_classificacao
    FROM purchase_requests pr
    JOIN pedidos_com_produto_aprovado ppa ON ppa.purchase_request_id = pr.id
    LEFT JOIN purchase_request_events pre ON pre.purchase_request_id = pr.id
    WHERE pr.created_at >= '2026-05-01' 
      AND pr.deleted_at IS NULL
      AND pre.event_type IN (
          'sent_for_approval','approved', 'sent_for_invoiced', 'pending_invoiced', 'invoiced','sent_for_proforma_invoice',
          'proforma_invoice', 'sent_for_classified_invoice', 'classified_invoice'
      )
    GROUP BY pr.id
)
SELECT 
    e.*,
    a.dt_envio_para_aprovacao,
    a.dt_max_approved,
    a.dt_anexo_pre_nota,
    a.dt_max_sent_for_proforma,
    a.dt_pendente_classificacao,
    a.dt_classificacao
FROM eventos e
LEFT JOIN agregados a ON e.id_pedido_de_compra = a.id_pedido_de_compra
 """

# ============================================================
# QUERY 3: Tabela Auxiliar Pre Nota
# ============================================================
QUERY_AUX_PRE_NOTA = """ 
WITH TABELA1 AS (
SELECT 
    b2.supplier_id AS id_fornecedor,
    pre.created_at AS evento_criado_em,
    pre.event_type AS historico_do_evento,
    pr.request_number AS numero_requisicao,
    b2.attachment AS anexos,
    i.invoice_competition_date_dre AS dt_competencia_dre,
    s.cnpj
FROM purchase_request_events pre
INNER JOIN purchase_requests pr ON pre.purchase_request_id = pr.id
JOIN result_centers rc ON pr.result_center_id = rc.id
JOIN budgets b2 ON b2.purchase_request_id = pr.id
LEFT JOIN invoices i ON i.budget_id = b2.id
JOIN suppliers s ON b2.supplier_id = s.id
LEFT JOIN companies c ON pr.company_id = c.id
WHERE pre.created_at >= '2026-05-01'--limitar consulta de dados 
)
SELECT
    T1.id_fornecedor,
    T1.evento_criado_em,
    T1.historico_do_evento,
    T1.numero_requisicao,
    T1.anexos,
    T1.dt_competencia_dre,
    T1.cnpj
FROM
    TABELA1 T1
 """

# ============================================================
# QUERY 4: Tabela Auxiliar Compras
# ============================================================
QUERY_AUX_COMPRAS = """ 
WITH tabela_aux_compras as (
SELECT
    pr.creator_id as id_criador,
    rc.id_protheus AS CR,
    i.invoice_number AS numero_nota_fiscal,
    to_char(i.emission_date, 'DD/MM/YYYY') AS dt_emissao_PC,
    pr.request_number AS numero_requisicao,
    s.company_name AS razao_social,
    s.id_protheus,
    s.cnpj,
    pr.deleted_at AS pedido_deletado_em,
    pr.created_at AS dt_criacao,
    b.delivery_estimated_date AS dt_estimada_entrega,
    b.delivery_days AS dias_estimado_entrega,
    pr.competition_date_dre,
    CONCAT('https://portal.gnflorestal.com.br/purchase-requests/', pr.id) AS link_acesso_pedido
FROM invoices i 
JOIN budgets b ON i.budget_id = b.id
JOIN purchase_requests pr ON b.purchase_request_id = pr.id
JOIN suppliers s ON b.supplier_id = s.id 
JOIN result_centers rc ON pr.result_center_id = rc.id 
WHERE pr.deleted_at IS NULL
  AND pr.created_at >= '2026-05-01'--limitar consulta de dados
  AND i.invoice_number IS NOT NULL 
  AND i.invoice_number != ''-- nao trazer os valores nulos
)
SELECT
    T1.id_criador,
    T1.CR,
    T1.numero_nota_fiscal,
    T1.dt_emissao_PC,
    T1.numero_requisicao,
    T1.razao_social,
    T1.id_protheus,
    T1.cnpj,
    T1.pedido_deletado_em,
    T1.dt_criacao,
    T1.dt_estimada_entrega,
    T1.dias_estimado_entrega,
    T1.competition_date_dre,
    T1.link_acesso_pedido
FROM
    tabela_aux_compras T1
 """

# ============================================================
# QUERY 5: Tabela Auxiliar Pagamentos
# ============================================================
QUERY_AUX_PAGAMENTOS = """
SELECT
    pr.creator_id as id_criador_PC,
    pr.id AS id_pagamento,
    rc.id_protheus AS cr,
    i.invoice_number AS numero_nf,
    to_char(i.emission_date, 'DD/MM/YYYY') AS dt_emissao_PF,
    pr.request_number AS numero_pedido,
    split_part(pr.supplier_protheus_id, '-', 1) AS id_protheus,
    pr.deleted_at,
    pr.created_at AS criado_em,
    CONCAT('https://portal.gnflorestal.com.br/payment-requests/', pr.id) AS link_acesso_pedido
FROM invoices i 
JOIN payment_requests pr ON i.payment_request_id = pr.id
JOIN payment_request_items pri ON pri.payment_request_id = pr.id
JOIN assignments a ON pri.assignment_id = a.id 
JOIN result_centers rc ON pri.result_center_id = rc.id
WHERE pr.created_at >= '2026-05-01'--limitar consulta de dados
"""

# ============================================================
# EXECUÇÃO
# ============================================================
print("=" * 70)
print("🚀 INICIANDO EXPORTAÇÃO - MODO SERIAL")
print("=" * 70)

os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

exportacoes = [
    (QUERY_PAGAMENTOS, PARQUET_PAGAMENTOS, 'Pagamentos', 'dt_evento_criado_em'),
    (QUERY_PEDIDOS, PARQUET_PEDIDOS, 'Pedidos Compra', 'evento_criado_em'),
    (QUERY_AUX_PRE_NOTA, PARQUET_AUX_PRE_NOTA, 'Aux Pre Nota', 'evento_criado_em'),
    (QUERY_AUX_COMPRAS, PARQUET_AUX_COMPRAS, 'Aux Compras', 'dt_criacao'),
    (QUERY_AUX_PAGAMENTOS, PARQUET_AUX_PAGAMENTOS, 'Aux Pagamentos', 'criado_em'),
]

resultados = {}

for i, (query, path, desc, col_data) in enumerate(exportacoes):
    print(f"\n{'='*70}")
    print(f"📋 TABELA {i+1}/{len(exportacoes)}: {desc}")
    print(f"{'='*70}")
    
    # Executa exportação
    sucesso = exportar_parquet(query, path, desc, col_data)
    resultados[desc] = sucesso
    
    # Aguarda entre as tabelas (exceto após a última)
    if i < len(exportacoes) - 1:
        print("\n⏳ Aguardando 30 segundos para o banco se recuperar...")
        time.sleep(30)
        gc.collect()

# ============================================================
# RESUMO FINAL
# ============================================================
print("\n" + "=" * 70)
print("📊 RESUMO FINAL")
print("=" * 70)

for nome, sucesso in resultados.items():
    print(f"{'✅' if sucesso else '❌'} {nome}")

print(f"\n📁 Local: {OUTPUT_BASE_DIR}")

spark.stop()
print("🔌 Spark encerrado.")