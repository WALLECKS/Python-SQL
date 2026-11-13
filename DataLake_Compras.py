#
#   SCRIPT OTIMIZADO PARA GERAR O MINIDATALAKE DO COMPRAS
#
#
import os
import time
import gc
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# ============================================================
# CONFIG BANCO (PREENCHER)
# ============================================================
DB_CONFIG = {
    'host': '',           # Substitua pelo seu host
    'port': 54321,                  # Porta padrão do PostgreSQL
    'database': 'postgres',       # Substitua pelo nome do banco
    'user': '',         # Substitua pelo usuário
    'password': ''        # Substitua pela senha
}

# ============================================================
# DIRETÓRIOS PARA SALVAR AS TABELAS
# ============================================================
OUTPUT_BASE_DIR = r'C:\DataLake\arquivos\Update_Compras\tabelas'

# ============================================================
# NOME DAS TABELAS 
# ============================================================
F_PEDIDOS_GERAIS = os.path.join(OUTPUT_BASE_DIR, 'F_PEDIDOS_GERAIS.parquet')
F_TABELA_EVENTOS_PF = os.path.join(OUTPUT_BASE_DIR, 'F_TABELA_EVENTOS_PF.parquet')
F_TABELA_EVENTOS_PC = os.path.join(OUTPUT_BASE_DIR, 'F_TABELA_EVENTOS_PC.parquet')
PARQUET_AUX_PAGAMENTOS = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Aux_Pagamentos.parquet')
PARQUET_NUMERO_SOLICITACOES = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Numero_Solicitacoes.parquet')
PARQUET_AUX_COMPRAS = os.path.join(OUTPUT_BASE_DIR, 'Tabela_Aux_Compras.parquet')

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
        .load()  # ← SEM numPartitions (1 conexão por vez)

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
# QUERIES (INALTERADAS)
# ============================================================
# QUERY 1: Tabela QUERY_COMPRAS_TABELA_PEDIDOS_GERAIS                 
QUERY_COMPRAS = """ 
WITH tabela1 AS (
SELECT
	pr.creator_id AS id_criador,
	pr.buyer_id AS id_comprador,
	pr.approver_id as id_aprovador,
	pr.id AS id_pedido_compra,
    pr.is_regularization AS reguralizacao,
	pr.request_number AS numero_solicitacao,
	min(CASE WHEN pre.event_type = 'sent_for_quotation' THEN pre.created_at END) AS dt_envio_inclusao_orcamentos, -- CAPITURAR A DATA minima() DO EVENTO "ENVIADO PARA COTAÇÃO 
	min(CASE WHEN pre.event_type = 'sent_for_approval' THEN pre.created_at END) AS dt_aguardando_aprovacao,-- CAPITURAR A DATA minima() DO EVENTO "ENVIADO PARA APROVAÇÃO"
	min(CASE WHEN pre.event_type = 'approved' THEN pre.created_at END) AS dt_da_aprovacao,-- CAPITURAR A DATA minima() DO EVENTO "FATURADO"
	min(CASE WHEN pre.event_type = 'sent_for_warehouse' THEN pre.created_at END) AS dt_anexo_nf,-- CAPITURAR A DATA minima() DO EVENTO "PRE NOTA" , NESSA FASE A NOTA É ADICIONADA
	min(CASE WHEN pre.event_type = 'sent_for_proforma_invoice' THEN pre.created_at END) AS dt_pendente_pre_nota -- CAPITURAR A DATA minima() DO EVENTO "PRE NOTA" , NESSA FASE DE ENVIO PARA PRE-NOTA
FROM
	purchase_request_events pre
JOIN purchase_requests pr ON
	pre.purchase_request_id = pr.id
	WHERE pr.created_at >= '01-05-2026' -- MM-DD-YYYY
GROUP BY
	pr.request_number,
	pr.id
)
SELECT  
	DISTINCT 
	b.id AS id_orcamento ,
	b.quotation_date AS dt_cotacao_do_orcamento,
	t1.*,
	pr.created_at AS dt_criacao_pedido,
--	i.emission_date AS dt_emissao_fatura,
--	i.created_at AS dt_anexo_nf,
	s.company_name,
	s.cnpj,
	bp.is_approved AS fornecedor_aprovado,
--######################### AJUSTE DA PRIORIDADE ######################### 
	CASE
        	WHEN pr.priority = 'normal' THEN 'normal'
        	WHEN pr.priority = 'more_than_five_thousand' THEN 'acima de 5 mil'
        	WHEN pr.priority = 'urgent' THEN 'urgente'
        	ELSE pr.priority
        END AS prioridade,
    rc.id_protheus AS cr,
--######################### AJUSTE DO STATUS #########################    
    CASE 
        	WHEN pr.status = 'approved' THEN 'Aprovado'
        	WHEN pr.status = 'classified_invoiced' THEN 'Fatura classificada'
        	WHEN pr.status = 'finished' THEN 'Finalizado'
        	WHEN pr.status = 'paid_invoice' THEN 'Fatura paga'
        	WHEN pr.status = 'pending_approval' THEN 'Pendente de aprovação'
        	WHEN pr.status = 'pending_classified_invoice' THEN 'Pendente de classificação'
        	WHEN pr.status = 'pending_budgets' THEN 'Pendente de orçamento'
        	WHEN pr.status = 'pending_paid_invoice' THEN 'Pendente de pagamento'
        	WHEN pr.status = 'pending_proforma_invoice' THEN 'Pendente de pré-nota'
        	WHEN pr.status = 'pending_warehouse' THEN 'Pendente de almoxarifado'
        	WHEN pr.status = 'pending_invoiced' THEN 'Aguardando faturamento'
        	WHEN pr.status = 'excluded' THEN 'Pedido excluído'
        	WHEN pr.status = 'warehouse' THEN 'Entregue no Almoxarifado'
        	WHEN pr.status = 'rejected' THEN 'Pedido regeitado'
        	WHEN pr.status = 'approved_partially' THEN 'Aprovado parcial'
        	WHEN pr.status = 'invoiced' THEN 'Pedido faturado'
        	WHEN pr.status = 'created' THEN 'Pedido criado'
        	WHEN pr.status = 'proforma_invoice' THEN 'Pre Nota Lançada'
        	WHEN pr.status = 'partially_paid' THEN 'Pago Parcial'
        	ELSE pr.status
        END AS status_atual,
        b.delivery_estimated_date AS dt_estimada_entrega,
        b.delivery_days AS dias_estimado_entrega,
        u.first_name ||' '|| u.last_name AS nome_comprador,
		rt.name as solicitante,
        b.deleted_at AS deletado_orcamento_nf,
       'https://portal.gnflorestal.com.br/purchase-requests/'||pr.id AS link_acesso
FROM
	tabela1 AS t1	
JOIN purchase_requests pr ON id_pedido_compra = pr.id 
join requesters rt ON pr.requester_id = rt.id
LEFT JOIN budgets b ON b.purchase_request_id = pr.id 
LEFT JOIN suppliers s ON b.supplier_id = s.id
LEFT JOIN budget_products bp ON bp.budget_id = b.id
JOIN result_centers rc ON pr.result_center_id = rc.id
LEFT JOIN users u ON pr.buyer_id = u.id 
 """
# ============================================================
# QUERY 2: Tabela QUERY_F_TABELA_EVENTOS_PF     
# ============================================================
QUERY_F_TABELA_EVENTOS_PF = """ 
with tabela1 as (
SELECT
    pre.payment_request_id as id_pedido,
    pr.creator_id AS id_criador,
    pr.created_at as dt_criacao,
    CASE
      WHEN pre.event_type = 'included_edited_competition_date' THEN 'A data de competência foi adicionada/editada por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'included_product' THEN 'A atribuição com o DRE - foi definida por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'modified' THEN 'A solicitação foi alterada por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'back_status' THEN 'A Solicitação teve seu status alterado por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'approved' THEN 'A solicitação foi aprovada por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'sent_for_proforma_invoice' THEN 'A solicitação foi enviada para Pré Nota por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'sent_for_approval' THEN 'A solicitação foi enviada para aprovação por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'modified_product' AND pre.details ->'product'->>'new_dre_name' IS NOT NULL AND pre.details->'product'->>'old_dre_name' IS NOT NULL THEN 'A solicitação teve seu DRE alterado por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT) 
      WHEN pre.event_type = 'modified_product' AND pre.details->'product'->>'new_dre_name' IS NULL AND pre.details->'product'->>'old_dre_name' IS NULL THEN 'A solicitação foi alterado por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'created' THEN 'A solicitação foi criada por'||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'classified_invoice' THEN 'Nota classificada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'sent_for_classified_invoice' THEN 'Enviado para classificação por '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'paid_invoice' THEN 'A solicitação foi faturada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'sent_for_paid_invoice' THEN 'A solicitação foi enviada para Pagamento por  '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'proforma_invoice' THEN 'Pre Nota adicionada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
      WHEN pre.event_type = 'finalized' THEN 'Pedido Finalizado por '|| CAST(pre.details->'user'->>'name' AS TEXT)
      ELSE pre.event_type
    END AS tipo_evento,
    pr.request_number AS numero_solicitacao,
    pre.details as detalhes,
    pre.created_at as dt_criacao_evento,
    pri.assignment_id AS product_id,
    rc.id AS result_center_id,
    rc.id_protheus AS codigo_cr,
    rc.name AS result_center_name,
    pr.is_regularization AS regularizacao,
    concat('https://portal.gnflorestal.com.br/payment-requests/',pre.payment_request_id) AS link_pedido
FROM payment_request_events pre
INNER JOIN payment_request_items pri
    ON pri.assignment_id = ((pre.details->'product'->>'id')::uuid)
    AND pri.payment_request_id = pre.payment_request_id
LEFT JOIN result_centers rc
    ON rc.id = pri.result_center_id
join payment_requests pr
    ON pre.payment_request_id = pr.id
WHERE pri.deleted_at IS NULL and pr.created_at >='2026-05-01'-- AND event_type = 'modified_product'
)
select 
    id_pedido,
    id_criador,
    tipo_evento,
    numero_solicitacao,
    dt_criacao,
    dt_criacao_evento,
    codigo_cr,
    regularizacao,
    link_pedido
FROM
    tabela1
 """

# ============================================================
# QUERY 3: TABELA QUERY_F_TABELA_EVENTOS_PC    
# ============================================================
QUERY_F_TABELA_EVENTOS_PC = """ 
WITH tabela AS (
SELECT
	DISTINCT 
		pr.id AS id_pedido,
		pr.buyer_id AS id_comprador,
		pr.creator_id AS id_criador,
        pr.created_at as dt_criacao,
		CASE
			WHEN pre.event_type='included_product' THEN 'Produto incluído por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='approved' THEN 'Pedido Aprovado por ' || CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='modified_quotation' THEN 'Cotação modificada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='approved_quotation' THEN 'Cotação aprovada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='warehouse' THEN 'Entregue no almoxarifado por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_approval' THEN 'Enviado para aprovação por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_warehouse' THEN 'Enviado para o almoxarifado por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_classified_invoice' THEN 'Enviado para classificação por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='classified_invoice' THEN 'Nota classificada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='proforma_invoice' THEN 'Pre Nota adicionada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_proforma_invoice' THEN 'Pre Nota adicionada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_quotation' THEN 'A solicitação foi enviada para inclusão de orçamentos por  '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='included_quotation' THEN 'Orçamento '||CAST(pre.details->'supplier'->>'name' AS TEXT) ||' foi incluído por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='modified' THEN 'A solicitação foi alterada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_paid_invoice' THEN 'A solicitação foi enviada para Pagamento por  '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='invoiced' THEN 'A solicitação foi faturada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='created' THEN 'A solicitação foi criada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='paid_invoice' THEN 'A solicitação foi faturada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='sent_for_invoiced' THEN 'A solicitação foi enviada para Faturamento por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='modified_product' THEN 'O produto '|| CAST(pre.details->'product'->>'name' AS TEXT)|| '  foi alterado por ' || CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='modified_unit_value_quotation' THEN 'O orçamento do '|| CAST(pre.details->'supplier'->>'name' AS TEXT) || ' teve seu valor orçamentário alterado por ' || CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='included_edited_competition_date' THEN 'A data de competência foi adicionada/editada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='modified_requesterName' THEN 'O solicitante foi alterado de '|| CAST(pre.details->'modifiedRequesterName'->>'oldRequesterName' AS TEXT) || ' para '||CAST(pre.details->'modifiedRequesterName'->>'newRequestName' AS TEXT)
			WHEN pre.event_type='removed_product' THEN 'O produto '|| CAST(pre.details->'product'->>'name' AS TEXT) || ' foi removido por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='back_status' THEN 'A solicitação teve seu status alterado por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='finalized' THEN 'Solicitação finalizada'
			WHEN pre.event_type='removed_quotation' THEN 'O orçamento do'|| CAST(pre.details->'supplier'->>'name' AS TEXT) || ' foi removido por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='modified_boleto' THEN 'Um novo boleto foi adicionado por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='purchase_deleted' THEN 'A solicitação foi deletado por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type='rejected' THEN 'A solicitação foi rejeitada por '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type = 'modified_allocation' AND pre.details->'product'->>'old_dre_name' IS NOT NULL THEN 'O dre da solicitação foi alterada por' ||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type = 'modified_allocation' AND pre.details->'product'->>'old_dre_name' IS NULL THEN 'O dre da solicitação foi adicionado por' ||' '|| CAST(pre.details->'user'->>'name' AS TEXT)
			WHEN pre.event_type = 'modified_boleto' THEN 'O boleto do fornecedor '|| CAST(pre.details->'supplier'->>'name' AS TEXT) || ' foi editado por '|| CAST(pre.details->'user'->'name' AS TEXT) 
			ELSE pre.event_type
		END AS tipo_evento,
	    pr.request_number AS numero_solicitacao,
	    pre.created_at AS dt_criacao_evento,
	    rc.id_protheus AS codigo_cr,
        pr.is_regularization AS regularizacao,
	--pre.details->'product'->>'new_dre_name' AS "novo_dre_atribuido",
    	--pre.details->'product'->>'old_dre_name' AS "dre_alterado",
	    concat('https://portal.gnflorestal.com.br/purchase-requests/',pr.id) AS link_Pedido  
FROM
	purchase_request_events pre
JOIN purchase_requests pr ON pre.purchase_request_id = pr.id
JOIN result_centers rc ON pr.result_center_id = rc.id
WHERE 
	pre.created_at >='2026-06-01'
)
SELECT 
	id_pedido,
	id_criador,
	id_comprador,	
	tipo_evento,
	numero_solicitacao,
	dt_criacao_evento,
    dt_criacao,
	codigo_cr,
    regularizacao,
	link_pedido
FROM tabela
 """
# ============================================================
# QUERY 4: Tabela Auxiliar Pagamentos
# ============================================================
QUERY_AUX_PAGAMENTOS = """
WITH 
-- ============================================================
-- CTE 1: FORNECEDORES
-- ============================================================
fornecedores_split AS (
    SELECT 
        id,
        contact_name AS fornecedor,
        cnpj,
        split_part(id_protheus, '-', 1) AS id_protheus_code
    FROM suppliers
    WHERE id_protheus IS NOT NULL
),
-- ============================================================
-- CTE 2: AGREGADOS (datas de pré-nota)
-- ============================================================
agregados AS ( 
    SELECT
        payment_request_id AS id_pedido_pagamento,
        MIN(CASE WHEN event_type = 'proforma_invoice' THEN created_at END) AS dt_pre_nota_add,
        MIN(CASE WHEN event_type = 'sent_for_proforma_invoice' THEN created_at END) AS dt_envio_para_pre_nota
    FROM payment_request_events
    WHERE event_type IN ('proforma_invoice', 'sent_for_proforma_invoice')
    GROUP BY payment_request_id
),
-- ============================================================
-- CTE 3: TABELA AUXILIAR PAGAMENTOS
-- ============================================================
tabela_aux_pagamentos AS (
    SELECT
        pr.id AS id_pagamento,
        pr.creator_id AS id_criador,
        rc.id_protheus AS cr,
        i.invoice_number AS numero_nf,
        i.emission_date AS dt_emissao_PF,
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
    WHERE pr.created_at >= '2026-05-01'
)
-- ============================================================
-- SELECT FINAL 
-- ============================================================
SELECT 
    tap.*,
    fs.fornecedor,
    fs.cnpj,
    agg.dt_pre_nota_add,
    agg.dt_envio_para_pre_nota
FROM tabela_aux_pagamentos tap
LEFT JOIN fornecedores_split fs ON tap.id_protheus = fs.id_protheus_code
LEFT JOIN agregados agg ON tap.id_pagamento = agg.id_pedido_pagamento
"""
# ============================================================
# QUERY 5: Tabela com os Numeros das Solicitações de Pagamentos e Compras
# ============================================================
QUERY_NUMERO_SOLICITACOES = """
WITH pedidos_pf_pc as (
select
    pr.created_at as criado_em,
    pr.request_number as numero_solicitacao
from
    payment_requests pr
WHERE
    pr.deleted_at is null 
UNION
select
    pr.created_at as criado_em,
    pr.request_number as numero_solicitacao
from
    purchase_requests pr
WHERE
    pr.deleted_at is null 
)
SELECT
    * 
FROM
    pedidos_pf_pc
"""
# ============================================================
# QUERY 6: Tabela Auxiliar Compras
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
    pr.competition_date_dre,
    CONCAT('https://portal.gnflorestal.com.br/purchase-requests/', pr.id) AS link_acesso_pedido
FROM invoices i 
JOIN budgets b ON i.budget_id = b.id
JOIN purchase_requests pr ON b.purchase_request_id = pr.id
JOIN suppliers s ON b.supplier_id = s.id 
JOIN result_centers rc ON pr.result_center_id = rc.id 
WHERE pr.deleted_at IS NULL
  AND pr.created_at >= '2026-05-01'
  AND i.invoice_number IS NOT NULL 
  AND i.invoice_number != ''
)
SELECT
    *
FROM
    tabela_aux_compras
 """
# ============================================================
# EXECUÇÃO
# ============================================================
print("=" * 70)
print("🚀 INICIANDO EXPORTAÇÃO - MODO SERIAL")
print("=" * 70)

os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

# Lista de exportações
exportacoes = [
    (QUERY_COMPRAS, F_PEDIDOS_GERAIS, 'Tabela_Pedidos_Gerais', 'dt_criacao_pedido'),
    (QUERY_F_TABELA_EVENTOS_PF, F_TABELA_EVENTOS_PF, 'Tabela_eventos_PF', 'dt_criacao_evento'),
    (QUERY_F_TABELA_EVENTOS_PC, F_TABELA_EVENTOS_PC, 'Tabela_eventos_PC', 'dt_criacao_evento'),
    (QUERY_AUX_PAGAMENTOS, PARQUET_AUX_PAGAMENTOS, 'Aux Pagamentos', 'criado_em'),
    (QUERY_AUX_COMPRAS, PARQUET_AUX_COMPRAS, 'Aux Compras', 'dt_criacao'),
    (QUERY_NUMERO_SOLICITACOES, PARQUET_NUMERO_SOLICITACOES, 'Numero_Solicitações', 'criado_em'),
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