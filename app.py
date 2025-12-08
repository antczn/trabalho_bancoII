#!/usr/bin/env python3
"""
API REST para integração de bases de dados.
Consolida dados do PostgreSQL, MongoDB e Neo4j no Redis.
"""

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import os
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
import json
from datetime import datetime

app = FastAPI(title="Sistema de Recomendação - API de Integração")

# Montar arquivos estáticos
app.mount("/static", StaticFiles(directory="static"), name="static")

# Configurações de conexão
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'recomendacao_db',
    'user': 'postgres',
    'password': 'postgres'
}

MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'admin123',
    'authSource': 'admin'
}

NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'neo4j123'
}

REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'decode_responses': True
}

# Modelos Pydantic
class ClienteResumo(BaseModel):
    id: int
    cpf: str
    nome: str
    cidade: str
    uf: str

class AmigoInfo(BaseModel):
    id: int
    nome: str
    cpf: str

class CompraInfo(BaseModel):
    id: int
    produto: str
    valor: float
    data: str
    tipo: str

class RecomendacaoInfo(BaseModel):
    cliente_id: int
    cliente_nome: str
    recomendacoes: List[Dict[str, Any]]


# Funções auxiliares de conexão
def get_postgres_connection():
    """Retorna conexão com PostgreSQL."""
    return psycopg2.connect(**POSTGRES_CONFIG)

def get_mongodb_client():
    """Retorna cliente MongoDB."""
    return MongoClient(
        f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
        f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
        f"?authSource={MONGODB_CONFIG['authSource']}"
    )

def get_neo4j_driver():
    """Retorna driver Neo4j."""
    return GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )

def get_redis_client():
    """Retorna cliente Redis."""
    return redis.Redis(**REDIS_CONFIG)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve a página HTML principal."""
    try:
        if os.path.exists("static/index.html"):
            return FileResponse("static/index.html")
        else:
            return "<h1>Arquivo index.html não encontrado</h1>"
    except Exception as e:
        return f"<h1>Erro ao carregar página: {str(e)}</h1>"


@app.post("/api/sync_data")
async def sync_data():
    """
    Rota de ETL: Consolida dados de PostgreSQL, MongoDB e Neo4j no Redis.
    Limpa o Redis e recria os dados consolidados.
    """
    try:
        print("Iniciando sincronização de dados...")
        
        # Conectar nos bancos
        pg_conn = get_postgres_connection()
        mongo_client = get_mongodb_client()
        neo4j_driver = get_neo4j_driver()
        redis_client = get_redis_client()
        
        # Limpar Redis
        print("Limpando Redis...")
        redis_client.flushdb()
        
        # Buscar todos os clientes do PostgreSQL
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute("""
            SELECT id, cpf, nome, endereco, cidade, uf, email
            FROM clientes
            ORDER BY id
        """)
        clientes_pg = pg_cursor.fetchall()
        
        # Buscar compras do PostgreSQL
        pg_cursor.execute("""
            SELECT c.id_cliente, c.id, c.data, p.produto, p.valor, p.tipo
            FROM compras c
            JOIN produtos p ON c.id_produto = p.id
            ORDER BY c.id_cliente, c.data
        """)
        compras_pg = pg_cursor.fetchall()
        
        # Organizar compras por cliente
        compras_por_cliente = {}
        for compra in compras_pg:
            cliente_id = compra[0]
            if cliente_id not in compras_por_cliente:
                compras_por_cliente[cliente_id] = []
            compras_por_cliente[cliente_id].append({
                'id': compra[1],
                'data': compra[2].isoformat() if compra[2] else None,
                'produto': compra[3],
                'valor': float(compra[4]),
                'tipo': compra[5]
            })
        
        # Buscar interesses do MongoDB
        mongo_db = mongo_client['recomendacao_db']
        mongo_collection = mongo_db['clientes_interesses']
        interesses_por_cliente = {}
        for doc in mongo_collection.find({}):
            cliente_id = doc.get('id_cliente')
            if cliente_id:
                interesses_por_cliente[cliente_id] = doc.get('interesses', [])
        
        # Buscar amigos do Neo4j
        amigos_por_cliente = {}
        with neo4j_driver.session() as session:
            for cliente_id, _, _, _, _, _, _ in clientes_pg:
                result = session.run("""
                    MATCH (p:Pessoa {id: $cliente_id})-[:AMIGO_DE]->(amigo:Pessoa)
                    RETURN amigo.id as id, amigo.nome as nome, amigo.cpf as cpf
                    ORDER BY amigo.nome
                """, cliente_id=cliente_id)
                
                amigos = []
                for record in result:
                    amigos.append({
                        'id': record['id'],
                        'nome': record['nome'],
                        'cpf': record['cpf']
                    })
                amigos_por_cliente[cliente_id] = amigos
        
        # Gerar recomendações baseadas em compras dos amigos
        def gerar_recomendacoes(cliente_id: int) -> List[Dict[str, Any]]:
            """Gera recomendações baseadas nas compras dos amigos."""
            recomendacoes = []
            amigos = amigos_por_cliente.get(cliente_id, [])
            
            # Produtos que o cliente já comprou
            produtos_cliente = set()
            for compra in compras_por_cliente.get(cliente_id, []):
                produtos_cliente.add(compra['produto'])
            
            # Contar produtos comprados pelos amigos
            produtos_amigos = {}
            for amigo in amigos:
                amigo_id = amigo['id']
                for compra in compras_por_cliente.get(amigo_id, []):
                    produto = compra['produto']
                    if produto not in produtos_cliente:  # Apenas produtos que o cliente não tem
                        if produto not in produtos_amigos:
                            produtos_amigos[produto] = {
                                'produto': produto,
                                'valor': compra['valor'],
                                'tipo': compra['tipo'],
                                'amigos_que_compraram': []
                            }
                        produtos_amigos[produto]['amigos_que_compraram'].append(amigo['nome'])
            
            # Converter para lista e ordenar por número de amigos que compraram
            recomendacoes = list(produtos_amigos.values())
            recomendacoes.sort(key=lambda x: len(x['amigos_que_compraram']), reverse=True)
            
            return recomendacoes[:10]  # Top 10 recomendações
        
        # Consolidar dados e salvar no Redis
        print(f"Consolidando dados de {len(clientes_pg)} clientes...")
        clientes_consolidados = []
        
        for cliente in clientes_pg:
            cliente_id = cliente[0]
            
            # Dados pessoais
            dados_pessoais = {
                'id': cliente_id,
                'cpf': cliente[1],
                'nome': cliente[2],
                'endereco': cliente[3],
                'cidade': cliente[4],
                'uf': cliente[5],
                'email': cliente[6]
            }
            
            # Compras
            compras = compras_por_cliente.get(cliente_id, [])
            
            # Interesses
            interesses = interesses_por_cliente.get(cliente_id, [])
            
            # Amigos
            amigos = amigos_por_cliente.get(cliente_id, [])
            
            # Recomendações
            recomendacoes = gerar_recomendacoes(cliente_id)
            
            # Objeto consolidado
            cliente_consolidado = {
                'dados_pessoais': dados_pessoais,
                'compras': compras,
                'interesses': interesses,
                'amigos': amigos,
                'recomendacoes': recomendacoes,
                'ultima_atualizacao': datetime.now().isoformat()
            }
            
            # Salvar no Redis (chave: cliente:{id})
            redis_key = f"cliente:{cliente_id}"
            redis_client.set(redis_key, json.dumps(cliente_consolidado, ensure_ascii=False))
            
            clientes_consolidados.append(cliente_consolidado)
        
        # Fechar conexões
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()
        neo4j_driver.close()
        redis_client.close()
        
        print(f"Sincronização concluída! {len(clientes_consolidados)} clientes consolidados.")
        
        return {
            "status": "success",
            "message": f"Dados sincronizados com sucesso",
            "clientes_processados": len(clientes_consolidados),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Erro durante sincronização: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Erro ao sincronizar dados: {str(e)}")


@app.get("/api/clientes")
async def get_clientes():
    """Retorna lista com dados básicos de todos os clientes (do Redis)."""
    try:
        redis_client = get_redis_client()
        
        # Buscar todas as chaves de clientes
        keys = redis_client.keys("cliente:*")
        
        clientes = []
        for key in keys:
            cliente_data = json.loads(redis_client.get(key))
            dados_pessoais = cliente_data.get('dados_pessoais', {})
            clientes.append({
                'id': dados_pessoais.get('id'),
                'cpf': dados_pessoais.get('cpf'),
                'nome': dados_pessoais.get('nome'),
                'cidade': dados_pessoais.get('cidade'),
                'uf': dados_pessoais.get('uf'),
                'email': dados_pessoais.get('email'),
                'num_compras': len(cliente_data.get('compras', [])),
                'num_amigos': len(cliente_data.get('amigos', [])),
                'num_interesses': len(cliente_data.get('interesses', []))
            })
        
        redis_client.close()
        
        # Ordenar por ID
        clientes.sort(key=lambda x: x['id'])
        
        return {
            "status": "success",
            "total": len(clientes),
            "clientes": clientes
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar clientes: {str(e)}")


@app.get("/api/clientes/amigos")
async def get_clientes_amigos():
    """Retorna clientes e seus respectivos amigos (do Redis)."""
    try:
        redis_client = get_redis_client()
        
        keys = redis_client.keys("cliente:*")
        
        clientes_amigos = []
        for key in keys:
            cliente_data = json.loads(redis_client.get(key))
            dados_pessoais = cliente_data.get('dados_pessoais', {})
            amigos = cliente_data.get('amigos', [])
            
            clientes_amigos.append({
                'cliente': {
                    'id': dados_pessoais.get('id'),
                    'nome': dados_pessoais.get('nome'),
                    'cpf': dados_pessoais.get('cpf')
                },
                'amigos': amigos,
                'total_amigos': len(amigos)
            })
        
        redis_client.close()
        
        # Ordenar por nome do cliente
        clientes_amigos.sort(key=lambda x: x['cliente']['nome'])
        
        return {
            "status": "success",
            "total": len(clientes_amigos),
            "clientes_amigos": clientes_amigos
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar clientes e amigos: {str(e)}")


@app.get("/api/clientes/compras")
async def get_clientes_compras():
    """Retorna clientes e suas compras realizadas (do Redis)."""
    try:
        redis_client = get_redis_client()
        
        keys = redis_client.keys("cliente:*")
        
        clientes_compras = []
        for key in keys:
            cliente_data = json.loads(redis_client.get(key))
            dados_pessoais = cliente_data.get('dados_pessoais', {})
            compras = cliente_data.get('compras', [])
            
            # Calcular valor total
            valor_total = sum(compra.get('valor', 0) for compra in compras)
            
            clientes_compras.append({
                'cliente': {
                    'id': dados_pessoais.get('id'),
                    'nome': dados_pessoais.get('nome'),
                    'cpf': dados_pessoais.get('cpf'),
                    'cidade': dados_pessoais.get('cidade')
                },
                'compras': compras,
                'total_compras': len(compras),
                'valor_total': round(valor_total, 2)
            })
        
        redis_client.close()
        
        # Ordenar por nome do cliente
        clientes_compras.sort(key=lambda x: x['cliente']['nome'])
        
        return {
            "status": "success",
            "total": len(clientes_compras),
            "clientes_compras": clientes_compras
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar compras: {str(e)}")


@app.get("/api/recomendacoes")
async def get_recomendacoes():
    """Lista os clientes e as recomendações geradas para eles (do Redis)."""
    try:
        redis_client = get_redis_client()
        
        keys = redis_client.keys("cliente:*")
        
        recomendacoes_list = []
        for key in keys:
            cliente_data = json.loads(redis_client.get(key))
            dados_pessoais = cliente_data.get('dados_pessoais', {})
            recomendacoes = cliente_data.get('recomendacoes', [])
            
            if recomendacoes:  # Apenas clientes com recomendações
                recomendacoes_list.append({
                    'cliente_id': dados_pessoais.get('id'),
                    'cliente_nome': dados_pessoais.get('nome'),
                    'cliente_cpf': dados_pessoais.get('cpf'),
                    'recomendacoes': recomendacoes,
                    'total_recomendacoes': len(recomendacoes)
                })
        
        redis_client.close()
        
        # Ordenar por nome do cliente
        recomendacoes_list.sort(key=lambda x: x['cliente_nome'])
        
        return {
            "status": "success",
            "total": len(recomendacoes_list),
            "recomendacoes": recomendacoes_list
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar recomendações: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

