#!/usr/bin/env python3
"""
Script para testar se todos os bancos de dados estão funcionando corretamente
e se os dados foram populados com sucesso.
"""

import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from typing import Dict, List

# Configurações de conexão (mesmas do seed_databases.py)
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


def test_postgres() -> Dict:
    """Testa conexão e dados no PostgreSQL."""
    print("\n" + "="*60)
    print("TESTANDO POSTGRESQL")
    print("="*60)
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Contar clientes
        cursor.execute("SELECT COUNT(*) FROM clientes;")
        num_clientes = cursor.fetchone()[0]
        print(f"[OK] Clientes: {num_clientes}")
        
        # Mostrar alguns clientes
        cursor.execute("SELECT id, cpf, nome, cidade FROM clientes LIMIT 5;")
        print("\n  Primeiros 5 clientes:")
        for row in cursor.fetchall():
            print(f"    ID: {row[0]}, CPF: {row[1]}, Nome: {row[2]}, Cidade: {row[3]}")
        
        # Contar produtos
        cursor.execute("SELECT COUNT(*) FROM produtos;")
        num_produtos = cursor.fetchone()[0]
        print(f"\n[OK] Produtos: {num_produtos}")
        
        # Mostrar alguns produtos
        cursor.execute("SELECT id, produto, valor, tipo FROM produtos LIMIT 5;")
        print("\n  Primeiros 5 produtos:")
        for row in cursor.fetchall():
            print(f"    ID: {row[0]}, Produto: {row[1]}, Valor: R$ {row[2]:.2f}, Tipo: {row[3]}")
        
        # Contar compras
        cursor.execute("SELECT COUNT(*) FROM compras;")
        num_compras = cursor.fetchone()[0]
        print(f"\n[OK] Compras: {num_compras}")
        
        # Estatísticas de compras
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT id_cliente) as clientes_com_compras,
                AVG(valor) as valor_medio_compra
            FROM compras c
            JOIN produtos p ON c.id_produto = p.id;
        """)
        stats = cursor.fetchone()
        print(f"  Clientes com compras: {stats[0]}")
        print(f"  Valor médio por compra: R$ {stats[1]:.2f}")
        
        # Verificar integridade referencial
        cursor.execute("""
            SELECT COUNT(*) 
            FROM compras c
            LEFT JOIN clientes cl ON c.id_cliente = cl.id
            LEFT JOIN produtos p ON c.id_produto = p.id
            WHERE cl.id IS NULL OR p.id IS NULL;
        """)
        erros_integridade = cursor.fetchone()[0]
        
        if erros_integridade == 0:
            print("\n[OK] Integridade referencial: OK")
        else:
            print(f"\n[ERRO] Erros de integridade: {erros_integridade}")
        
        cursor.close()
        conn.close()
        
        return {
            'status': 'OK',
            'clientes': num_clientes,
            'produtos': num_produtos,
            'compras': num_compras,
            'integridade': erros_integridade == 0
        }
        
    except Exception as e:
        print(f"\n[ERRO] Erro ao conectar/testar PostgreSQL: {e}")
        return {'status': 'ERRO', 'erro': str(e)}


def test_mongodb() -> Dict:
    """Testa conexão e dados no MongoDB."""
    print("\n" + "="*60)
    print("TESTANDO MONGODB")
    print("="*60)
    
    try:
        client = MongoClient(
            f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
            f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
            f"?authSource={MONGODB_CONFIG['authSource']}"
        )
        
        db = client['recomendacao_db']
        colecao = db['clientes_interesses']
        
        # Contar documentos
        num_docs = colecao.count_documents({})
        print(f"[OK] Documentos na coleção 'clientes_interesses': {num_docs}")
        
        # Mostrar alguns documentos
        print("\n  Primeiros 3 documentos:")
        for doc in colecao.find().limit(3):
            print(f"    ID Cliente: {doc.get('id_cliente')}, CPF: {doc.get('cpf')}")
            print(f"    Nome: {doc.get('nome')}")
            print(f"    Interesses: {', '.join(doc.get('interesses', []))}")
            print()
        
        # Estatísticas de interesses
        pipeline = [
            {'$unwind': '$interesses'},
            {'$group': {'_id': '$interesses', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 5}
        ]
        interesses_populares = list(colecao.aggregate(pipeline))
        print("  Top 5 interesses mais comuns:")
        for item in interesses_populares:
            print(f"    {item['_id']}: {item['count']} clientes")
        
        client.close()
        
        return {
            'status': 'OK',
            'documentos': num_docs
        }
        
    except Exception as e:
        print(f"\n[ERRO] Erro ao conectar/testar MongoDB: {e}")
        return {'status': 'ERRO', 'erro': str(e)}


def test_neo4j() -> Dict:
    """Testa conexão e dados no Neo4j."""
    print("\n" + "="*60)
    print("TESTANDO NEO4J")
    print("="*60)
    
    try:
        driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'],
            auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
        )
        
        with driver.session() as session:
            # Contar nós
            result = session.run("MATCH (p:Pessoa) RETURN COUNT(p) as count")
            num_pessoas = result.single()['count']
            print(f"[OK] Pessoas (nós): {num_pessoas}")
            
            # Mostrar algumas pessoas
            result = session.run("MATCH (p:Pessoa) RETURN p.id, p.cpf, p.nome LIMIT 5")
            print("\n  Primeiras 5 pessoas:")
            for record in result:
                print(f"    ID: {record['p.id']}, CPF: {record['p.cpf']}, Nome: {record['p.nome']}")
            
            # Contar relacionamentos
            result = session.run("MATCH ()-[r:AMIGO_DE]->() RETURN COUNT(r) as count")
            num_relacionamentos = result.single()['count']
            print(f"\n[OK] Relacionamentos AMIGO_DE: {num_relacionamentos}")
            
            # Estatísticas de rede
            result = session.run("""
                MATCH (p:Pessoa)-[:AMIGO_DE]->(amigo:Pessoa)
                WITH p, COUNT(amigo) as num_amigos
                RETURN AVG(num_amigos) as media_amigos, 
                       MIN(num_amigos) as min_amigos,
                       MAX(num_amigos) as max_amigos
            """)
            stats = result.single()
            print(f"\n  Estatísticas da rede:")
            print(f"    Média de amigos por pessoa: {stats['media_amigos']:.2f}")
            print(f"    Mínimo de amigos: {stats['min_amigos']}")
            print(f"    Máximo de amigos: {stats['max_amigos']}")
            
            # Verificar se há pessoas isoladas (sem amigos)
            result = session.run("""
                MATCH (p:Pessoa)
                WHERE NOT (p)-[:AMIGO_DE]-()
                RETURN COUNT(p) as isoladas
            """)
            isoladas = result.single()['isoladas']
            if isoladas == 0:
                print(f"\n[OK] Todas as pessoas têm pelo menos um amigo")
            else:
                print(f"\n  Pessoas sem amigos: {isoladas}")
        
        driver.close()
        
        return {
            'status': 'OK',
            'pessoas': num_pessoas,
            'relacionamentos': num_relacionamentos
        }
        
    except Exception as e:
        print(f"\n[ERRO] Erro ao conectar/testar Neo4j: {e}")
        return {'status': 'ERRO', 'erro': str(e)}


def test_redis() -> Dict:
    """Testa conexão com Redis."""
    print("\n" + "="*60)
    print("TESTANDO REDIS")
    print("="*60)
    
    try:
        r = redis.Redis(**REDIS_CONFIG)
        
        # Testar ping
        r.ping()
        print("[OK] Conexão estabelecida")
        
        # Verificar chave de teste
        teste = r.get('teste_conexao')
        if teste:
            print(f"[OK] Chave de teste encontrada: teste_conexao = {teste}")
        else:
            print("  Chave de teste não encontrada (normal se ainda não foi populada)")
        
        # Informações do servidor
        info = r.info('server')
        print(f"\n  Versão do Redis: {info.get('redis_version', 'N/A')}")
        print(f"  Uptime: {info.get('uptime_in_seconds', 0)} segundos")
        
        # Contar chaves
        num_chaves = r.dbsize()
        print(f"  Chaves no banco: {num_chaves}")
        
        r.close()
        
        return {
            'status': 'OK',
            'chaves': num_chaves
        }
        
    except Exception as e:
        print(f"\n[ERRO] Erro ao conectar/testar Redis: {e}")
        return {'status': 'ERRO', 'erro': str(e)}


def test_consistencia_cross_database() -> Dict:
    """Testa consistência entre os bancos (mesmo CPF/ID)."""
    print("\n" + "="*60)
    print("TESTANDO CONSISTÊNCIA ENTRE BANCOS")
    print("="*60)
    
    try:
        # Buscar CPFs do PostgreSQL
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT id, cpf FROM clientes LIMIT 10;")
        postgres_data = {row[1]: row[0] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        
        # Buscar do MongoDB
        client = MongoClient(
            f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
            f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
            f"?authSource={MONGODB_CONFIG['authSource']}"
        )
        db = client['recomendacao_db']
        colecao = db['clientes_interesses']
        
        mongodb_data = {}
        for doc in colecao.find({'cpf': {'$in': list(postgres_data.keys())}}):
            mongodb_data[doc['cpf']] = doc['id_cliente']
        
        client.close()
        
        # Buscar do Neo4j
        driver = GraphDatabase.driver(
            NEO4J_CONFIG['uri'],
            auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
        )
        
        neo4j_data = {}
        with driver.session() as session:
            cpf_list = list(postgres_data.keys())
            for cpf in cpf_list:
                result = session.run(
                    "MATCH (p:Pessoa {cpf: $cpf}) RETURN p.id as id",
                    cpf=cpf
                )
                record = result.single()
                if record:
                    neo4j_data[cpf] = record['id']
        
        driver.close()
        
        # Verificar consistência
        print(f"  Verificando {len(postgres_data)} clientes...")
        
        consistentes = 0
        inconsistentes = []
        
        for cpf, postgres_id in postgres_data.items():
            mongodb_id = mongodb_data.get(cpf)
            neo4j_id = neo4j_data.get(cpf)
            
            if mongodb_id == postgres_id and neo4j_id == postgres_id:
                consistentes += 1
            else:
                inconsistentes.append({
                    'cpf': cpf,
                    'postgres': postgres_id,
                    'mongodb': mongodb_id,
                    'neo4j': neo4j_id
                })
        
        print(f"[OK] Clientes consistentes: {consistentes}/{len(postgres_data)}")
        
        if inconsistentes:
            print(f"\n[ERRO] Clientes inconsistentes: {len(inconsistentes)}")
            for inc in inconsistentes[:3]:  # Mostrar apenas os 3 primeiros
                print(f"    CPF {inc['cpf']}: PG={inc['postgres']}, MDB={inc['mongodb']}, N4J={inc['neo4j']}")
        else:
            print("[OK] Todos os clientes verificados estão consistentes entre os bancos!")
        
        return {
            'status': 'OK' if len(inconsistentes) == 0 else 'AVISO',
            'consistentes': consistentes,
            'inconsistentes': len(inconsistentes)
        }
        
    except Exception as e:
        print(f"\n[ERRO] Erro ao testar consistência: {e}")
        return {'status': 'ERRO', 'erro': str(e)}


def main():
    """Função principal."""
    print("="*60)
    print("TESTE DE INTEGRIDADE DOS BANCOS DE DADOS")
    print("="*60)
    
    resultados = {}
    
    # Testar cada banco
    resultados['postgres'] = test_postgres()
    resultados['mongodb'] = test_mongodb()
    resultados['neo4j'] = test_neo4j()
    resultados['redis'] = test_redis()
    resultados['consistencia'] = test_consistencia_cross_database()
    
    # Resumo final
    print("\n" + "="*60)
    print("RESUMO DOS TESTES")
    print("="*60)
    
    todos_ok = True
    for banco, resultado in resultados.items():
        status = resultado.get('status', 'ERRO')
        if status == 'OK':
            print(f"[OK] {banco.upper()}: OK")
        elif status == 'AVISO':
            print(f"[AVISO] {banco.upper()}: AVISO (verifique acima)")
            todos_ok = False
        else:
            print(f"[ERRO] {banco.upper()}: ERRO")
            todos_ok = False
    
    print("\n" + "="*60)
    if todos_ok:
        print("[OK] TODOS OS TESTES PASSARAM!")
    else:
        print("[ERRO] ALGUNS TESTES FALHARAM - Verifique os detalhes acima")
    print("="*60)
    
    return 0 if todos_ok else 1


if __name__ == '__main__':
    exit(main())

