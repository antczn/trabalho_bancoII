#!/usr/bin/env python3
"""
Demonstração rápida e não-interativa para apresentação.
Altera dados e sincroniza automaticamente.
"""

import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
import json
import requests
import time
import random

# Configurações
POSTGRES_CONFIG = {
    'host': 'localhost', 'port': 5432, 'database': 'recomendacao_db',
    'user': 'postgres', 'password': 'postgres'
}

MONGODB_CONFIG = {
    'host': 'localhost', 'port': 27017,
    'username': 'admin', 'password': 'admin123', 'authSource': 'admin'
}

NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j', 'password': 'neo4j123'
}

REDIS_CONFIG = {'host': 'localhost', 'port': 6379, 'decode_responses': True}
API_BASE_URL = 'http://localhost:8000'


def main():
    print("=" * 70)
    print("DEMONSTRAÇÃO RÁPIDA - ATUALIZAÇÃO VIA API")
    print("=" * 70)
    
    # 1. Buscar um cliente
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT id, nome, cidade FROM clientes LIMIT 1;")
    cliente = cursor.fetchone()
    cliente_id, nome_antigo, cidade_antiga = cliente
    
    print(f"\nCliente selecionado: {nome_antigo} (ID: {cliente_id})")
    print(f"Cidade atual: {cidade_antiga}")
    
    # 2. Alterar no PostgreSQL
    nova_cidade = "São Paulo" if cidade_antiga != "São Paulo" else "Rio de Janeiro"
    cursor.execute("UPDATE clientes SET cidade = %s WHERE id = %s", (nova_cidade, cliente_id))
    conn.commit()
    print(f"[OK] PostgreSQL atualizado: cidade = {nova_cidade}")
    
    # 3. Alterar no MongoDB
    client = MongoClient(
        f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
        f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
        f"?authSource={MONGODB_CONFIG['authSource']}"
    )
    db = client['recomendacao_db']
    colecao = db['clientes_interesses']
    doc = colecao.find_one({'id_cliente': cliente_id})
    if doc:
        novos_interesses = list(set(doc.get('interesses', []) + ['tecnologia', 'viagens']))
        colecao.update_one(
            {'id_cliente': cliente_id},
            {'$set': {'interesses': novos_interesses}}
        )
        print(f"[OK] MongoDB atualizado: interesses = {len(novos_interesses)}")
    client.close()
    
    # 4. Verificar Redis (deve estar desatualizado)
    r = redis.Redis(**REDIS_CONFIG)
    key = f"cliente:{cliente_id}"
    dados_antigos = r.get(key)
    if dados_antigos:
        dados = json.loads(dados_antigos)
        cidade_redis = dados.get('dados_pessoais', {}).get('cidade')
        print(f"[AVISO] Redis desatualizado: cidade = {cidade_redis} (deveria ser {nova_cidade})")
    r.close()
    
    # 5. Sincronizar via API
    print(f"\nSincronizando via API...")
    try:
        response = requests.post(f"{API_BASE_URL}/api/sync_data", timeout=60)
        if response.status_code == 200:
            data = response.json()
            print(f"[OK] Sincronização concluída! ({data.get('clientes_processados', 0)} clientes)")
        else:
            print(f"[ERRO] Erro: {response.status_code}")
            return
    except Exception as e:
        print(f"[ERRO] Erro ao sincronizar: {e}")
        print("Certifique-se de que a API está rodando (python app.py)")
        return
    
    # 6. Verificar Redis atualizado
    r = redis.Redis(**REDIS_CONFIG)
    dados_novos = r.get(key)
    if dados_novos:
        dados = json.loads(dados_novos)
        cidade_redis = dados.get('dados_pessoais', {}).get('cidade')
        interesses = dados.get('interesses', [])
        print(f"[OK] Redis atualizado: cidade = {cidade_redis}")
        print(f"[OK] Interesses atualizados: {len(interesses)} interesses")
    r.close()
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("[OK] DEMONSTRAÇÃO CONCLUÍDA!")
    print("=" * 70)
    print("\nDados foram:")
    print("  1. Alterados nos bancos originais (PostgreSQL, MongoDB)")
    print("  2. Redis estava desatualizado")
    print("  3. Sincronizados via API")
    print("  4. Redis agora está atualizado")
    print("\nVerifique no front-end: http://localhost:8000")


if __name__ == '__main__':
    main()

