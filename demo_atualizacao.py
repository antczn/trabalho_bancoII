#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de demonstração para apresentação do trabalho.
Altera dados nos bancos originais e demonstra a atualização via API.
"""

import os
import sys
# Garantir que o encoding padrão é UTF-8
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    # Configurar stdout/stderr para UTF-8
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
import json
import requests
import time
import random
from typing import Dict, List

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

API_BASE_URL = 'http://localhost:8000'


def print_separator(titulo: str = ""):
    """Imprime um separador visual."""
    print("\n" + "=" * 70)
    if titulo:
        print(f"  {titulo}")
        print("=" * 70)
    else:
        print("=" * 70)


def print_subsection(titulo: str):
    """Imprime um subtítulo."""
    print(f"\n{'─' * 70}")
    print(f"  {titulo}")
    print(f"{'─' * 70}")


def ler_dados_redis(cliente_id: int) -> Dict:
    """Lê dados de um cliente do Redis."""
    r = redis.Redis(**REDIS_CONFIG)
    key = f"cliente:{cliente_id}"
    data = r.get(key)
    r.close()
    if data:
        return json.loads(data)
    return None


def mostrar_dados_cliente(cliente_id: int, fonte: str = "Redis"):
    """Mostra os dados de um cliente."""
    if fonte == "Redis":
        dados = ler_dados_redis(cliente_id)
        if not dados:
            print(f"  [ERRO] Cliente {cliente_id} não encontrado no Redis")
            return
        
        pessoais = dados.get('dados_pessoais', {})
        print(f"\n  Dados Pessoais:")
        print(f"     Nome: {pessoais.get('nome')}")
        print(f"     Cidade: {pessoais.get('cidade')}")
        print(f"     Email: {pessoais.get('email')}")
        
        print(f"\n  Compras: {len(dados.get('compras', []))}")
        if dados.get('compras'):
            print(f"     Última compra: {dados['compras'][-1].get('produto')}")
        
        print(f"\n  Interesses: {', '.join(dados.get('interesses', []))}")
        print(f"\n  Amigos: {len(dados.get('amigos', []))}")
        if dados.get('amigos'):
            print(f"     Primeiro amigo: {dados['amigos'][0].get('nome')}")
        
        print(f"\n  Última atualização: {dados.get('ultima_atualizacao', 'N/A')}")


def alterar_dados_postgres():
    """Altera dados no PostgreSQL."""
    print_subsection("ALTERANDO DADOS NO POSTGRESQL")
    
    config = POSTGRES_CONFIG.copy()
    config['client_encoding'] = 'UTF8'
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    
    # Buscar um cliente para alterar
    cursor.execute("SELECT id, nome, cidade, email FROM clientes LIMIT 1;")
    cliente = cursor.fetchone()
    
    if not cliente:
        print("  [ERRO] Nenhum cliente encontrado")
        conn.close()
        return None
    
    cliente_id, nome_antigo, cidade_antiga, email_antigo = cliente
    
    print(f"\n  Cliente selecionado: ID {cliente_id}")
    print(f"     Nome atual: {nome_antigo}")
    print(f"     Cidade atual: {cidade_antiga}")
    print(f"     Email atual: {email_antigo}")
    
    # Alterar dados
    novo_nome = f"{nome_antigo} (Atualizado)"
    nova_cidade = "São Paulo" if cidade_antiga != "São Paulo" else "Rio de Janeiro"
    novo_email = f"atualizado_{cliente_id}@exemplo.com"
    
    cursor.execute("""
        UPDATE clientes 
        SET nome = %s, cidade = %s, email = %s
        WHERE id = %s
    """, (novo_nome, nova_cidade, novo_email, cliente_id))
    
    # Adicionar uma nova compra
    cursor.execute("SELECT id FROM produtos ORDER BY RANDOM() LIMIT 1;")
    produto_id = cursor.fetchone()[0]
    
    cursor.execute("""
        INSERT INTO compras (id_produto, data, id_cliente)
        VALUES (%s, CURRENT_DATE, %s)
        RETURNING id;
    """, (produto_id, cliente_id))
    compra_id = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT p.produto FROM compras c
        JOIN produtos p ON c.id_produto = p.id
        WHERE c.id = %s
    """, (compra_id,))
    produto_nome = cursor.fetchone()[0]
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n  [OK] Dados alterados:")
    print(f"     Novo nome: {novo_nome}")
    print(f"     Nova cidade: {nova_cidade}")
    print(f"     Novo email: {novo_email}")
    print(f"     Nova compra adicionada: {produto_nome}")
    
    return cliente_id


def alterar_dados_mongodb(cliente_id: int):
    """Altera dados no MongoDB."""
    print_subsection("ALTERANDO DADOS NO MONGODB")
    
    client = MongoClient(
        f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
        f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
        f"?authSource={MONGODB_CONFIG['authSource']}"
    )
    
    db = client['recomendacao_db']
    colecao = db['clientes_interesses']
    
    # Buscar documento atual
    doc = colecao.find_one({'id_cliente': cliente_id})
    
    if not doc:
        print(f"  [ERRO] Cliente {cliente_id} não encontrado no MongoDB")
        client.close()
        return
    
    interesses_antigos = doc.get('interesses', [])
    print(f"\n  Cliente ID: {cliente_id}")
    print(f"     Interesses atuais: {', '.join(interesses_antigos)}")
    
    # Adicionar novos interesses
    novos_interesses = ['tecnologia', 'viagens', 'culinária']
    interesses_atualizados = list(set(interesses_antigos + novos_interesses))
    
    colecao.update_one(
        {'id_cliente': cliente_id},
        {'$set': {
            'interesses': interesses_atualizados,
            'data_atualizacao': time.strftime('%Y-%m-%dT%H:%M:%S')
        }}
    )
    
    print(f"\n  [OK] Interesses atualizados:")
    print(f"     Novos interesses: {', '.join(interesses_atualizados)}")
    
    client.close()


def alterar_dados_neo4j(cliente_id: int):
    """Altera dados no Neo4j."""
    print_subsection("ALTERANDO DADOS NO NEO4J")
    
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    with driver.session() as session:
        # Buscar pessoa atual
        result = session.run("""
            MATCH (p:Pessoa {id: $id})
            RETURN p.nome as nome, 
                   [(p)-[:AMIGO_DE]->(a) | a.nome] as amigos
        """, id=cliente_id)
        
        record = result.single()
        if not record:
            print(f"  [ERRO] Cliente {cliente_id} não encontrado no Neo4j")
            driver.close()
            return
        
        nome_atual = record['nome']
        amigos_atuais = record['amigos']
        
        print(f"\n  Pessoa ID: {cliente_id}")
        print(f"     Nome atual: {nome_atual}")
        print(f"     Amigos atuais: {len(amigos_atuais)}")
        
        # Buscar todas as pessoas para criar novo relacionamento
        result = session.run("""
            MATCH (p:Pessoa)
            WHERE p.id <> $id
            RETURN p.id as id, p.nome as nome
        """, id=cliente_id)
        
        todas_pessoas = [(record['id'], record['nome']) for record in result]
        
        # Selecionar aleatoriamente até 3 pessoas que ainda não são amigas
        import random
        pessoas_disponiveis = [
            (pid, pnome) for pid, pnome in todas_pessoas 
            if pnome not in amigos_atuais
        ]
        
        if not pessoas_disponiveis:
            pessoas_disponiveis = todas_pessoas[:3]
        else:
            pessoas_disponiveis = random.sample(
                pessoas_disponiveis, 
                min(3, len(pessoas_disponiveis))
            )
        
        novos_amigos = []
        for novo_amigo_id, novo_amigo_nome in pessoas_disponiveis:
            # Verificar se já existe relacionamento
            check = session.run("""
                MATCH (p1:Pessoa {id: $id1})-[r:AMIGO_DE]-(p2:Pessoa {id: $id2})
                RETURN count(r) as existe
            """, id1=cliente_id, id2=novo_amigo_id)
            
            existe = check.single()['existe'] > 0
            
            if not existe:
                # Criar relacionamento bidirecional
                session.run("""
                    MATCH (p1:Pessoa {id: $id1})
                    MATCH (p2:Pessoa {id: $id2})
                    MERGE (p1)-[:AMIGO_DE]->(p2)
                    MERGE (p2)-[:AMIGO_DE]->(p1)
                """, id1=cliente_id, id2=novo_amigo_id)
                
                novos_amigos.append(novo_amigo_nome)
        
        print(f"\n  [OK] Novos relacionamentos criados:")
        print(f"     Novos amigos adicionados: {len(novos_amigos)}")
        for amigo in novos_amigos:
            print(f"       - {amigo}")
    
    driver.close()


def verificar_redis_antes(cliente_id: int):
    """Verifica dados no Redis antes da atualização."""
    print_subsection("VERIFICANDO REDIS (ANTES DA ATUALIZAÇÃO)")
    
    dados = ler_dados_redis(cliente_id)
    if dados:
        pessoais = dados.get('dados_pessoais', {})
        print(f"\n  [AVISO] Dados no Redis estão DESATUALIZADOS:")
        print(f"     Nome: {pessoais.get('nome')}")
        print(f"     Cidade: {pessoais.get('cidade')}")
        print(f"     Interesses: {len(dados.get('interesses', []))}")
        print(f"     Amigos: {len(dados.get('amigos', []))}")
        print(f"     Última atualização: {dados.get('ultima_atualizacao', 'N/A')}")
    else:
        print(f"\n  [ERRO] Cliente {cliente_id} não encontrado no Redis")


def sincronizar_via_api():
    """Chama a API para sincronizar dados."""
    print_subsection("SINCRONIZANDO DADOS VIA API")
    
    print(f"\n  Chamando POST {API_BASE_URL}/api/sync_data...")
    
    try:
        response = requests.post(f"{API_BASE_URL}/api/sync_data", timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n  [OK] Sincronização concluída!")
            print(f"     Clientes processados: {data.get('clientes_processados', 0)}")
            print(f"     Timestamp: {data.get('timestamp', 'N/A')}")
            return True
        else:
            print(f"\n  [ERRO] Erro na sincronização: {response.status_code}")
            print(f"     {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"\n  [ERRO] API não está rodando em {API_BASE_URL}")
        print(f"     Certifique-se de que a API está executando (python app.py)")
        return False
    except Exception as e:
        print(f"\n  [ERRO] Erro ao sincronizar: {e}")
        return False


def verificar_redis_depois(cliente_id: int):
    """Verifica dados no Redis depois da atualização."""
    print_subsection("VERIFICANDO REDIS (DEPOIS DA ATUALIZAÇÃO)")
    
    dados = ler_dados_redis(cliente_id)
    if dados:
        pessoais = dados.get('dados_pessoais', {})
        print(f"\n  [OK] Dados no Redis foram ATUALIZADOS:")
        print(f"     Nome: {pessoais.get('nome')}")
        print(f"     Cidade: {pessoais.get('cidade')}")
        print(f"     Email: {pessoais.get('email')}")
        print(f"     Compras: {len(dados.get('compras', []))}")
        print(f"     Interesses: {', '.join(dados.get('interesses', []))}")
        print(f"     Amigos: {len(dados.get('amigos', []))}")
        print(f"     Última atualização: {dados.get('ultima_atualizacao', 'N/A')}")
    else:
        print(f"\n  [ERRO] Cliente {cliente_id} não encontrado no Redis")


def demonstrar_consulta_api():
    """Demonstra consulta via API."""
    print_subsection("CONSULTANDO DADOS VIA API (DO REDIS)")
    
    try:
        response = requests.get(f"{API_BASE_URL}/api/clientes", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n  [OK] Consulta bem-sucedida!")
            print(f"     Total de clientes: {data.get('total', 0)}")
            print(f"     Fonte: Redis (consolidado)")
            
            # Mostrar alguns clientes
            clientes = data.get('clientes', [])[:3]
            print(f"\n  Primeiros 3 clientes:")
            for cliente in clientes:
                print(f"     - {cliente.get('nome')} (ID: {cliente.get('id')})")
                print(f"       Compras: {cliente.get('num_compras')}, "
                      f"Amigos: {cliente.get('num_amigos')}, "
                      f"Interesses: {cliente.get('num_interesses')}")
        else:
            print(f"\n  [ERRO] Erro na consulta: {response.status_code}")
            
    except Exception as e:
        print(f"\n  [ERRO] Erro ao consultar API: {e}")


def main():
    """Função principal de demonstração."""
    print_separator("DEMONSTRAÇÃO: ATUALIZAÇÃO DE DADOS VIA API")
    
    print("\nEste script demonstra:")
    print("  1. Alteração de dados nos bancos originais (PostgreSQL, MongoDB, Neo4j)")
    print("  2. Verificação de que o Redis está desatualizado")
    print("  3. Uso do front-end para sincronizar/atualizar bases")
    print("  4. Consulta dos dados atualizados via front-end")
    
    input("\nPressione ENTER para começar a demonstração...")
    
    try:
        # Passo 1: Alterar dados no PostgreSQL
        cliente_id = alterar_dados_postgres()
        
        if not cliente_id:
            print("\n[ERRO] Não foi possível encontrar um cliente para alterar")
            return 1
        
        input("\nPressione ENTER para continuar...")
        
        # Passo 2: Alterar dados no MongoDB
        alterar_dados_mongodb(cliente_id)
        
        input("\nPressione ENTER para continuar...")
        
        # Passo 3: Alterar dados no Neo4j
        alterar_dados_neo4j(cliente_id)
        
        input("\nPressione ENTER para continuar...")
        
        # Passo 4: Verificar Redis (deve estar desatualizado)
        verificar_redis_antes(cliente_id)
        
        print_subsection("SINCRONIZAÇÃO VIA FRONT-END")
        print("\n  Agora use o botão 'Sincronizar/Atualizar Bases' no front-end.")
        print("  Este script não chama mais a API para atualizar o Redis.")
        print("  Após a sincronização pelo front, consulte os dados atualizados por lá.")
        input("\nPressione ENTER quando finalizar a sincronização no front-end...")
        
        print_separator("DEMONSTRAÇÃO CONCLUÍDA")
        print("\n[OK] Resumo da demonstração:")
        print("  - Dados foram alterados nos bancos originais")
        print("  - Redis estava desatualizado")
        print("  - Sincronização deve ser feita pelo front-end (botão 'Sincronizar/Atualizar Bases')")
        print("  - Dados atualizados podem ser consultados pelo front-end")

    except Exception as e:
        print(f"\n[ERRO] Erro durante a demonstração: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

