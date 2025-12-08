#!/usr/bin/env python3
"""
Script para popular os bancos de dados com dados fictícios consistentes.
Mantém a integridade referencial entre PostgreSQL, MongoDB e Neo4j.
"""

import psycopg2
from psycopg2 import sql
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from faker import Faker
import random
import time
from typing import List, Dict, Tuple

# Configuração do Faker para português
fake = Faker('pt_BR')
Faker.seed(42)  # Para garantir reprodutibilidade
random.seed(42)

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

# Lista de interesses possíveis
INTERESSES = [
    'esportes', 'filmes', 'música', 'tecnologia', 'culinária',
    'viagens', 'leitura', 'jogos', 'fotografia', 'arte',
    'moda', 'automóveis', 'natureza', 'ciência', 'história'
]

# Tipos de produtos
TIPOS_PRODUTOS = [
    'eletrônicos', 'roupas', 'livros', 'casa', 'esportes',
    'beleza', 'alimentos', 'brinquedos', 'ferramentas', 'jogos'
]

# Produtos realistas por categoria
PRODUTOS_POR_TIPO = {
    'eletrônicos': [
        'Smartphone Samsung Galaxy', 'iPhone 15 Pro', 'Notebook Dell Inspiron',
        'Tablet iPad Air', 'Smart TV LG 55"', 'Fone de Ouvido Bluetooth',
        'Mouse Gamer Logitech', 'Teclado Mecânico RGB', 'Webcam Full HD',
        'Caixa de Som JBL', 'Smartwatch Apple Watch', 'Carregador Wireless',
        'Power Bank 20000mAh', 'Monitor 27" 4K', 'Roteador Wi-Fi 6'
    ],
    'roupas': [
        'Camiseta Básica Algodão', 'Calça Jeans Skinny', 'Tênis Nike Air Max',
        'Jaqueta Corta Vento', 'Vestido Floral Verão', 'Shorts Esportivo',
        'Blusa de Moletom', 'Sapato Social Couro', 'Bolsa Feminina',
        'Óculos de Sol Ray-Ban', 'Relógio Casio', 'Cinto de Couro',
        'Meia Esportiva', 'Gorro de Lã', 'Luvas de Inverno'
    ],
    'livros': [
        'O Pequeno Príncipe', 'Dom Casmurro', '1984 - George Orwell',
        'A Arte da Guerra', 'O Hobbit', 'Harry Potter e a Pedra Filosofal',
        'Cem Anos de Solidão', 'O Alquimista', 'A Menina que Roubava Livros',
        'O Código Da Vinci', 'Percy Jackson', 'A Culpa é das Estrelas',
        'O Senhor dos Anéis', 'Crime e Castigo', 'Orgulho e Preconceito'
    ],
    'casa': [
        'Aspirador de Pó Robot', 'Panela de Pressão', 'Jogo de Pratos',
        'Cafeteira Expresso', 'Micro-ondas 30L', 'Frigideira Antiaderente',
        'Liquidificador 1000W', 'Ferro de Passar', 'Ventilador de Teto',
        'Luminária de Mesa', 'Cortina Blackout', 'Tapete Persa',
        'Almofada Decorativa', 'Quadro Decorativo', 'Vaso de Cerâmica'
    ],
    'esportes': [
        'Bicicleta Mountain Bike', 'Esteira Elétrica', 'Halteres Ajustáveis',
        'Bola de Futebol', 'Raquete de Tênis', 'Skate Completo',
        'Patins Inline', 'Corda de Pular', 'Yoga Mat',
        'Mochila Esportiva', 'Garrafa Térmica', 'Protetor Solar FPS 50',
        'Óculos de Natação', 'Boné Esportivo', 'Tênis de Corrida'
    ],
    'beleza': [
        'Kit de Maquiagem', 'Perfume Importado', 'Creme Hidratante Facial',
        'Shampoo Anticaspa', 'Condicionador Reparador', 'Máscara Facial',
        'Esmalte de Unhas', 'Batom Matte', 'Base Líquida',
        'Pincel de Maquiagem', 'Secador de Cabelo', 'Chapinha Alisadora',
        'Creme para Mãos', 'Protetor Labial', 'Água Micelar'
    ],
    'alimentos': [
        'Café em Grãos Premium', 'Azeite de Oliva Extra Virgem', 'Chocolate Belga',
        'Vinho Tinto Reserva', 'Queijo Brie', 'Mel Orgânico',
        'Granola Caseira', 'Biscoito Artesanal', 'Geleia de Frutas',
        'Pasta de Amendoim', 'Castanha do Pará', 'Açaí Premium',
        'Iogurte Grego', 'Suco Natural', 'Água de Coco'
    ],
    'brinquedos': [
        'Lego Creator', 'Boneca Barbie', 'Carrinho de Controle Remoto',
        'Quebra-Cabeça 1000 Peças', 'Jogo de Tabuleiro', 'Pelúcia Ursinho',
        'Kit de Pintura', 'Bicicleta Infantil', 'Patins Infantil',
        'Bola de Basquete', 'Boneco Action Figure', 'Kit de Magia',
        'Triciclo', 'Balanço Infantil', 'Carrinho de Bebê'
    ],
    'ferramentas': [
        'Furadeira Elétrica', 'Parafusadeira Sem Fio', 'Serra Circular',
        'Multímetro Digital', 'Jogo de Chaves de Fenda', 'Martelo de Borracha',
        'Alicate Universal', 'Nível a Laser', 'Fita Métrica',
        'Chave Inglesa Ajustável', 'Lixadeira Orbital', 'Soldador Elétrico',
        'Serra Tico-Tico', 'Pistola de Cola Quente', 'Kit de Brocas'
    ],
    'jogos': [
        'PlayStation 5', 'Xbox Series X', 'Nintendo Switch',
        'Jogo FIFA 24', 'Jogo Call of Duty', 'Controle Gamer',
        'Headset Gamer', 'Mousepad RGB', 'Cadeira Gamer',
        'Teclado Mecânico Gamer', 'Monitor Curvo 144Hz', 'Placa de Vídeo RTX',
        'Jogo The Witcher 3', 'Jogo GTA V', 'Jogo Minecraft'
    ]
}


def gerar_nome_produto(tipo: str) -> str:
    """Gera um nome de produto realista baseado no tipo."""
    produtos = PRODUTOS_POR_TIPO.get(tipo, [])
    if produtos:
        # Escolher um produto da lista e adicionar variação
        produto_base = random.choice(produtos)
        # Adicionar variações ocasionais
        variacoes = ['', ' Premium', ' Pro', ' Plus', ' Deluxe', ' Edition']
        if random.random() < 0.3:  # 30% de chance de adicionar variação
            produto_base += random.choice(variacoes)
        return produto_base
    else:
        # Fallback para tipos não mapeados
        return fake.word().capitalize() + ' ' + fake.word().capitalize()


def wait_for_services():
    """Aguarda os serviços estarem prontos."""
    print("Aguardando serviços ficarem prontos...")
    max_retries = 30
    retry_delay = 2
    
    # PostgreSQL
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.close()
            print("[OK] PostgreSQL pronto")
            break
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_delay)
    
    # MongoDB
    for i in range(max_retries):
        try:
            client = MongoClient(
                f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
                f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
                f"?authSource={MONGODB_CONFIG['authSource']}"
            )
            client.admin.command('ping')
            client.close()
            print("[OK] MongoDB pronto")
            break
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_delay)
    
    # Neo4j
    for i in range(max_retries):
        try:
            driver = GraphDatabase.driver(
                NEO4J_CONFIG['uri'],
                auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
            )
            driver.verify_connectivity()
            driver.close()
            print("[OK] Neo4j pronto")
            break
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_delay)
    
    # Redis
    for i in range(max_retries):
        try:
            r = redis.Redis(**REDIS_CONFIG)
            r.ping()
            print("[OK] Redis pronto")
            break
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_delay)


def setup_postgres_schema(conn):
    """Cria as tabelas no PostgreSQL."""
    print("\n[PostgreSQL] Criando esquema...")
    cursor = conn.cursor()
    
    # Limpar tabelas existentes
    cursor.execute("DROP TABLE IF EXISTS compras CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS produtos CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS clientes CASCADE;")
    
    # Criar tabela Clientes
    cursor.execute("""
        CREATE TABLE clientes (
            id SERIAL PRIMARY KEY,
            cpf VARCHAR(11) UNIQUE NOT NULL,
            nome VARCHAR(255) NOT NULL,
            endereco VARCHAR(255),
            cidade VARCHAR(100),
            uf VARCHAR(2),
            email VARCHAR(255)
        );
    """)
    
    # Criar tabela Produtos
    cursor.execute("""
        CREATE TABLE produtos (
            id SERIAL PRIMARY KEY,
            produto VARCHAR(255) NOT NULL,
            valor DECIMAL(10, 2) NOT NULL,
            quantidade INTEGER NOT NULL,
            tipo VARCHAR(50) NOT NULL
        );
    """)
    
    # Criar tabela Compras
    cursor.execute("""
        CREATE TABLE compras (
            id SERIAL PRIMARY KEY,
            id_produto INTEGER NOT NULL,
            data DATE NOT NULL,
            id_cliente INTEGER NOT NULL,
            FOREIGN KEY (id_produto) REFERENCES produtos(id),
            FOREIGN KEY (id_cliente) REFERENCES clientes(id)
        );
    """)
    
    conn.commit()
    cursor.close()
    print("[OK] Esquema criado com sucesso")


def populate_postgres(conn, num_clientes: int = 50, num_produtos: int = 30):
    """Popula o PostgreSQL com dados fictícios."""
    print(f"\n[PostgreSQL] Populando com {num_clientes} clientes e {num_produtos} produtos...")
    cursor = conn.cursor()
    
    # Gerar clientes
    clientes_data = []
    clientes_map = {}
    for i in range(num_clientes):
        cpf = fake.cpf().replace('.', '').replace('-', '')
        nome = fake.name()
        endereco = fake.address()
        cidade = fake.city()
        uf = fake.state_abbr()
        email = fake.email()
        
        cursor.execute("""
            INSERT INTO clientes (cpf, nome, endereco, cidade, uf, email)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, cpf;
        """, (cpf, nome, endereco, cidade, uf, email))
        
        cliente_id, cpf_returned = cursor.fetchone()
        clientes_map[cpf_returned] = cliente_id
    
    print(f"[OK] {len(clientes_map)} clientes inseridos")
    
    # Gerar produtos
    produtos_ids = []
    for i in range(num_produtos):
        tipo = random.choice(TIPOS_PRODUTOS)
        produto = gerar_nome_produto(tipo)
        
        # Valores mais realistas baseados no tipo
        valores_por_tipo = {
            'eletrônicos': (100.0, 5000.0),
            'roupas': (30.0, 500.0),
            'livros': (20.0, 80.0),
            'casa': (50.0, 800.0),
            'esportes': (40.0, 2000.0),
            'beleza': (15.0, 300.0),
            'alimentos': (10.0, 150.0),
            'brinquedos': (25.0, 400.0),
            'ferramentas': (30.0, 600.0),
            'jogos': (50.0, 6000.0)
        }
        
        min_valor, max_valor = valores_por_tipo.get(tipo, (10.0, 1000.0))
        valor = round(random.uniform(min_valor, max_valor), 2)
        quantidade = random.randint(1, 100)
        
        cursor.execute("""
            INSERT INTO produtos (produto, valor, quantidade, tipo)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (produto, valor, quantidade, tipo))
        
        produto_id = cursor.fetchone()[0]
        produtos_ids.append(produto_id)
    
    print(f"[OK] {len(produtos_ids)} produtos inseridos")
    
    # Gerar compras (cada cliente faz 2-10 compras)
    compras_data = []
    for cliente_id in clientes_map.values():
        num_compras = random.randint(2, 10)
        for _ in range(num_compras):
            compras_data.append((
                random.choice(produtos_ids),
                fake.date_between(start_date='-1y', end_date='today'),
                cliente_id
            ))
    
    cursor.executemany("""
        INSERT INTO compras (id_produto, data, id_cliente)
        VALUES (%s, %s, %s);
    """, compras_data)
    
    conn.commit()
    cursor.close()
    print(f"[OK] {len(compras_data)} compras inseridas")
    
    return clientes_map


def populate_mongodb(clientes_map: Dict[str, int]):
    """Popula o MongoDB com interesses dos clientes."""
    print("\n[MongoDB] Populando interesses dos clientes...")
    
    client = MongoClient(
        f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
        f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
        f"?authSource={MONGODB_CONFIG['authSource']}"
    )
    
    db = client['recomendacao_db']
    colecao = db['clientes_interesses']
    
    # Limpar coleção existente
    colecao.delete_many({})
    
    # Inserir documentos
    documentos = []
    for cpf, cliente_id in clientes_map.items():
        num_interesses = random.randint(3, 8)
        interesses_cliente = random.sample(INTERESSES, num_interesses)
        
        documento = {
            'id_cliente': cliente_id,
            'cpf': cpf,
            'nome': fake.name(),  # Pode ser diferente do PostgreSQL para simular dados resumidos
            'interesses': interesses_cliente,
            'data_atualizacao': fake.date_time_between(start_date='-6m', end_date='now').isoformat()
        }
        documentos.append(documento)
    
    colecao.insert_many(documentos)
    client.close()
    print(f"[OK] {len(documentos)} documentos inseridos")


def populate_neo4j(clientes_map: Dict[str, int]):
    """Popula o Neo4j com pessoas e relacionamentos de amizade."""
    print("\n[Neo4j] Populando rede de amigos...")
    
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    # Limpar dados existentes
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    
    # Criar nós (Pessoas)
    with driver.session() as session:
        # Criar todas as pessoas primeiro
        for cpf, cliente_id in clientes_map.items():
            session.run("""
                CREATE (p:Pessoa {
                    id: $id,
                    cpf: $cpf,
                    nome: $nome
                })
            """, id=cliente_id, cpf=cpf, nome=fake.name())
        
        print(f"[OK] {len(clientes_map)} pessoas criadas")
        
        # Criar relacionamentos AMIGO_DE
        # Cada pessoa terá 2-5 amigos aleatórios
        clientes_list = list(clientes_map.values())
        num_amizades = 0
        
        for cliente_id in clientes_list:
            num_amigos = random.randint(2, 5)
            amigos = random.sample(
                [c for c in clientes_list if c != cliente_id],
                min(num_amigos, len(clientes_list) - 1)
            )
            
            for amigo_id in amigos:
                # Criar relacionamento bidirecional (evitar duplicatas)
                if cliente_id < amigo_id:
                    session.run("""
                        MATCH (p1:Pessoa {id: $id1})
                        MATCH (p2:Pessoa {id: $id2})
                        MERGE (p1)-[:AMIGO_DE]->(p2)
                        MERGE (p2)-[:AMIGO_DE]->(p1)
                    """, id1=cliente_id, id2=amigo_id)
                    num_amizades += 2
        
        print(f"[OK] {num_amizades} relacionamentos AMIGO_DE criados")
    
    driver.close()


def test_redis():
    """Testa a conexão com Redis e adiciona uma chave de teste."""
    print("\n[Redis] Testando conexão...")
    
    r = redis.Redis(**REDIS_CONFIG)
    r.set('teste_conexao', 'OK', ex=3600)  # Expira em 1 hora
    valor = r.get('teste_conexao')
    
    if valor:
        print("[OK] Redis conectado e funcionando")
        print(f"  Chave de teste: teste_conexao = {valor}")
    else:
        print("[ERRO] Erro ao conectar no Redis")
    
    r.close()


def main():
    """Função principal."""
    print("=" * 60)
    print("Script de Povoamento de Bancos de Dados")
    print("=" * 60)
    
    try:
        # Aguardar serviços
        wait_for_services()
        
        # Conectar no PostgreSQL
        print("\n[PostgreSQL] Conectando...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("[OK] Conectado")
        
        # Configurar e popular PostgreSQL
        setup_postgres_schema(conn)
        clientes_map = populate_postgres(conn, num_clientes=50, num_produtos=30)
        conn.close()
        
        # Popular MongoDB
        populate_mongodb(clientes_map)
        
        # Popular Neo4j
        populate_neo4j(clientes_map)
        
        # Testar Redis
        test_redis()
        
        print("\n" + "=" * 60)
        print("[OK] Povoamento concluído com sucesso!")
        print("=" * 60)
        print(f"\nResumo:")
        print(f"  - PostgreSQL: 50 clientes, 30 produtos, ~300 compras")
        print(f"  - MongoDB: 50 documentos com interesses")
        print(f"  - Neo4j: 50 pessoas com rede de amizades")
        print(f"  - Redis: Conectado e testado")
        
    except Exception as e:
        print(f"\n[ERRO] Erro durante o povoamento: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

