#!/usr/bin/env python3
"""
Script para adicionar mais 20 registros em cada banco de dados
sem remover os dados existentes.
"""

import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from faker import Faker
import random
from typing import Dict

# Configuração do Faker para português
fake = Faker('pt_BR')
# Não usar seed fixo para gerar dados diferentes dos anteriores
random.seed()

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

# Produtos realistas por categoria (mesma estrutura do seed_databases.py)
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
        produto_base = random.choice(produtos)
        variacoes = ['', ' Premium', ' Pro', ' Plus', ' Deluxe', ' Edition']
        if random.random() < 0.3:
            produto_base += random.choice(variacoes)
        return produto_base
    else:
        return fake.word().capitalize() + ' ' + fake.word().capitalize()


def adicionar_clientes_postgres(conn, num_clientes: int = 20) -> Dict[str, int]:
    """Adiciona novos clientes no PostgreSQL."""
    print(f"\n[PostgreSQL] Adicionando {num_clientes} novos clientes...")
    cursor = conn.cursor()
    
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
    
    conn.commit()
    cursor.close()
    print(f"[OK] {len(clientes_map)} novos clientes adicionados")
    return clientes_map


def adicionar_produtos_postgres(conn, num_produtos: int = 20) -> list:
    """Adiciona novos produtos no PostgreSQL."""
    print(f"\n[PostgreSQL] Adicionando {num_produtos} novos produtos...")
    cursor = conn.cursor()
    
    produtos_ids = []
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
    
    for i in range(num_produtos):
        tipo = random.choice(TIPOS_PRODUTOS)
        produto = gerar_nome_produto(tipo)
        
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
    
    conn.commit()
    cursor.close()
    print(f"[OK] {len(produtos_ids)} novos produtos adicionados")
    return produtos_ids


def adicionar_compras_postgres(conn, clientes_map: Dict[str, int], produtos_ids: list):
    """Adiciona novas compras no PostgreSQL."""
    print(f"\n[PostgreSQL] Adicionando compras para os novos clientes...")
    cursor = conn.cursor()
    
    # Buscar TODOS os produtos disponíveis (antigos + novos)
    cursor.execute("SELECT id FROM produtos;")
    todos_produtos_ids = [row[0] for row in cursor.fetchall()]
    
    compras_data = []
    for cliente_id in clientes_map.values():
        num_compras = random.randint(2, 10)
        for _ in range(num_compras):
            # Escolher um produto aleatório de todos os produtos disponíveis
            produto_id = random.choice(todos_produtos_ids)
            
            compras_data.append((
                produto_id,
                fake.date_between(start_date='-1y', end_date='today'),
                cliente_id
            ))
    
    cursor.executemany("""
        INSERT INTO compras (id_produto, data, id_cliente)
        VALUES (%s, %s, %s);
    """, compras_data)
    
    conn.commit()
    cursor.close()
    print(f"[OK] {len(compras_data)} novas compras adicionadas")


def adicionar_documentos_mongodb(clientes_map: Dict[str, int]):
    """Adiciona novos documentos no MongoDB."""
    print(f"\n[MongoDB] Adicionando {len(clientes_map)} novos documentos...")
    
    client = MongoClient(
        f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@"
        f"{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/"
        f"?authSource={MONGODB_CONFIG['authSource']}"
    )
    
    db = client['recomendacao_db']
    colecao = db['clientes_interesses']
    
    documentos = []
    for cpf, cliente_id in clientes_map.items():
        num_interesses = random.randint(3, 8)
        interesses_cliente = random.sample(INTERESSES, num_interesses)
        
        documento = {
            'id_cliente': cliente_id,
            'cpf': cpf,
            'nome': fake.name(),
            'interesses': interesses_cliente,
            'data_atualizacao': fake.date_time_between(start_date='-6m', end_date='now').isoformat()
        }
        documentos.append(documento)
    
    colecao.insert_many(documentos)
    client.close()
    print(f"[OK] {len(documentos)} novos documentos inseridos")


def adicionar_pessoas_neo4j(clientes_map: Dict[str, int]):
    """Adiciona novas pessoas e relacionamentos no Neo4j."""
    print(f"\n[Neo4j] Adicionando {len(clientes_map)} novas pessoas...")
    
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    # Criar nós (Pessoas)
    with driver.session() as session:
        for cpf, cliente_id in clientes_map.items():
            session.run("""
                CREATE (p:Pessoa {
                    id: $id,
                    cpf: $cpf,
                    nome: $nome
                })
            """, id=cliente_id, cpf=cpf, nome=fake.name())
        
        print(f"[OK] {len(clientes_map)} novas pessoas criadas")
        
        # Buscar todas as pessoas (antigas e novas) para criar relacionamentos
        result = session.run("MATCH (p:Pessoa) RETURN p.id as id")
        todas_pessoas = [record['id'] for record in result]
        
        # Criar relacionamentos AMIGO_DE para os novos clientes
        num_amizades = 0
        for cliente_id in clientes_map.values():
            num_amigos = random.randint(2, 5)
            # Escolher amigos de todas as pessoas (incluindo as antigas)
            amigos = random.sample(
                [c for c in todas_pessoas if c != cliente_id],
                min(num_amigos, len(todas_pessoas) - 1)
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
        
        print(f"[OK] {num_amizades} novos relacionamentos AMIGO_DE criados")
    
    driver.close()


def main():
    """Função principal."""
    print("=" * 60)
    print("Script para Adicionar Mais Dados aos Bancos")
    print("=" * 60)
    print("\nEste script adiciona 20 novos registros em cada banco")
    print("SEM remover os dados existentes.\n")
    
    try:
        # Conectar no PostgreSQL
        print("[PostgreSQL] Conectando...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("[OK] Conectado")
        
        # Adicionar novos clientes
        clientes_map = adicionar_clientes_postgres(conn, num_clientes=20)
        
        # Adicionar novos produtos
        produtos_ids = adicionar_produtos_postgres(conn, num_produtos=20)
        
        # Adicionar compras para os novos clientes
        adicionar_compras_postgres(conn, clientes_map, produtos_ids)
        
        conn.close()
        
        # Adicionar documentos no MongoDB
        adicionar_documentos_mongodb(clientes_map)
        
        # Adicionar pessoas no Neo4j
        adicionar_pessoas_neo4j(clientes_map)
        
        print("\n" + "=" * 60)
        print("[OK] Dados adicionados com sucesso!")
        print("=" * 60)
        print(f"\nResumo:")
        print(f"  - PostgreSQL: +20 clientes, +20 produtos, +compras")
        print(f"  - MongoDB: +20 documentos com interesses")
        print(f"  - Neo4j: +20 pessoas com relacionamentos")
        print(f"\n[AVISO] IMPORTANTE: Execute a sincronização na API")
        print(f"   (POST /api/sync_data) para atualizar o Redis!")
        
    except Exception as e:
        print(f"\n[ERRO] Erro ao adicionar dados: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

