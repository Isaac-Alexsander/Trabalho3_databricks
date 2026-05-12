-- ============================================================
-- SCRIPT DE SETUP - SeguroDB no Supabase (PostgreSQL)
-- Trabalho 3 - Engenharia de Dados - SATC
-- Domínio: Seguro de Veículos
-- ============================================================

-- ========================
-- 1. TABELAS DE DOMÍNIO
-- ========================

CREATE TABLE IF NOT EXISTS regiao (
    id_regiao     SERIAL PRIMARY KEY,
    nome_regiao   VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS estado (
    id_estado     SERIAL PRIMARY KEY,
    nome_estado   VARCHAR(80) NOT NULL,
    uf            CHAR(2)     NOT NULL,
    id_regiao     INT         NOT NULL REFERENCES regiao(id_regiao)
);

CREATE TABLE IF NOT EXISTS municipio (
    id_municipio  SERIAL PRIMARY KEY,
    nome_municipio VARCHAR(100) NOT NULL,
    id_estado     INT          NOT NULL REFERENCES estado(id_estado)
);

CREATE TABLE IF NOT EXISTS marca (
    id_marca      SERIAL PRIMARY KEY,
    nome_marca    VARCHAR(60) NOT NULL
);

CREATE TABLE IF NOT EXISTS modelo (
    id_modelo     SERIAL PRIMARY KEY,
    nome_modelo   VARCHAR(80) NOT NULL,
    id_marca      INT         NOT NULL REFERENCES marca(id_marca),
    ano_modelo    INT         NOT NULL
);

-- ========================
-- 2. TABELAS DE CLIENTE
-- ========================

CREATE TABLE IF NOT EXISTS cliente (
    id_cliente    SERIAL PRIMARY KEY,
    nome          VARCHAR(120) NOT NULL,
    cpf           CHAR(11)     NOT NULL UNIQUE,
    data_nasc     DATE         NOT NULL,
    email         VARCHAR(120),
    sexo          CHAR(1)      CHECK (sexo IN ('M','F','O'))
);

CREATE TABLE IF NOT EXISTS endereco (
    id_endereco   SERIAL PRIMARY KEY,
    id_cliente    INT          NOT NULL REFERENCES cliente(id_cliente),
    id_municipio  INT          NOT NULL REFERENCES municipio(id_municipio),
    logradouro    VARCHAR(150) NOT NULL,
    numero        VARCHAR(10),
    bairro        VARCHAR(80),
    cep           CHAR(8)
);

CREATE TABLE IF NOT EXISTS telefone (
    id_telefone   SERIAL PRIMARY KEY,
    id_cliente    INT          NOT NULL REFERENCES cliente(id_cliente),
    ddd           CHAR(2)      NOT NULL,
    numero        VARCHAR(9)   NOT NULL,
    tipo          VARCHAR(15)  CHECK (tipo IN ('celular','residencial','comercial'))
);

-- ========================
-- 3. TABELAS DE VEÍCULO
-- ========================

CREATE TABLE IF NOT EXISTS carro (
    id_carro      SERIAL PRIMARY KEY,
    id_modelo     INT          NOT NULL REFERENCES modelo(id_modelo),
    id_cliente    INT          NOT NULL REFERENCES cliente(id_cliente),
    placa         CHAR(7)      NOT NULL UNIQUE,
    cor           VARCHAR(30),
    ano_fabricacao INT         NOT NULL,
    chassi        VARCHAR(17)  NOT NULL UNIQUE
);

-- ========================
-- 4. TABELAS DE SEGURO
-- ========================

CREATE TABLE IF NOT EXISTS apolice (
    id_apolice        SERIAL PRIMARY KEY,
    id_carro          INT           NOT NULL REFERENCES carro(id_carro),
    id_cliente        INT           NOT NULL REFERENCES cliente(id_cliente),
    numero_apolice    VARCHAR(20)   NOT NULL UNIQUE,
    data_inicio       DATE          NOT NULL,
    data_fim          DATE          NOT NULL,
    valor_premio      NUMERIC(10,2) NOT NULL,
    tipo_cobertura    VARCHAR(30)   CHECK (tipo_cobertura IN ('basica','intermediaria','completa')),
    status            VARCHAR(15)   CHECK (status IN ('ativa','cancelada','vencida'))
);

CREATE TABLE IF NOT EXISTS sinistro (
    id_sinistro       SERIAL PRIMARY KEY,
    id_apolice        INT           NOT NULL REFERENCES apolice(id_apolice),
    data_ocorrencia   DATE          NOT NULL,
    data_registro     DATE          NOT NULL DEFAULT CURRENT_DATE,
    tipo_sinistro     VARCHAR(50),
    descricao         TEXT,
    valor_prejuizo    NUMERIC(12,2),
    status_sinistro   VARCHAR(20)   CHECK (status_sinistro IN ('aberto','em_analise','aprovado','negado','pago'))
);

-- ============================================================
-- 5. CARGA DE DADOS
-- ============================================================

-- REGIÕES
INSERT INTO regiao (nome_regiao) VALUES
('Norte'), ('Nordeste'), ('Centro-Oeste'), ('Sudeste'), ('Sul')
ON CONFLICT DO NOTHING;

-- ESTADOS
INSERT INTO estado (nome_estado, uf, id_regiao) VALUES
('Santa Catarina',  'SC', 5),
('Rio Grande do Sul','RS', 5),
('Paraná',          'PR', 5),
('São Paulo',       'SP', 4),
('Minas Gerais',    'MG', 4),
('Rio de Janeiro',  'RJ', 4)
ON CONFLICT DO NOTHING;

-- MUNICÍPIOS
INSERT INTO municipio (nome_municipio, id_estado) VALUES
('Criciúma',         1), ('Florianópolis',  1), ('Joinville',    1),
('Porto Alegre',     2), ('Caxias do Sul',  2),
('Curitiba',         3), ('Londrina',       3),
('São Paulo',        4), ('Campinas',       4),
('Belo Horizonte',   5),
('Rio de Janeiro',   6)
ON CONFLICT DO NOTHING;

-- MARCAS
INSERT INTO marca (nome_marca) VALUES
('Volkswagen'), ('Fiat'), ('Chevrolet'), ('Toyota'), ('Honda'), ('Hyundai')
ON CONFLICT DO NOTHING;

-- MODELOS
INSERT INTO modelo (nome_modelo, id_marca, ano_modelo) VALUES
('Gol',         1, 2020), ('Polo',        1, 2022), ('Virtus',     1, 2023),
('Argo',        2, 2021), ('Strada',      2, 2022), ('Pulse',      2, 2023),
('Onix',        3, 2021), ('Tracker',     3, 2022),
('Corolla',     4, 2022), ('Hilux',       4, 2021),
('Civic',       5, 2022), ('HR-V',        5, 2023),
('HB20',        6, 2021), ('Creta',       6, 2022)
ON CONFLICT DO NOTHING;

-- CLIENTES
INSERT INTO cliente (nome, cpf, data_nasc, email, sexo) VALUES
('Ana Souza',        '12345678901', '1990-03-15', 'ana.souza@email.com',      'F'),
('Bruno Lima',       '23456789012', '1985-07-22', 'bruno.lima@email.com',     'M'),
('Carla Mendes',     '34567890123', '1992-11-08', 'carla.mendes@email.com',   'F'),
('Diego Costa',      '45678901234', '1978-05-30', 'diego.costa@email.com',    'M'),
('Eliane Ferreira',  '56789012345', '1995-01-18', 'eliane.f@email.com',       'F'),
('Felipe Rocha',     '67890123456', '1988-09-25', 'felipe.r@email.com',       'M'),
('Gabriela Alves',   '78901234567', '2000-04-12', 'gabi.alves@email.com',     'F'),
('Henrique Nunes',   '89012345678', '1975-12-03', 'h.nunes@email.com',        'M'),
('Isabela Martins',  '90123456789', '1998-06-20', 'isabela.m@email.com',      'F'),
('João Pereira',     '01234567890', '1983-08-14', 'joao.pereira@email.com',   'M')
ON CONFLICT DO NOTHING;

-- ENDEREÇOS
INSERT INTO endereco (id_cliente, id_municipio, logradouro, numero, bairro, cep) VALUES
(1, 1, 'Rua das Flores',      '123', 'Centro',       '88800000'),
(2, 4, 'Av. Borges de Medeiros', '456', 'Moinhos',   '90000000'),
(3, 6, 'Rua XV de Novembro',  '789', 'Centro',       '80000000'),
(4, 8, 'Av. Paulista',        '1000','Bela Vista',   '01310000'),
(5, 1, 'Rua Henrique Lage',   '55', 'Próspera',      '88813000'),
(6, 2, 'Rua Felipe Schmidt',  '300', 'Centro',       '88010000'),
(7, 9, 'Rua Treze de Maio',   '87', 'Centro',        '13010000'),
(8, 10,'Av. Afonso Pena',     '2000','Centro',        '30130000'),
(9, 3, 'Rua Blumenau',        '450', 'América',      '89204000'),
(10,11,'Rua da Carioca',      '10', 'Centro',        '20051000')
ON CONFLICT DO NOTHING;

-- TELEFONES
INSERT INTO telefone (id_cliente, ddd, numero, tipo) VALUES
(1,'48','999991111','celular'),  (1,'48','33331111','residencial'),
(2,'51','988882222','celular'),
(3,'41','977773333','celular'),  (3,'41','33223322','comercial'),
(4,'11','966664444','celular'),
(5,'48','955553333','celular'),
(6,'48','944442222','celular'),  (6,'48','32221100','residencial'),
(7,'19','933331111','celular'),
(8,'31','922220000','celular'),
(9,'47','911119999','celular'),
(10,'21','900008888','celular')
ON CONFLICT DO NOTHING;

-- CARROS
INSERT INTO carro (id_modelo, id_cliente, placa, cor, ano_fabricacao, chassi) VALUES
(1,  1, 'ABC1234', 'Branco',    2020, 'CHASSI00000000001'),
(4,  2, 'DEF5678', 'Prata',     2021, 'CHASSI00000000002'),
(7,  3, 'GHI9012', 'Preto',     2021, 'CHASSI00000000003'),
(9,  4, 'JKL3456', 'Cinza',     2022, 'CHASSI00000000004'),
(11, 5, 'MNO7890', 'Branco',    2022, 'CHASSI00000000005'),
(13, 6, 'PQR1234', 'Azul',      2021, 'CHASSI00000000006'),
(2,  7, 'STU5678', 'Vermelho',  2022, 'CHASSI00000000007'),
(5,  8, 'VWX9012', 'Branco',    2022, 'CHASSI00000000008'),
(10, 9, 'YZA3456', 'Prata',     2021, 'CHASSI00000000009'),
(6, 10, 'BCD7890', 'Verde',     2023, 'CHASSI00000000010')
ON CONFLICT DO NOTHING;

-- APÓLICES
INSERT INTO apolice (id_carro, id_cliente, numero_apolice, data_inicio, data_fim, valor_premio, tipo_cobertura, status) VALUES
(1,  1, 'AP-2024-0001', '2024-01-01', '2025-01-01', 1800.00, 'completa',      'vencida'),
(2,  2, 'AP-2024-0002', '2024-02-01', '2025-02-01', 1200.00, 'intermediaria', 'vencida'),
(3,  3, 'AP-2024-0003', '2024-03-01', '2025-03-01',  900.00, 'basica',        'ativa'),
(4,  4, 'AP-2024-0004', '2024-04-01', '2025-04-01', 2500.00, 'completa',      'ativa'),
(5,  5, 'AP-2024-0005', '2024-05-01', '2025-05-01', 1500.00, 'intermediaria', 'ativa'),
(6,  6, 'AP-2024-0006', '2024-06-01', '2025-06-01',  800.00, 'basica',        'ativa'),
(7,  7, 'AP-2025-0001', '2025-01-01', '2026-01-01', 1900.00, 'completa',      'ativa'),
(8,  8, 'AP-2025-0002', '2025-02-01', '2026-02-01', 1100.00, 'intermediaria', 'ativa'),
(9,  9, 'AP-2025-0003', '2025-03-01', '2026-03-01', 2200.00, 'completa',      'ativa'),
(10,10, 'AP-2025-0004', '2025-04-01', '2026-04-01',  750.00, 'basica',        'ativa')
ON CONFLICT DO NOTHING;

-- SINISTROS
INSERT INTO sinistro (id_apolice, data_ocorrencia, data_registro, tipo_sinistro, descricao, valor_prejuizo, status_sinistro) VALUES
(1, '2024-03-10', '2024-03-11', 'colisao',    'Batida traseira em semáforo',       8500.00, 'pago'),
(1, '2024-08-22', '2024-08-23', 'furto',      'Furto de pertences no interior',    1200.00, 'aprovado'),
(2, '2024-05-15', '2024-05-16', 'colisao',    'Colisão lateral em cruzamento',    12000.00, 'pago'),
(3, '2024-07-04', '2024-07-05', 'incendio',   'Princípio de incêndio no motor',    5000.00, 'em_analise'),
(4, '2024-09-18', '2024-09-19', 'roubo',      'Roubo do veículo completo',        45000.00, 'aprovado'),
(5, '2024-11-30', '2024-12-01', 'colisao',    'Abalroamento em garagem',           3200.00, 'pago'),
(7, '2025-02-14', '2025-02-15', 'granizo',    'Danos causados por granizo',        4800.00, 'em_analise'),
(8, '2025-03-22', '2025-03-23', 'colisao',    'Colisão frontal em rodovia',       22000.00, 'aberto'),
(9, '2025-04-10', '2025-04-11', 'furto',      'Furto de roda e pneu',              2800.00, 'em_analise'),
(10,'2025-04-25', '2025-04-26', 'alagamento', 'Danos por alagamento urbano',       9500.00, 'aberto')
ON CONFLICT DO NOTHING;

-- Fim do script
SELECT 'Setup concluído! Tabelas e dados carregados.' AS status;
