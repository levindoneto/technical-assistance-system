import mysql.connector

def connect():
    global db_connection
    global cursor
    try:
        db_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="new_password",
            database="fbdproj"
        )
        cursor = db_connection.cursor()
        print("Connect to db: fbdproj\nCursor: ", cursor)
        # return db_connection.cursor()
    except:
        try:
            db_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd="new_password"
            )
            cursor = db_connection.cursor()
            print("Connect to create new db")
        except:
            print("Can't connect to database")
            return -1

connect()

def initDb(createDb=False, executeQuery=True):
    if(createDb):
        cursor.execute("CREATE DATABASE fbdproj")
        print("Database fbdproj created")
    if(executeQuery):
        cursor.execute("""        
            CREATE TABLE marca (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(50) NOT NULL UNIQUE
            );

            CREATE TABLE pedido (
                numero INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                data_abertura DATETIME NOT NULL,
                observacoes BLOB,
                tipo CHAR(1) NOT NULL,
                data_entrega DATETIME,
                fk_pessoa_id INT UNSIGNED NOT NULL
            );

            CREATE TABLE pessoa (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(255) NOT NULL,
                cidade VARCHAR(50),
                logradouro VARCHAR(50),
                estado CHAR(2),
                telefone VARCHAR(15),
                fornecedor BOOLEAN NOT NULL,
                email VARCHAR(50),
                pessoa_juridica BOOLEAN NOT NULL,
                cnpj CHAR(14) UNIQUE,
                ie VARCHAR(15),
                im VARCHAR(15),
                cpf CHAR(11) UNIQUE,
                rg VARCHAR(15),
                nascimento DATE
            );

            CREATE TABLE ordem_servico (
                fk_pedido_numero INT UNSIGNED PRIMARY KEY,
                equipamento VARCHAR(255) NOT NULL,
                informacoes VARCHAR(255),
                prob_informado BLOB NOT NULL,
                prob_constatado BLOB,
                servicos_realizados BLOB
            );

            CREATE TABLE caixa (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(50) NOT NULL,
                numero_conta VARCHAR(15),
                nome_banco VARCHAR(50),
                numero_agencia VARCHAR(15)
            );

            CREATE TABLE meio_pagamento (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(50) NOT NULL UNIQUE
            );

            CREATE TABLE lancamento (
                descricao VARCHAR(50),
                data_lancamento DATE NOT NULL,
                data_vencimento DATE,
                data_quitacao DATE,
                valor DECIMAL(19,4) NOT NULL,
                debito BOOLEAN NOT NULL,
                fk_meio_pagamento_id INT UNSIGNED NOT NULL,
                fk_caixa_id INT UNSIGNED NOT NULL,
                fk_pedido_numero INT UNSIGNED
            );

            CREATE TABLE categoria (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(50) NOT NULL UNIQUE
            );

            CREATE TABLE produto (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                descricao VARCHAR(255) NOT NULL UNIQUE,
                valor DECIMAL(19,4) NOT NULL,
                valor_custo DECIMAL(19,4) NOT NULL,
                estoque_atual INT UNSIGNED NOT NULL,
                estoque_minimo INT UNSIGNED,
                fk_marca_id INT UNSIGNED
            );

            CREATE TABLE item_pedido (
                fk_pedido_numero INT UNSIGNED NOT NULL,
                fk_produto_id INT UNSIGNED NOT NULL,
                quantidade DOUBLE NOT NULL,
                desconto DECIMAL(19,4),
                valor_unitario DECIMAL(19,4) NOT NULL
            );

            CREATE TABLE item_servico (
                fk_ordem_servico_numero INT UNSIGNED NOT NULL,
                fk_servico_id INT UNSIGNED NOT NULL,
                desconto DECIMAL(19,4),
                valor_unitario DECIMAL(19,4) NOT NULL,
                quantidade DOUBLE NOT NULL
            );

            CREATE TABLE fornecimento (
                fk_pessoa_id INT UNSIGNED NOT NULL,
                fk_produto_id INT UNSIGNED NOT NULL,
                PRIMARY KEY (fk_pessoa_id, fk_produto_id)
            );

            CREATE TABLE classificacao (
                fk_categoria_id INT UNSIGNED NOT NULL,
                fk_produto_id INT UNSIGNED NOT NULL,
                PRIMARY KEY (fk_categoria_id, fk_produto_id)
            );

            CREATE TABLE servico (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                descricao VARCHAR(255) NOT NULL UNIQUE,
                valor DECIMAL(19,4) NOT NULL
            );


            ALTER TABLE pedido ADD CONSTRAINT fk_pedido_pessoa
                FOREIGN KEY (fk_pessoa_id)
                REFERENCES pessoa (id);
            
            ALTER TABLE ordem_servico ADD CONSTRAINT fk_ordem_servico_pedido
                FOREIGN KEY (fk_pedido_numero)
                REFERENCES pedido (numero)
                ON DELETE CASCADE;
            
            ALTER TABLE lancamento ADD CONSTRAINT fk_lancamento_meio_pagamento
                FOREIGN KEY (fk_meio_pagamento_id)
                REFERENCES meio_pagamento (id);
            
            ALTER TABLE lancamento ADD CONSTRAINT fk_lancamento_caixa
                FOREIGN KEY (fk_caixa_id)
                REFERENCES caixa (id);
            
            ALTER TABLE lancamento ADD CONSTRAINT fk_lancamento_pedido
                FOREIGN KEY (fk_pedido_numero)
                REFERENCES pedido (numero);
            
            ALTER TABLE produto ADD CONSTRAINT fk_produto_marca
                FOREIGN KEY (fk_marca_id)
                REFERENCES marca (id)
                ON DELETE SET NULL;
            
            ALTER TABLE item_pedido ADD CONSTRAINT fk_item_pedido
                FOREIGN KEY (fk_pedido_numero)
                REFERENCES pedido (numero)
                ON DELETE CASCADE;
            
            ALTER TABLE item_pedido ADD CONSTRAINT fk_item_produto
                FOREIGN KEY (fk_produto_id)
                REFERENCES produto (id);
            
            ALTER TABLE item_servico ADD CONSTRAINT fk_item_ordem_servico
                FOREIGN KEY (fk_ordem_servico_numero)
                REFERENCES ordem_servico (fk_pedido_numero)
                ON DELETE CASCADE;
            
            ALTER TABLE item_servico ADD CONSTRAINT fk_item_servico
                FOREIGN KEY (fk_servico_id)
                REFERENCES servico (id);
            
            ALTER TABLE fornecimento ADD CONSTRAINT fk_fornecimento_pessoa
                FOREIGN KEY (fk_pessoa_id)
                REFERENCES pessoa (id)
                ON DELETE CASCADE;
            
            ALTER TABLE fornecimento ADD CONSTRAINT fk_fornecimento_recurso
                FOREIGN KEY (fk_produto_id)
                REFERENCES produto (id)
                ON DELETE CASCADE;
            
            ALTER TABLE classificacao ADD CONSTRAINT fk_classificacao_produto
                FOREIGN KEY (fk_produto_id)
                REFERENCES produto (id)
                ON DELETE CASCADE;
            
            ALTER TABLE classificacao ADD CONSTRAINT fk_classificacao_categoria
                FOREIGN KEY (fk_categoria_id)
                REFERENCES categoria (id)
                ON DELETE CASCADE;
                
            DELIMITER $$

            CREATE PROCEDURE pessoa_verifica_tipo (
                IN cpf CHAR(11),
                IN rg VARCHAR(15),
                IN nascimento DATE,
                IN pessoa_juridica BOOLEAN,
                IN cnpj CHAR(14),
                IN ie VARCHAR(15),
                IN im VARCHAR(15)
                )
                BEGIN
                    IF (pessoa_juridica) THEN
                        IF (cnpj IS NULL) THEN
                            SIGNAL SQLSTATE '45000'
                                SET MESSAGE_TEXT = 'Uma pessoa juridica deve ter CNPJ.';
                        ELSE
                            IF NOT ((cpf IS NULL) AND (rg IS NULL) AND (nascimento IS NULL)) THEN
                            SIGNAL SQLSTATE '45000'
                                SET MESSAGE_TEXT = 'Uma pessoa juridica nao pode ter dados de fisica.';
                            END IF;
                        END IF;
                    ELSE
                        IF (cpf IS NULL) THEN
                            SIGNAL SQLSTATE '45000'
                                SET MESSAGE_TEXT = 'Uma pessoa fisica deve ter CPF.';
                        ELSE
                            IF NOT ((cnpj IS NULL) AND (ie IS NULL) AND (im IS NULL)) THEN
                                SIGNAL SQLSTATE '45000'
                                    SET MESSAGE_TEXT = 'Uma pessoa fisica nao pode ter dados de juridica.';
                            END IF;
                        END IF;
                    END IF;
                END$$
                
            CREATE TRIGGER pessoa_verifica_tipo_insert
            BEFORE INSERT ON pessoa
            FOR EACH ROW
                BEGIN
                    CALL pessoa_verifica_tipo(NEW.cpf, NEW.rg, NEW.nascimento, NEW.pessoa_juridica, NEW.cnpj, NEW.ie, NEW.im);
                END$$
                            
            CREATE TRIGGER pessoa_verifica_tipo_update
            BEFORE UPDATE ON pessoa
            FOR EACH ROW
                BEGIN
                    CALL pessoa_verifica_tipo(NEW.cpf, NEW.rg, NEW.nascimento, NEW.pessoa_juridica, NEW.cnpj, NEW.ie, NEW.im);
                END$$
                
            CREATE PROCEDURE pedido_verifica_tipo (
                IN tipo CHAR(1))
                BEGIN
                    IF NOT (tipo IN ('C', 'V', 'S')) THEN
                        SIGNAL SQLSTATE '45000'
                                SET MESSAGE_TEXT = 'Tipo de pedido invalido. Deve ser V, C ou S.';
                    END IF;
                END$$

            CREATE TRIGGER pedido_verifica_tipo_insert
            BEFORE INSERT ON pedido
            FOR EACH ROW
                BEGIN
                    CALL pedido_verifica_tipo(NEW.tipo);
                END$$
                            
            CREATE TRIGGER pedido_verifica_tipo_update
            BEFORE UPDATE ON pedido
            FOR EACH ROW
                BEGIN
                    CALL pedido_verifica_tipo(NEW.tipo);
                END$$
                
            CREATE PROCEDURE lancamento_verifica_descricao (
                IN descricao VARCHAR(50),
                IN fk_pedido_numero INT UNSIGNED)
                BEGIN
                    IF (descricao IS NULL AND fk_pedido_numero IS NULL) THEN
                        SIGNAL SQLSTATE '45000'
                                SET MESSAGE_TEXT = 'Um lancamento deve ter pelo menos uma descricao.';
                    END IF;
                END$$
                
            CREATE TRIGGER lancamento_verifica_descricao_insert
            BEFORE INSERT ON lancamento
            FOR EACH ROW
                BEGIN
                    CALL lancamento_verifica_descricao(NEW.descricao, NEW.fk_pedido_numero);
                END$$
                            
            CREATE TRIGGER lancamento_verifica_descricao_update
            BEFORE UPDATE ON lancamento
            FOR EACH ROW
                BEGIN
                    CALL lancamento_verifica_descricao(NEW.descricao, NEW.fk_pedido_numero);
                END$$
                
            DELIMITER ;

            INSERT INTO meio_pagamento (nome) VALUES
            ('Dinheiro'), ('Crédito 30 dias'), ('Crédito Parcelado'), ('Débito'),
            ('Depósito ou Transferência'), ('Boleto Bancário'), ('Cheque');

                
            INSERT INTO categoria (nome) VALUES
            ('Acessórios'),('Periféricos'),('Áudio'),('Vídeo'),('Componentes'),
            ('Hardware'),('Armazenamento'),('Software'),('Impressão'),('Energia'),
            ('Monitoramento'),('Telefonia'),('Segurança'),('Tecnologia'),('Rede');

            INSERT INTO marca (nome) VALUES
            ('Sony'),('Microsoft'),('Google'),('Apple'),('TP-Link'),
            ('Ubiquiti'),('LG'),('Multilaser'),('C3 Tech'),('Logitech'),
            ('Kingston'),('SanDisk'),('Intelbras'),('Western Digital'),('Samsung');

            INSERT INTO caixa (nome, nome_banco, numero_agencia, numero_conta) VALUES
            ('Conta Banrisul', 'Banco do Estado do Rio Grande do Sul', '0897', '351643860-0'),
            ('Conta Bradesco', 'Banco Bradesco', '7187', '32107-9'),
            ('Caixa Dinheiro', NULL, NULL, NULL),
            ('PagSeguro', NULL, NULL, NULL);

            INSERT INTO produto (descricao, valor, valor_custo, estoque_atual, estoque_minimo, fk_marca_id) VALUES
            ('Headset USB H390', 249.9900, 140.8000, 4, 8, 10),
            ('Licença Windows 10 Pro 64 Bits COEM', 949.9900, 790.8300, 3, 0, 2),
            ('Monitor TV LED 28" Full HD', 899.9000, 654.9000, 4, 2, 7),
            ('SSD A400 120GB', 299.9000, 190.4800, 8, 5, 11),
            ('Emenda RJ45', 9.0000, 3.5000, 20, 10, NULL),
            ('Teclado USB Multimidia KB-34BR', 34.9000, 21.9000, 5, 5, 9);

            INSERT INTO servico (descricao, valor) VALUES
            ('Reinstalação de Sistema Operacional', 100.0000),
            ('Hora Técnica', 80.0000),
            ('Limpeza Interna', 60.0000),
            ('Regravação de BIOS', 150.0000),
            ('Backup de dados 50GB', 20.0000);

            INSERT INTO classificacao (fk_categoria_id, fk_produto_id) VALUES
            (1, 1), (3, 1), (8, 2), (3, 3), (4, 3),
            (6, 4), (7, 4), (14, 4), (15, 5), (2, 6);

            INSERT INTO pessoa (nome, logradouro, cidade, estado, telefone, email, fornecedor, cpf, rg, nascimento, pessoa_juridica, cnpj, ie, im) VALUES
            ('Alexandrino De Alencar', 'Av. Dorival de Oliveira, 231', 'Gravataí', 'RS', '51987473884', 'aledeale@gmail.com', 0, '01283884773', '5744882308', '1991-06-04', 0, NULL, NULL, NULL),
            ('Levindonho de Moraes', 'Rua Mostardeiros, 123, Ap 201', 'Porto Alegre', 'RS', '5130432334', NULL, 0, '03994811234', NULL, '1992-12-03', 0, NULL, NULL, NULL),
            ('Fulandro de Noronha', 'Rua dos Cebolenses, 282', 'Recife', 'PE', NULL, 'fulandro@fuleiros.com', 1, '01001093988', NULL, '1959-11-19', 0, NULL, NULL, NULL),
            ('Gaucha Informatica', 'Av. Brasil, 162', 'Porto Alegre', 'RS', '5132697800', NULL, 1, NULL, NULL, NULL, 1, '07955127000103', '963213733', '0894065041'),
            ('RGE Sul Distribuidora de Energia S.A.', NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, 1, '02016440000162', 'ISENTO', '45045'),
            ('Pauta Distribuidora e Logítstica', 'R Vidal Procopio Lohn, 144', 'São José', 'SC', '4830637639', 'fiscal@pauta.com.br', 1, NULL, NULL, NULL, 1, '83064741000163', NULL, NULL),
            ('Ravasauto Com. de Veic. LTDA', 'Av. Nilo Peçanha, 1034', 'Porto Alegre', 'RS', '5130125445', 'compras@ravasauto.com.br', 0, NULL, NULL, NULL, 1, '60080789000123', '0960150010', NULL);

            INSERT INTO pedido (data_abertura, observacoes, tipo, data_entrega, fk_pessoa_id) VALUES
            ('2020-07-28 09:22:46', 'Cliente aguarda aprovação da diretoria da empresa.', 'V', NULL, 7), 
            ('2020-07-29 10:34:21', NULL, 'V', '2020-07-29 10:35:42', 1), 
            ('2020-08-01 14:45:58', NULL, 'C', '2020-08-02 13:24:41', 4), 
            ('2020-08-02 14:15:17', 'Serviço pronto, aguarda retirada.', 'S', NULL, 3), 
            ('2020-08-03 13:28:23', 'Cliente solicita urgência.', 'S', '2020-08-03 17:28:44', 2),
            ('2020-08-03 17:30:35', NULL, 'S', NULL, 2);

            INSERT INTO ordem_servico (fk_pedido_numero, equipamento, informacoes, prob_informado, prob_constatado, servicos_realizados) VALUES
            (4, 'Dekltop Dell Optiplex', 'Sem tampa lateral', 'Máquina se desliga sozinha após um tempo de uso.', 'Sujeira no dissipador causa aquecimento elevado e a máquina se desliga por segurança.', 'Limpeza interna e troca da pasta térmica.'),
            (5, 'Notebook Samsung RV415', 'Com carregador', 'Máquina muito lenta', 'Sistema sobrecarregado com vírus', 'Instalação de SSD e reinstalação de sistema operacional.'),
            (6, 'Monitor Dell 24"', 'Com fonte', 'Não dá vídeo', 'Placa principal com capacitores estourados', 'Troca dos capacitores');

            INSERT INTO item_servico (fk_ordem_servico_numero, fk_servico_id, desconto, valor_unitario, quantidade) VALUES
            (4, 3, NULL, 60.0000, 1),(5, 1, 10.0000, 100.0000, 1),(6, 2, NULL, 80.0000, 2);

            INSERT INTO item_pedido (fk_pedido_numero, fk_produto_id, quantidade, desconto, valor_unitario) VALUES
            (5, 4, 1, NULL, 299.9000),(1,2,3,30.0000,949.9000),(2,6,2,NULL,34.9000),(2,1,1,NULL,249.9000),(3,1,10,100.0000,140.8000);

            INSERT INTO fornecimento (fk_pessoa_id, fk_produto_id) VALUES
            (3, 1), (6, 2), (6, 3), (4, 4), (6, 4), (4, 5);

            INSERT INTO lancamento (descricao, data_lancamento, data_vencimento, data_quitacao, valor, debito, fk_meio_pagamento_id, fk_caixa_id, fk_pedido_numero) VALUES
            ('Conta de Luz', '2020-08-05', '2020-08-25', NULL, 155.8500, 1, 6, 2, NULL),
            (NULL, '2020-08-01', NULL, '2020-08-01', 1308.0000, 1, 5, 1, 3),
            (NULL, '2020-07-29', NULL, '2020-07-29', 100.0000, 0, 1, 3, 2),
            (NULL, '2020-07-29', '2020-08-29', NULL, 219.7000, 0, 2, 4, 2),
            (NULL, '2020-08-03', '2020-08-17', NULL, 194.9500, 0, 6, 1, 5),
            (NULL, '2020-08-03', '2020-08-31', NULL, 194.9500, 0, 6, 1, 5),
            ('Rendimentos CDB Automático', '2020-08-01', NULL, '2020-08-01', 23.8500, 0, 5, 2, NULL),
            ('Aluguel', '2020-08-01', '2020-08-05', '2020-08-05', 950.0000, 1, 1, 3, NULL),
            ('Capital Social', '2020-07-01', NULL, '2020-07-01', 8000.0000, 0, 5, 1, NULL),
            ('Capital Social', '2020-07-01', NULL, '2020-07-01', 4800.0000, 0, 5, 2, NULL),
            ('Capital Social', '2020-07-01', NULL, '2020-07-01', 1380.0000, 0, 1, 3, NULL);

            CREATE VIEW lista_ordem_servico AS
            SELECT data_abertura,numero,data_entrega,id id_pessoa,nome,telefone,equipamento
            FROM pedido PD
            JOIN ordem_servico OS ON PD.numero = OS.fk_pedido_numero
            JOIN pessoa PE ON PD.fk_pessoa_id = PE.id
            WHERE tipo = 'S'
            ORDER BY data_abertura;

            CREATE VIEW lista_pedido_venda AS
            SELECT data_abertura,numero,data_entrega,nome,telefone
            FROM pedido PD
            JOIN pessoa PE ON PD.fk_pessoa_id = PE.id
            WHERE tipo = 'V'
            ORDER BY data_abertura;

            CREATE VIEW lista_pedido_compra AS
            SELECT data_abertura,numero,data_entrega,nome,telefone
            FROM pedido PD
            JOIN pessoa PE ON PD.fk_pessoa_id = PE.id
            WHERE tipo = 'C'
            ORDER BY data_abertura;

            CREATE VIEW lista_pessoa AS
            SELECT id,nome,telefone,cidade,estado,cnpj,cpf,fornecedor
            FROM pessoa
            ORDER BY nome;


            CREATE VIEW lista_saldo_caixa AS
            SELECT IFNULL(cx_ent,cx_sai) id_caixa, IFNULL(entradas,0)-IFNULL(saidas,0) saldo
            FROM (SELECT SAI.id cx_sai, saidas, ENT.id cx_ent, entradas
                    FROM (SELECT fk_caixa_id id,SUM(valor) saidas
                            FROM lancamento
                            WHERE debito AND data_quitacao IS NOT NULL
                            GROUP BY id) SAI
                    LEFT JOIN
                            (SELECT fk_caixa_id id,SUM(valor) entradas
                            FROM lancamento
                            WHERE NOT debito AND data_quitacao IS NOT NULL
                            GROUP BY id) ENT
                    ON SAI.id = ENT.id
                    UNION
                    SELECT SAI.id cx_sai, saidas, ENT.id cx_ent, entradas 
                    FROM (SELECT fk_caixa_id id,SUM(valor) saidas
                            FROM lancamento
                            WHERE debito AND data_quitacao IS NOT NULL
                            GROUP BY id) SAI
                    RIGHT JOIN
                            (SELECT fk_caixa_id id,SUM(valor) entradas
                            FROM lancamento
                            WHERE NOT debito AND data_quitacao IS NOT NULL
                            GROUP BY id) ENT
                    ON SAI.id = ENT.id) SALDO
            ORDER BY id_caixa;
        """, multi=True)
        print("Queries executed")

# a.i) Relatório de categorias com respectivas quantidades de produtos e marcas com produtos daquela categoria (categoria/classificacao/produto)
def getCategoriesReport():
    cursor.execute("""
        SELECT nome,COUNT(DISTINCT(fk_produto_id)) produtos,COUNT(DISTINCT(fk_marca_id)) marcas
        FROM categoria CA
        JOIN classificacao CL ON CL.fk_categoria_id = CA.id
        JOIN produto PR ON PR.id = CL.fk_produto_id
        GROUP BY nome;
    """)
    categories = dict()
    for category in cursor:
        categories[category[0]] = {
            "products": category[1],
            "brands": category[2]
        }
    return categories

# b.i) Relatório de fornecedores dos quais nunca foi comprado produtos que trabalhamos mas que estão relacionados como fornecedores desses produtos
def getProvidersNotBoughtReport():
    cursor.execute("""
        SELECT nome, telefone, email, FM.fk_produto_id, descricao
        FROM pessoa PE
        JOIN fornecimento FM ON FM.fk_pessoa_id = PE.id
        JOIN produto PR ON PR.id = FM.fk_produto_id
        WHERE NOT EXISTS (
        SELECT *
        FROM pedido PD
        JOIN item_pedido IP ON IP.fk_pedido_numero = PD.numero
        WHERE PD.fk_pessoa_id = FM.fk_pessoa_id
        AND IP.fk_produto_id = FM.fk_produto_id)
        ORDER BY nome;
    """)
    providers = list()
    for provider in cursor:
        providers.append({
            "name": provider[0],
            "phone": "" if "None" == provider[1] else provider[1],
            "email": "" if "None" == provider[2] else provider[2],
            "productId": provider[3],
            "description": provider[4]
        })
    
    return providers

# b.ii) Relatório de OS em aberto de clientes com lançamentos vencidos em aberto para reter equipamento até quitarem tudo
def getOpenOrderReport():
    cursor.execute("""
        SELECT *
        FROM lista_ordem_servico
        WHERE id_pessoa IN (
        SELECT PE.id
        FROM lancamento L
        JOIN pedido PD ON PD.numero = L.fk_pedido_numero
        JOIN pessoa PE ON PE.id = PD.fk_pessoa_id
        WHERE data_vencimento < '2020-08-25'
        AND data_quitacao IS NULL
        AND NOT debito)
        AND data_entrega IS NULL;
    """)
    orders = list()
    for order in cursor:
        orders.append({
            "openedDate": str(order[0]),
            "number": order[1],
            "deliveredDate": "" if "None" == str(order[2]) else str(order[2]),
            "personId": order[3],
            "phone": order[4],
            "equipment": order[5]
        })
    return orders

# c) Relatório de clientes que já fizeram compras e com o respectivo total gasto, mas que nunca abriram uma ordem de serviço
def getClientsWhoBoughtReport():
    cursor.execute("""
        SELECT id, nome, SUM(valor) gastos
        FROM pessoa PE
        JOIN pedido PD ON PD.fk_pessoa_id = PE.id
        JOIN lancamento L ON L.fk_pedido_numero = PD.numero
        WHERE NOT debito
        AND tipo = 'V'
        AND NOT EXISTS (SELECT *
                            FROM pedido
                            WHERE fk_pessoa_id = PE.id
                            AND tipo = 'S')					 
        GROUP BY id, nome;
    """)
    clients = list()
    for client in cursor:
        clients.append({
            "id": client[0],
            "name": client[1],
            "spent": float(client[2])
        })
    
    return clients

# d.1) Relatório das ordens de serviço com total de produtos e serviços
def getOSTotaltReport():
    cursor.execute("""
        SELECT data_abertura, fk_pedido_numero numero_os, nome, equipamento, SUM(IP.quantidade*IP.valor_unitario-IFNULL(IP.desconto,0)) produtos, SUM(SV.quantidade*SV.valor_unitario-IFNULL(SV.desconto,0)) servicos, data_entrega
        FROM item_pedido IP
        JOIN lista_ordem_servico LOS ON LOS.numero = IP.fk_pedido_numero
        JOIN item_servico SV ON SV.fk_ordem_servico_numero = LOS.numero
        GROUP BY fk_pedido_numero;
    """)
    orders = list()
    for order in cursor:
        orders.append({
            "openedDate": str(order[0]),
            "os_number": order[1],
            "name": order[2],
            "equipment": order[3],
            "products": float(order[4]),
            "services": float(order[5]),
            "deliveredDate": "" if "None" == str(order[6]) else str(order[6])
        })
    
    return orders

# d.2) b.ii

# e.1) Listagem de todas as contas a pagar em aberto ou finalizadas
def getBillsToPay():
    cursor.execute("""
        SELECT data_vencimento,PE.nome,valor,numero,tipo,MP.nome AS meio_pagamento,CX.nome AS caixa,descricao,data_quitacao,data_lancamento
        FROM lancamento L
        LEFT JOIN pedido PD ON PD.numero = L.fk_pedido_numero
        LEFT JOIN pessoa PE ON PE.id = PD.fk_pessoa_id
        JOIN caixa CX ON CX.id = L.fk_caixa_id
        JOIN meio_pagamento MP ON MP.id = L.fk_meio_pagamento_id
        WHERE debito AND data_vencimento IS NOT NULL
        ORDER BY data_vencimento,data_quitacao;

    """)
    bills = list()
    for bill in cursor:
        bills.append({
            "dueDate": str(bill[0]),
            "name": "" if "None" == str(bill[1]) else str(bill[1]),
            "value": float(bill[2]),
            "number": None if "None" == str(bill[3]) else str(bill[3]),
            "type": "" if "None" == str(bill[4]) else str(bill[4]),
            "paymentMethod": bill[5],
            "description": bill[6],
            "paymentDate": "" if "None" == str(bill[7]) else str(bill[7]),
            "releaseDate": "" if "None" == str(bill[8]) else str(bill[8]),
        })
    
    return bills