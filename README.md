


# Documentação do DAG `fin_cotacoes_bcb_classic`

Este DAG realiza o processo de ETL (Extração, Transformação e Carga) para obter dados de cotações do Banco Central do Brasil e armazená-los em uma tabela no PostgreSQL. 

## Estrutura do DAG
O DAG está dividido nas seguintes tarefas:

1. **Extract**: Extrai dados de cotações diárias do site do Banco Central.
2. **Transform**: Transforma os dados, aplicando tipagem e formatação adequada.
3. **Create Table**: Cria a tabela `cotacoes` no PostgreSQL caso ainda não exista.
4. **Load**: Realiza o carregamento dos dados transformados para a tabela `cotacoes`, com suporte para operação de upsert.

## Requisitos

### Instalação do Homebrew

O [Homebrew](https://brew.sh/) é necessário para instalar o Astronomer CLI de maneira conveniente.

Para instalar o Homebrew, execute o seguinte comando no terminal:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Após a instalação, certifique-se de adicionar o Homebrew ao seu PATH:
```bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

### Instalação do Astronomer CLI

Utilize o Homebrew para instalar o Astronomer CLI. Execute o comando:
```bash
brew install astronomer/tap/astro
```

Verifique se a instalação foi bem-sucedida com o comando:
```bash
astro version
```

## Inicializando o Astronomer

Para inicializar o Astronomer e configurar o ambiente para o DAG, execute o comando abaixo no diretório do projeto:
```bash
astro dev start
```

Esse comando inicializa o ambiente local do Airflow configurado no Astronomer, permitindo executar o DAG `fin_cotacoes_bcb_classic`.

## Estrutura do Código

Abaixo está uma descrição das principais funções e tarefas do DAG:

### Função `extract`

Extrai os dados CSV da API do Banco Central com as cotações do dia anterior. Em caso de sucesso, retorna o conteúdo do CSV como uma string.

### Função `transform`

Transforma os dados extraídos em um DataFrame do Pandas, aplicando tipos de dados, formatando datas e adicionando uma coluna `DT_PROCESSAMENTO` com a data e hora atuais.

### Função `create_table`

Define o DDL SQL para criar a tabela `cotacoes`, que armazena os dados das cotações no PostgreSQL.

### Função `load`

Carrega o DataFrame transformado para o PostgreSQL. A função utiliza o método `insert_rows` para inserir ou atualizar registros existentes.

## Fluxo do DAG

As tarefas são executadas na seguinte ordem:

1. `extract_task` → Extrai os dados do CSV.
2. `transform_task` → Transforma os dados extraídos em um DataFrame.
3. `create_table_postgres_task` → Cria a tabela `cotacoes` no PostgreSQL (se não existir).
4. `load_task` → Carrega os dados transformados na tabela `cotacoes`.

