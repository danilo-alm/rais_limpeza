# RAIS Limpeza

Este repositório contém código em Python que escrevi originalmente para **uso próprio** com o objetivo de limpar e padronizar dados da **RAIS (Relação Anual de Informações Sociais)**.
A RAIS é um levantamento anual feito pelo governo brasileiro com informações detalhadas sobre vínculos empregatícios formais. Os dados completos **não são públicos** — só podem ser acessados por meio de solicitações específicas e com autorização.

## Contexto

Os dados da RAIS do Brasil inteiro são enormes. No meu caso, trabalhei com o período **2008–2020**, o que totalizou cerca de **\~70 GB de arquivos CSV compactados**, que se expandem para aproximadamente **\~700 GB descompactados**.

Esses arquivos apresentam particularidades que exigem um tratamento cuidadoso:

* Nomes de colunas variam entre anos.
* Alguns campos mudam de formato ao longo do tempo.
* Problemas de formatação (ex.: CPFs sem zeros à esquerda).
* Em alguns anos não há coluna de idade já calculada — sendo necessário derivá-la a partir da data de nascimento.

## Estrutura do código

O pacote **`dataset_reader/`** contém a implementação principal, incluindo a classe **`DatasetReader`**, que:

1. **Identifica e mapeia nomes de colunas** usando as classes definidas em [`column_mapping.py`](dataset_reader/column_mapping.py).

   * Todos os nomes possíveis são testados para cada coluna esperada.
   * O mapeamento final é baseado na correspondência encontrada.
2. **Corrige tipos (`dtypes`)** para garantir consistência.
3. **Calcula a idade** quando ela não é fornecida nos dados.
4. **Corrige campos problemáticos**, como CPFs com zeros à esquerda omitidos.
5. **Lê e salva os dados de forma eficiente**:

   * Lê os arquivos CSV em *chunks* para evitar estouro de memória.
   * Salva o resultado em **Parquet** com compressão **Zstandard (`.zst`)**, que é altamente eficiente devido à grande quantidade de valores repetidos (campos categóricos).

A função **`read_and_save_chunks`** é especialmente útil para lidar com estados como **São Paulo**, cujo subset de colunas com as quais trabalhei podia consumir **+18 GB de RAM** quando carregadas de uma só vez.

## Scripts de filtragem e organização

A pasta **`filtering/`** inclui scripts usados no processamento inicial dos arquivos originais da RAIS:

* Os arquivos originais vinham em formato `.7z`, contendo `.txt` que na verdade eram CSVs.
* Esses scripts fazem:

  1. Extração do `.txt` de dentro do `.7z`.
  2. Leitura usando o `DatasetReader` (executando todas as etapas de padronização e limpeza citadas acima).
  3. Salvamento em um diretório de saída.
  4. Exclusão do arquivo `.txt` temporário para liberar espaço.

Existem dois scripts principais:

* **`filtering.py`** — Para os anos **anteriores a 2018**, quando os dados vinham **um arquivo por estado**.
* **`filtering_2018up.py`** — Para **2018 em diante**, quando a organização mudou: agora os arquivos são separados por **regiões** (ex.: Nordeste), com uma coluna adicional indicando o estado.

## Observações

* O código foi escrito **para meu uso pessoal** e **não foi originalmente planejado para ser público**.
* **Leia o código.** Você precisará modificar a filtragem para apontar o diretório de entrada e saída e o leitor dos CSVs para indicar as colunas que lhe interessam.
* Incluí [`cnae_and_cbo_manager.py`](filtering/cnae_and_cbo_manager.py) apenas porque meu [`filtering_2018up.py`](filtering/filtering_2018up.py) já dependia dele, mas você não conseguirá usá-lo sem os arquivos .xls e .csv que estão referenciadas lá. A planilha `RAIS_vinculos_layout.xls`, recebi junto com os dados da RAIS; a referente aos CBOs, achei facilmente na internet. Caso não as possua, crie seu próprio dicionário `city_to_state` em `filtering_2018up.py`, linha 27 para rodar o script de filtragem.  
* Embora eu tenha feito uma refatoração, possivelmente ainda há partes menos intuitivas do que o ideal.
* Não inclui nenhum dado da RAIS — apenas o código para processar datasets que você já possua.

<hr>

Fique a vontade para usar o código, mas não me responsabilizo por qualquer merda que você fizer. Boa pesquisa!