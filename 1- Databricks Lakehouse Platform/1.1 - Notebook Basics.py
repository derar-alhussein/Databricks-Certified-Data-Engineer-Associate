# Databricks notebook source
print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC text with a **bold** and *italicized* in it.
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC
# MAGIC Images:
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank"> Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %run ../Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC > Este módulo fornece diversas utilidades para que os usuários interajam com o restante do Databricks.
# MAGIC - credenciais: DatabricksCredentialUtils -> Utilitários para interagir com credenciais dentro de notebooks
# MAGIC - dados: DataUtils -> Utilitários para entender e interagir com conjuntos de dados (EXPERIMENTAL)
# MAGIC - fs: DbfsUtils -> Manipula o sistema de arquivos do Databricks (DBFS) a partir do console
# MAGIC - tarefas: JobsUtils -> Utilitários para aproveitar os recursos de tarefas
# MAGIC - biblioteca: LibraryUtils -> Utilitários para bibliotecas isoladas por sessão
# MAGIC - meta: MetaUtils -> Métodos para interceptar o compilador (EXPERIMENTAL)
# MAGIC - notebook: NotebookUtils -> Utilitários para o fluxo de controle de um notebook (EXPERIMENTAL)
# MAGIC - pré-visualização: Preview -> Utilitários na categoria de pré-visualização
# MAGIC - segredos: SecretUtils -> Fornece utilitários para aproveitar segredos dentro de notebooks
# MAGIC - widgets: WidgetsUtils -> Métodos para criar e obter o valor vinculado de widgets de entrada dentro de notebooks

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC > O dbutils.fs fornece utilitários para trabalhar com sistemas de arquivos. A maioria dos métodos neste pacote pode receber um caminho DBFS (por exemplo, "/foo" ou "dbfs:/foo") ou outro URI de sistema de arquivos. Para obter mais informações - sobre um método, use dbutils.fs.help("nomeDoMétodo"). Em notebooks, você também pode usar a abreviação %fs para acessar o DBFS. A abreviação %fs mapeia diretamente para chamadas dbutils. Por exemplo, "%fs head --maxBytes=10000 /caminho/do/- arquivo" se traduz em "dbutils.fs.head("/caminho/do/arquivo", maxBytes = 10000)".
# MAGIC
# MAGIC > fsutils
# MAGIC - cp(from: String, to: String, recurse: boolean = false): boolean -> Copia um arquivo ou diretório, possivelmente entre sistemas de arquivos diferentes
# MAGIC - head(file: String, maxBytes: int = 65536): String -> Retorna os primeiros 'maxBytes' bytes do arquivo fornecido como uma string codificada em UTF-8
# MAGIC - ls(dir: String): Seq -> Lista o conteúdo de um diretório
# MAGIC - mkdirs(dir: String): boolean -> Cria o diretório fornecido se ele não existir, criando também quaisquer diretórios pai necessários
# MAGIC - mv(from: String, to: String, recurse: boolean = false): boolean -> Move um arquivo ou diretório, possivelmente entre sistemas de arquivos diferentes
# MAGIC - put(file: String, contents: String, overwrite: boolean = false): boolean -> Escreve a string fornecida em um arquivo, codificada em UTF-8
# MAGIC - rm(dir: String, recurse: boolean = false): boolean -> Remove um arquivo ou diretório
# MAGIC
# MAGIC > mount
# MAGIC - mount(source: String, mountPoint: String, encryptionType: String = "", owner: String = null, extraConfigs: Map = Map.empty[String, String]): boolean -> Monta o diretório de origem fornecido no DBFS no ponto de montagem especificado
# MAGIC - mounts: Seq -> Exibe informações sobre o que está montado no DBFS
# MAGIC - refreshMounts: boolean -> Força todas as máquinas neste cluster a atualizarem seu cache de montagem, garantindo que recebam as informações mais recentes
# MAGIC - unmount(mountPoint: String): boolean -> Exclui um ponto de montagem do DBFS
# MAGIC - updateMount(source: String, mountPoint: String, encryptionType: String = "", owner: String = null, extraConfigs: Map = Map.empty[String, String]): boolean -> Semelhante a mount(), mas atualiza um ponto de montagem existente (se houver) em vez de criar um novo

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 📘 Introdução aos Notebooks no Databricks
# MAGIC
# MAGIC Nesta lição, vamos aprender a trabalhar com **notebooks no Databricks**, desde a criação até funcionalidades mais avançadas.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧱 Criando um Notebook
# MAGIC
# MAGIC Para criar um novo notebook:
# MAGIC
# MAGIC 1. Vá até a aba **Workspace (Espaço de trabalho)**
# MAGIC 2. Clique em **Create (Criar)**
# MAGIC 3. Selecione **Notebook**
# MAGIC
# MAGIC Um notebook vazio chamado **“Untitled Notebook”** será criado automaticamente.
# MAGIC
# MAGIC ### ✏️ Renomeando o notebook
# MAGIC
# MAGIC * Clique no nome do notebook
# MAGIC * Digite o novo nome (ex: **“Notebook Basics”**)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧑‍💻 Linguagens Suportadas
# MAGIC
# MAGIC Por padrão, o notebook utiliza **Python**, mas você pode mudar a qualquer momento.
# MAGIC
# MAGIC O Databricks suporta:
# MAGIC
# MAGIC * Python
# MAGIC * SQL
# MAGIC * Scala
# MAGIC * R
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Conectando a um Cluster
# MAGIC
# MAGIC Antes de executar qualquer código:
# MAGIC
# MAGIC 1. Selecione um cluster na barra superior
# MAGIC 2. Clique em **Start (Iniciar)**
# MAGIC 3. Confirme a ação
# MAGIC
# MAGIC ⏳ O cluster pode levar alguns minutos para iniciar.
# MAGIC 🟢 Um círculo verde indica que ele está ativo.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ▶️ Executando Código
# MAGIC
# MAGIC Os notebooks funcionam com **células**.
# MAGIC
# MAGIC Para executar uma célula:
# MAGIC
# MAGIC * Clique no botão **Play**
# MAGIC * Ou use o atalho **Shift + Enter**
# MAGIC
# MAGIC Exemplo:
# MAGIC
# MAGIC ```python
# MAGIC print("Hello World")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ➕ Criando Novas Células
# MAGIC
# MAGIC * Passe o mouse sobre a célula atual
# MAGIC * Clique no botão **+**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Trabalhando com Múltiplas Linguagens
# MAGIC
# MAGIC Se você tentar rodar SQL em um notebook Python, ocorrerá erro.
# MAGIC
# MAGIC ### ✔️ Solução: Comandos mágicos
# MAGIC
# MAGIC Você pode mudar o idioma da célula:
# MAGIC
# MAGIC ```sql
# MAGIC %sql
# MAGIC SELECT * FROM tabela
# MAGIC ```
# MAGIC
# MAGIC 🔹 O `%sql` é um **comando mágico**
# MAGIC 🔹 Permite usar outra linguagem dentro do notebook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Markdown (Texto Formatado)
# MAGIC
# MAGIC Para adicionar texto formatado:
# MAGIC
# MAGIC ```markdown
# MAGIC %md
# MAGIC # Título
# MAGIC Texto em **negrito** ou *itálico*
# MAGIC ```
# MAGIC
# MAGIC Com Markdown você pode:
# MAGIC
# MAGIC * Criar títulos
# MAGIC * Listas
# MAGIC * Inserir imagens
# MAGIC * Criar tabelas (`|`)
# MAGIC * Usar HTML (links, por exemplo)
# MAGIC
# MAGIC 📌 Títulos criados aparecem automaticamente no **índice lateral**, facilitando navegação.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Executando Outro Notebook
# MAGIC
# MAGIC Você pode reutilizar código com o comando:
# MAGIC
# MAGIC ```python
# MAGIC %run /Includes/Setup
# MAGIC ```
# MAGIC
# MAGIC Isso executa outro notebook como se fosse parte do atual.
# MAGIC
# MAGIC 💡 Muito útil para:
# MAGIC
# MAGIC * Modularizar código
# MAGIC * Reutilizar variáveis e funções
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📂 Trabalhando com Arquivos
# MAGIC
# MAGIC ### 🔹 Comando mágico `%fs`
# MAGIC
# MAGIC ```bash
# MAGIC %fs ls /databricks-datasets
# MAGIC ```
# MAGIC
# MAGIC Lista arquivos de um diretório.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 Usando `dbutils`
# MAGIC
# MAGIC Mais poderoso que `%fs`:
# MAGIC
# MAGIC ```python
# MAGIC files = dbutils.fs.ls("/databricks-datasets")
# MAGIC display(files)
# MAGIC ```
# MAGIC
# MAGIC Com `dbutils` você pode:
# MAGIC
# MAGIC * Listar arquivos
# MAGIC * Copiar/remover arquivos
# MAGIC * Trabalhar com secrets
# MAGIC * Criar widgets
# MAGIC
# MAGIC 📊 A função `display()` mostra os dados de forma organizada (tabela, gráfico, etc.)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💾 Exportando Notebooks
# MAGIC
# MAGIC Para exportar:
# MAGIC
# MAGIC 1. Clique em **File**
# MAGIC 2. Vá em **Export**
# MAGIC 3. Escolha **iPython Notebook**
# MAGIC
# MAGIC ### 📦 Exportando múltiplos notebooks
# MAGIC
# MAGIC * Exporte como **DBC (Databricks Cloud)**
# MAGIC * Um arquivo zip com notebooks e diretórios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📥 Importando Notebooks
# MAGIC
# MAGIC * Clique em **Import**
# MAGIC * Selecione um arquivo `.dbc`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🕘 Histórico de Versões
# MAGIC
# MAGIC O Databricks salva automaticamente versões do notebook.
# MAGIC
# MAGIC Para acessar:
# MAGIC
# MAGIC 1. Clique em **Last Edit (Última edição)**
# MAGIC 2. Escolha uma versão
# MAGIC 3. Clique em **Restore this revision**
# MAGIC
# MAGIC 🔁 Permite voltar facilmente para versões anteriores.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Conclusão
# MAGIC
# MAGIC Nesta lição você aprendeu:
# MAGIC
# MAGIC * Criar e configurar notebooks
# MAGIC * Executar código em células
# MAGIC * Usar múltiplas linguagens
# MAGIC * Aplicar comandos mágicos
# MAGIC * Trabalhar com arquivos
# MAGIC * Reutilizar código com `%run`
# MAGIC * Exportar/importar notebooks
# MAGIC * Usar controle de versões
# MAGIC
# MAGIC * Atualizado 
# MAGIC
