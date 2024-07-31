from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("HollywoodMovies CSV").getOrCreate()

# Carregar o CSV em um DataFrame
df = spark.read.csv("HollywoodMovies.csv", header=True, inferSchema=True)

# Mostrar o conteúdo do DataFrame
print("Mostrando todo o conteúdo")
df.show()

# Mostrar as 10 primeiras linhas
print("Apenas as 10 primeiras linhas")
df.limit(10).show()

# Selecionar Colunas Específicas
df_filtro = df.select("Movie", "Story", "Genre", "Year")

# Filtrar
action_df = df_filtro.filter((col("Genre") == "Action") & (col("Story") == "Quest") & (col("Year") == "2008"))
print("Aplicando filtros")
action_df.show()

# Ordem Crescente referente a "Filmes"
print("Aplicando ordem crescente")
df_sorted = action_df.orderBy("Movie")
df_sorted.show()

print("Ordem decrescente")
df_sorted2 = action_df.orderBy(col("Movie").desc())
df_sorted2.show()

# Contagem de itens por gênero
print("Contagem de item")
genre_count = df.groupBy("Genre").count()

# Calcular o total de filmes
total_count = df.count()
print(f"Total de filmes: {total_count}")

# Calcular a porcentagem
genre_percentage = genre_count.withColumn("Percentage", (col("count") / total_count) * 100)

# Mostrar o DataFrame com a porcentagem
print("Porcentagem dos gêneros")
genre_percentage.show()

# Coletar os resultados como um Pandas DataFrame para visualização
genre_percentage_pd = genre_percentage.toPandas()

# Substituir valores nulos por uma string padrão
genre_percentage_pd["Genre"].fillna("Desconhecido", inplace=True)

# Garantir que todos os valores na coluna 'Genre' sejam strings
genre_percentage_pd["Genre"] = genre_percentage_pd["Genre"].astype(str)

# # Configurar o gráfico
plt.figure(figsize=(12, 8))
plt.bar(genre_percentage_pd["Genre"], genre_percentage_pd["Percentage"], color='red')

# Adicionar rótulos e título
plt.xlabel('Gênero')
plt.ylabel('Porcentagem (%)')
plt.title('Porcentagem de Filmes por Gênero')

# Ajustar o layout e exibir o gráfico
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Ordenar por 'Year' (crescente) e depois por 'Movie' (decrescente)
df_sorted3 = df.orderBy(col("Year").asc(), col("Movie").desc())
df_sorted3.show()

# Teste de filtro 2 
print("Pegando os 20 filmes mais bem avaliados no Rotten Tomatoes e filmes somente de Drama")
df_filtro_2 = df.select("Movie", "RottenTomatoes" , "Genre", "Year")

# Aplica o filtro para gênero 'Drama' e ordena por 'RottenTomatoes' em ordem decrescente
df_filtered_sorted = df_filtro_2.filter(col("Genre") == "Drama").orderBy(col("RottenTomatoes").desc())

# Mostra os resultados sem truncar o texto
df_filtered_sorted.show(truncate=False)





# Contagem de filmes por ano
print("Contagem de filmes por ano")
year_count = df_filtro.groupBy("Year").count()

# Calcular o total de filmes
total_count = df_filtro.count()
print(f"Total de filmes: {total_count}")

# Calcular a porcentagem de filmes por ano
year_percentage = year_count.withColumn("Percentage", (col("count") / total_count) * 100)

# Mostrar o DataFrame com a porcentagem
print("Porcentagem de filmes por ano")
year_percentage.show()

# Coletar os resultados como um Pandas DataFrame para visualização
year_percentage_pd = year_percentage.toPandas()

# Garantir que todos os valores na coluna 'Year' sejam strings
year_percentage_pd["Year"] = year_percentage_pd["Year"].astype(str)

# Configurar o gráfico
plt.figure(figsize=(12, 8))
plt.bar(year_percentage_pd["Year"], year_percentage_pd["count"], color='skyblue')

# Adicionar rótulos e título
plt.xlabel('Ano')
plt.ylabel('Número de Filmes')
plt.title('Distribuição de Filmes por Ano')

# Ajustar o layout e exibir o gráfico
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()