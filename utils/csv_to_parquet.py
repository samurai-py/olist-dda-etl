from pyspark.sql import SparkSession
import os

# Inicializar uma sessão Spark
spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

# Diretório de entrada (CSV)
input_dir = "include/raw_data/csv"

# Diretório de saída (Parquet)
output_dir = "include/raw_data/parquet"

# Lista todos os arquivos no diretório de entrada (CSV)
csv_files = [os.path.join(input_dir, file) for file in os.listdir(input_dir) if file.endswith(".csv")]

# Loop sobre cada arquivo CSV
for csv_file in csv_files:
    # Obter o nome do arquivo sem a extensão
    file_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    # Ler o arquivo CSV
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    
    # Salvar o DataFrame como arquivo Parquet
    parquet_file_path = os.path.join(output_dir, f"{file_name}.parquet")
    df.write.parquet(parquet_file_path, mode="overwrite")

# Encerrar a sessão Spark
spark.stop()
