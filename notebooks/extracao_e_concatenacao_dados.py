import os
import polars as pl

class ParquetMerger:
    def __init__(self, folder_path: str, output_file: str):
        self.folder_path = folder_path
        self.output_file = output_file
        self.lazy_frames = []

    def extract_year(self, filename: str) -> int:
        """ Extrai o ano do nome do arquivo (ex: 'BancoVDE 2019.xlsx' -> 2019) """
        return int(filename.split()[-1].replace(".xlsx", ""))

    def load_excels(self):
        """ Carrega todos os arquivos .xlsx da pasta usando Polars LazyFrame """
        files = [f for f in os.listdir(self.folder_path) if f.startswith("BancoVDE") and f.endswith(".xlsx")]
        for file in files:
            file_path = os.path.join(self.folder_path, file)
            try:
                lf = pl.read_excel(file_path).lazy()
                lf = lf.with_columns(pl.lit(self.extract_year(file)).alias("ano"))  # Adiciona a coluna 'ano'
                self.lazy_frames.append(lf)
                print(f"[✓] Carregado: {file}")
            except Exception as e:
                print(f"[!] Erro ao processar {file}: {e}")

    def save_parquet(self):
        """ Concatena os LazyFrames e salva como Parquet """
        if not self.lazy_frames:
            raise RuntimeError("Nenhum dado carregado para salvar.")

        full_lf = pl.concat(self.lazy_frames)  # Mantém execução Lazy até aqui
        full_lf.sink_parquet(self.output_file)  # Salva diretamente no formato Parquet

        print(f"[✓] Arquivo salvo: {self.output_file}")

if __name__ == "__main__":
    folder_path = r"C:/Users/55819/Downloads/crime"  # Caminho onde os arquivos estão
    output_file = os.path.join(folder_path, "BancoVDE_2015_2025.parquet")

    merger = ParquetMerger(folder_path, output_file)
    merger.load_excels()
    merger.save_parquet()
